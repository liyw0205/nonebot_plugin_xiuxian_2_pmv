"""One populated same-stage read workload on the retained synthetic fixture."""
from __future__ import annotations

from collections import Counter
from copy import deepcopy
import hashlib
import re

from tests import mixed_inventory_projection_fixture as inventory
from tests import mixed_rank_gameplay_fixture as base
from tests import mixed_sect_shared_fixture as bounded
from tests import mixed_shared_query_fixture as transport


shared = base.shared
PARTICIPANTS = 128
COMMANDS = (("activity_rewards", "活动奖励"), ("activity_boss_status", "活动首领"),
            ("main_backpack", "我的背包"))


def phase_specs():
    return [{"label": mode, "mode": mode, "requests": 128, "concurrency": cap,
             "arrival_rate": rate, "order": "overview-stage-v29"}
            for mode, cap, rate in (("pure", 10, 20), ("mixed", 20, 20), ("contention", 20, 35))]


def _activity_snapshot(paths):
    queries = {
        "sign": "SELECT user_id,sign_days,total_sign_days,last_sign_date FROM activity_user ORDER BY user_id",
        "damage": "SELECT activity_key,user_id,total_damage FROM activity_boss_damage ORDER BY activity_key,user_id",
        "state": "SELECT * FROM activity_boss_state ORDER BY activity_key",
        "rank_claim": "SELECT * FROM activity_boss_rank_claim ORDER BY activity_key,user_id,tier_key",
        "milestone_claim": "SELECT * FROM activity_boss_milestone_claim ORDER BY activity_key,user_id,milestone_key",
        "fight": "SELECT * FROM activity_boss_fight_log ORDER BY rowid",
    }
    return {name: {"rows": len(rows), "sha256": base._digest(rows)}
            for name, sql in queries.items() for rows in (shared._rows(paths.game_db, sql),)}


def seed(context, managers, *, participants=PARTICIPANTS):
    from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_activity import activity_config
    from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_activity.activity_rules import get_gameplay_activities
    from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils.item_json import Items
    from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils.shared_read_cache import process_read_cache

    manifest = inventory.seed(context, managers, participants=participants)
    paths, actors = context.paths, manifest["actors"]
    config_path = activity_config.CONFIG_PATH.resolve(strict=True)
    if not config_path.is_relative_to(paths.game_db.parent.resolve()):
        raise ValueError("V29 config must be restored with the isolated fixture")
    config = activity_config._load_config_projection_readonly()
    bosses = [row for row in get_gameplay_activities(config) if row.get("type") == "activity_boss"]
    if len(bosses) != 1:
        raise ValueError("V29 requires one original boss configuration")
    first = deepcopy(bosses[0])
    second = deepcopy(first)
    # Reuse an already-populated activity key, not another million-user DB.
    second["key"] = next(key for key in manifest["activity_keys"] if key != first["key"])
    for index, boss in enumerate((first, second)):
        boss.update(name=f"V29首领{index + 1}", boss_name=f"V29首领{index + 1}", max_hp=1_000_000,
                    rank_rewards=[{"rank_min": 1, "rank_max": 4096, "name": "前半名次", "reward": "灵石x1"}])
    config["gameplay_activities"] = [row for row in config.get("gameplay_activities", ())
                                     if row.get("type") != "activity_boss"] + [first, second]
    config.setdefault("extensions", {})["rank"] = {
        "boards": [{"key": source, "label": label, "source": source, "limit": 10}
                   for source, label in (("sign", "签到榜"), ("boss", "首领榜"))],
        "rewards": [{"board": source, "rank_min": 1, "rank_max": 8192,
                     "name": "参与奖励", "reward": "灵石x1"} for source in ("sign", "boss")],
    }
    activity_config._save_config_projection(config)
    rows = shared._rows(paths.game_db,
                        "SELECT user_id,total_damage FROM activity_boss_damage WHERE activity_key=? "
                        "ORDER BY total_damage,user_id", (first["key"],))
    if len(rows) != 8192:
        raise ValueError("V29 requires8192 real participants on each selected boss")
    with shared._uow(paths.game_db) as uow:
        for boss in (first, second):
            uow.execute("INSERT INTO activity_boss_state(activity_key,hp_left,max_hp) VALUES(?,750000,1000000) "
                        "ON CONFLICT(activity_key) DO UPDATE SET hp_left=750000,max_hp=1000000", (boss["key"],))
        counts = uow.executemany(
            "UPDATE activity_boss_damage SET total_damage=? WHERE activity_key=? AND user_id=?",
            ((1_000_000 + (8192 - index) * 1234, second["key"], str(row["user_id"]))
             for index, row in enumerate(rows)),
        )
        if counts.rowcount != 8192:
            raise ValueError("V29 second boss must reuse8192 existing damage rows")
    signs = shared._rows(paths.game_db,
                         "SELECT user_id,sign_days,total_sign_days,last_sign_date FROM activity_user WHERE sign_days>0")
    signs.sort(key=lambda row: (-int(row["sign_days"]), -int(row["total_sign_days"]),
                               str(row["last_sign_date"] or ""), str(row["user_id"])))
    sign_rank = {str(row["user_id"]): index for index, row in enumerate(signs, 1)}
    boss_rows = [shared._rows(paths.game_db,
                              "SELECT user_id,total_damage FROM activity_boss_damage WHERE activity_key=? "
                              "AND total_damage>0 ORDER BY total_damage DESC,user_id", (boss["key"],))
                 for boss in (first, second)]
    ranks = [{str(row["user_id"]): index for index, row in enumerate(rows, 1)} for rows in boss_rows]
    damage = [{str(row["user_id"]): int(row["total_damage"]) for row in rows} for rows in boss_rows]
    actor_oracle = {actor: {"ranks": [sign_rank[actor], ranks[0][actor], ranks[0][actor], ranks[1][actor]],
                            "damage": [damage[0][actor], damage[1][actor]],
                            "boss_hints": [1 <= rank[actor] <= 4096 for rank in ranks]} for actor in actors}
    catalog = Items()
    equipped = shared._rows(paths.game_db,
                             "SELECT goods_id,goods_num,bind_num FROM back WHERE user_id=? AND state=1 ORDER BY goods_id",
                             (actors[0],))
    weapon = next(row for row in equipped if catalog.get_data_by_item_id(row["goods_id"])["item_type"] == "法器")
    definition = catalog.get_data_by_item_id(weapon["goods_id"])
    manifest.update(fixture_version="mixed-overview-stage-v29", phase_specs=phase_specs(),
                    initial_generation=bounded._generation(paths.game_db),
                    overview_stage={"actors": actor_oracle, "bosses": [first["boss_name"], second["boss_name"]],
                        "boss_keys": [first["key"], second["key"]], "boss_rows": [len(rows) for rows in boss_rows],
                        "activity_snapshot": _activity_snapshot(paths), "inventory_snapshot": transport._snapshot(paths),
                        "config_sha256": hashlib.sha256(config_path.read_bytes()).hexdigest(),
                        "backpack": {"name": definition["name"], "level": definition["level"],
                                     "quantity": weapon["goods_num"], "bound": weapon["bind_num"]}})
    manifest["limits"] += [
        "V29 times3changed read matchers, not all9static beneficiaries or674/41 runtime entries",
        "two distinct live boss rankings,8192positive existing rows each; secondary uses an existing activity key with reversed scores",
        "same retained1M users,128actors,213positive back rows each; no additional million fixture",
        "activity reward positions/own boss damage/rank hints independently checked; reward transactions unchanged and not timed",
        "384requests/version with64real dual-field training writes; short diagnostic sample, not sustained50successful/s",
        "special-item replay, alchemy and unequip receive affected regression coverage, not timed capacity claims",
    ]
    manifest.pop("seed_sha256", None)
    manifest["seed_sha256"] = base._digest(manifest)
    process_read_cache.clear()
    for manager in managers:
        clear = getattr(manager, "_clear_read_cache", None)
        if callable(clear):
            clear()
    return manifest


def request_plan(manifest, mode, count, phase_number=0):
    actors, specs = manifest["actors"], phase_specs()
    if (phase_number not in range(3) or mode != specs[phase_number]["mode"] or count != 128
            or len(actors) != 128 or len(set(actors)) != 128):
        raise ValueError("invalid frozen V29 plan")
    plan, reads, writes = [], Counter(), 0
    for index in range(count):
        if mode != "pure" and index % 4 == 3:
            row = {"key": "stone_training", "command": "灵石修炼", "input_text": "灵石修炼 1000",
                   "actor": actors[0 if mode == "contention" else writes],
                   "requested_stone": base.REQUESTED_STONE, "exp_gain": base.EXP_GAIN,
                   "performance_role": "unchanged_write_control"}
            writes += 1
        else:
            key, command = COMMANDS[sum(reads.values()) % len(COMMANDS)]
            actor = actors[(0, 43, 75)[phase_number] + reads[key]]
            row = {"key": key, "command": command, "input_text": command, "actor": actor,
                   "performance_role": "optimized_same_stage_read",
                   "expected": manifest["overview_stage"]["actors"][actor]}
            if key == "activity_boss_status":
                row["bosses"] = manifest["overview_stage"]["bosses"]
            if key == "main_backpack":
                row["backpack"] = manifest["overview_stage"]["backpack"]
                row["input_text"] += " " + row["backpack"]["name"]
            reads[key] += 1
        plan.append({**row, "number": 890_000_000 + phase_number * 100_000 + index,
                     "event_type": "group", "phase_label": mode})
    return plan


def classify_result(request, result):
    general = shared.classify_result(request, result)
    if general["outcome"] in {"boundary", "timeout", "exception", "cooldown", "business_rejection"}:
        return general
    try:
        text = transport._reply_text(result)
    except ValueError:
        return {"outcome": "unclassified", "reason": "v29_invalid_envelope"}
    if request["key"] == "stone_training":
        return base.classify_result(request, {**result, "replies": [text]})
    lines = [re.sub(r"^\[CQ:at,qq=[0-9]+\]\s*", "", line).lstrip("> ").strip() for line in text.splitlines()]
    if request["key"] == "activity_rewards":
        valid = [line for line in lines if line.startswith("我的名次：")] == [
            f"我的名次：第 {rank} 名" for rank in request["expected"]["ranks"]]
    elif request["key"] == "activity_boss_status":
        valid = [line for line in lines if line.startswith("我的累计伤害 ")] == [
            "我的累计伤害 " + bounded._number(value) for value in request["expected"]["damage"]]
        valid = valid and sum(line.startswith("全服血量 ") for line in lines) == 2
        valid = valid and [line for line in lines if line.startswith("可用 活动首领领奖 排行")] == [
            f"可用 活动首领领奖 排行 {name} 领取" for name, hint in zip(
                request["bosses"], request["expected"]["boss_hints"]) if hint]
    elif request["key"] == "main_backpack":
        item = request["backpack"]
        plain = (any(item["level"] + "-" + item["name"] in line for line in lines)
                 and f"拥有数量：{item['quantity']}，绑定数量：{item['bound']}" in lines
                 and "※已装备※" in lines)
        fallback = (f"- {item['name']} 数量:{item['quantity']} 绑定:{item['bound']} ※已装备※" in lines
                    and f"使用：使用 {item['name']}" in lines)
        valid = (plain or fallback) and "第 1/1 页" in lines
    else:
        valid = False
    return ({"outcome": "success", "reason": "v29_exact_live_projection"} if valid else
            {"outcome": "unclassified", "reason": "v29_reply_mismatch"})


def check_invariants(context, managers, manifest, results):
    from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_activity import activity_config

    checked = base.check_invariants(context, managers, manifest, results)
    errors, oracle = list(checked["errors"]), manifest["overview_stage"]
    if transport._snapshot(context.paths) != oracle["inventory_snapshot"]:
        errors.append("V29 bounded inventory/BuffInfo snapshot changed")
    if _activity_snapshot(context.paths) != oracle["activity_snapshot"]:
        errors.append("V29 sign/boss/claim/fight snapshot changed")
    if hashlib.sha256(activity_config.CONFIG_PATH.read_bytes()).hexdigest() != oracle["config_sha256"]:
        errors.append("V29 config changed")
    if bounded._generation(context.paths.game_db) != {
        key: value + checked["fresh_training"] for key, value in manifest["initial_generation"].items()
    }:
        errors.append("V29 generation mismatch")
    counts = Counter(row["request"]["key"] for row in results)
    expected = Counter(activity_rewards=43, activity_boss_status=43, main_backpack=42) if results and results[0]["request"]["phase_label"] == "pure" else Counter(
        activity_rewards=32, activity_boss_status=32, main_backpack=32, stone_training=32)
    if results and (len(results) != 128 or counts != expected):
        errors.append("V29 frozen request counts differ")
    identities = [(row["request"]["key"], row["request"]["actor"]) for row in results
                  if row["request"]["key"] != "stone_training"]
    if len(identities) != len(set(identities)):
        errors.append("V29 read cooldown identity repeated")
    for row in results:
        result = classify_result(row["request"], row)
        if result != {"outcome": row["outcome"], "reason": row["reason"]} or result["outcome"] != "success":
            errors.append("V29 reply/outcome mismatch")
    return {**checked, "ok": not errors, "errors": errors,
            "overview_stage_checks": {"per_command": dict(counts), "boss_rows": oracle["boss_rows"]},
            "invariants": checked["invariants"] + "; full bounded back/BuffInfo and activity projection hashes, exact live ranks/hints/damage and generation"}
