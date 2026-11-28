"""Bounded live item/profile reads with unchanged real training writes."""
from __future__ import annotations

from collections import Counter
import re

from tests import mixed_inventory_projection_fixture as inventory
from tests import mixed_rank_gameplay_fixture as base
from tests import mixed_shared_query_fixture as transport
from tests import mixed_sect_shared_fixture as bounded


shared = base.shared
PARTICIPANTS = 128
COMMANDS = (("item_detail", "查看效果"), ("item_catalog", "查看修仙界物品"),
            ("breakthrough_probability", "我的突破概率"))


def phase_specs():
    return [{"label": mode, "mode": mode, "requests": 128, "concurrency": cap,
             "arrival_rate": rate, "order": "live-read-v28"}
            for mode, cap, rate in (("pure", 10, 20), ("mixed", 20, 20), ("contention", 20, 35))]


def _rank(definition, ranks):
    raw = int(definition.get("rank", 1))
    # Synthetic actors have ordinary roots. This independently applies the
    # catalog rank offset; production formatting is not the expected output.
    rank = (23 if raw == -5 else raw) + len(ranks) - 1 - 55
    return ranks[max(0, min(len(ranks) - rank, len(ranks) - 1))]


def seed(context, managers, *, participants=PARTICIPANTS):
    from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils.item_json import Items
    from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils.data_source import jsondata
    from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_config import convert_rank
    from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils.shared_read_cache import process_read_cache

    manifest = inventory.seed(context, managers, participants=participants)
    actors, catalog = manifest["actors"], Items()
    rows = shared._rows(context.paths.game_db,
                        "SELECT goods_id,state FROM back WHERE user_id=? ORDER BY goods_id", (actors[0],))
    owned = {int(row["goods_id"]) for row in rows}
    equipped = [int(row["goods_id"]) for row in rows if row["state"] == 1
                and catalog.get_data_by_item_id(row["goods_id"])["item_type"] == "法器"]
    mains = catalog.get_data_by_item_type(("功法",))
    ids = [int(item) for item in mains]
    available = [item for item in ids if item in owned]
    if len(equipped) != 1 or len(ids) < 15 or not available:
        raise ValueError("V28 needs one equipped weapon and15 real catalog main skills")
    main_id = next((item for item in available if catalog.get_data_by_item_id(item)["number"] > 0), available[0])
    main = catalog.get_data_by_item_id(main_id)
    with shared._uow(context.paths.game_db) as uow:
        for index, actor in enumerate(actors):
            if uow.execute("UPDATE BuffInfo SET main_buff=? WHERE user_id=?", (main_id, actor)).rowcount != 1:
                raise ValueError("V28 requires one existing BuffInfo per actor")
            if uow.execute("UPDATE user_xiuxian SET level_up_rate=? WHERE user_id=?",
                           (1 + index % 13, actor)).rowcount != 1:
                raise ValueError("V28 requires one existing live profile per actor")
    selected = list(dict.fromkeys([equipped[0], *ids[:15]]))
    totals = {item: 0 for item in selected}
    for row in shared._rows(context.paths.game_db,
                            f"SELECT goods_id,SUM(goods_num) AS n FROM back WHERE goods_id IN ({shared._marks(selected)}) "
                            "GROUP BY goods_id", selected):
        totals[int(row["goods_id"])] = row["n"]
    ranks = convert_rank("江湖好手")[1]

    def expected(item, name_label):
        definition = catalog.get_data_by_item_id(item)
        return {"id": str(item), "name_line": name_label + definition["name"],
                "rank_line": "境界：" + _rank(definition, ranks),
                "holding_line": "全服持有：" + bounded._number(totals[item]) + " 个"}

    detail = expected(equipped[0], "名字：")
    detail["name"] = catalog.get_data_by_item_id(equipped[0])["name"]
    if totals[equipped[0]] <= 0:
        raise ValueError("V28 equipped detail must exercise positive global holdings")
    profiles = shared._rows(context.paths.game_db,
                            f"SELECT user_id,level,level_up_rate,root_type FROM user_xiuxian "
                            f"WHERE user_id IN ({shared._marks(actors)}) ORDER BY user_id", actors)
    if len(profiles) != PARTICIPANTS or any(row["root_type"] != "天灵根" for row in profiles):
        raise ValueError("V28 probability oracle requires128 ordinary-root profiles")
    rates = jsondata.level_rate_data()
    probabilities = {str(row["user_id"]): "道友下一次突破成功概率为"
                     + str(rates[row["level"]] + int(row["level_up_rate"]) + main["number"]) + "%"
                     for row in profiles}
    snapshot = transport._snapshot(context.paths)
    manifest.update(fixture_version="mixed-live-read-v28", phase_specs=phase_specs(),
                    initial_generation=bounded._generation(context.paths.game_db),
                    live_read={"detail": detail, "catalog": [expected(item, "功法名字：") for item in ids[:15]],
                               "catalog_total": len(ids), "catalog_pages": (len(ids) + 14) // 15,
                               "probabilities": probabilities, "main_id": main_id, "main_number": main["number"],
                               "profiles": profiles, "snapshot": snapshot})
    manifest["limits"] += [
        "V28 reuses128actors x213 real inventory rows; one retained1M database, no additional million fixture",
        "384requests:128pure reads; mixed/contention96reads+32real dual-field stone training each",
        "three successful read matchers: equipped item detail, first15main-skill catalog entries, live probability",
        "catalog oracle uses definition order and isolated raw SQL SUM, not production item-message formatter",
        "ordinary roots only; probability varies failure bonus and uses real owned main-skill number",
        "contention repeats one training writer; read actor/command pairs remain unique across phases; real cooldowns retained",
        "full bounded back/BuffInfo hashes and live profile fields frozen; training receipts/ledger/balances/generation checked",
        "description whitespace intentionally ignored because existing catalog descriptions accumulate leading newlines",
        "no item transfer/action,60second cultivation, reincarnation rank adjustment or all674matcher coverage claimed",
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
    specs, actors = phase_specs(), manifest["actors"]
    if (phase_number not in range(3) or mode != specs[phase_number]["mode"] or count != 128
            or len(actors) != PARTICIPANTS or len(set(actors)) != PARTICIPANTS):
        raise ValueError("invalid frozen V28 plan")
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
            oracle = manifest["live_read"]
            suffix = " " + oracle["detail"]["name"] if key == "item_detail" else " 功法" if key == "item_catalog" else ""
            row = {"key": key, "command": command, "input_text": command + suffix, "actor": actor,
                   "performance_role": "optimized_live_read"}
            if key == "item_detail":
                row["expected_detail"] = oracle["detail"]
            elif key == "item_catalog":
                row.update(expected_catalog=oracle["catalog"], expected_pages=oracle["catalog_pages"])
            else:
                row["expected_reply"] = oracle["probabilities"][actor]
            reads[key] += 1
        plan.append({**row, "number": 890_000_000 + phase_number * 100_000 + index,
                     "event_type": "group", "phase_label": mode})
    return plan


def _semantic_lines(text):
    return [re.sub(r"^\[CQ:at,qq=[0-9]+\]\s*", "", line).lstrip("> ").strip()
            for line in text.splitlines()]


def classify_result(request, result):
    general = shared.classify_result(request, result)
    if general["outcome"] in {"boundary", "timeout", "exception", "cooldown", "business_rejection"}:
        return general
    try:
        text = transport._reply_text(result)
    except ValueError:
        return {"outcome": "unclassified", "reason": "v28_invalid_envelope"}
    key, lines = request["key"], _semantic_lines(text)
    if key == "stone_training":
        return base.classify_result(request, {**result, "replies": [text]})
    if key == "breakthrough_probability":
        valid = [line for line in lines if line] == [request["expected_reply"]]
    elif key in {"item_detail", "item_catalog"}:
        expected = [request["expected_detail"]] if key == "item_detail" else request["expected_catalog"]
        patterns = (("id", "ID："), ("name_line", "名字：" if key == "item_detail" else "功法名字："),
                    ("rank_line", "境界："), ("holding_line", "全服持有："))
        valid = all([line for line in lines if line.startswith(prefix)] == [
            prefix + row[field] if field == "id" else row[field] for row in expected]
                    for field, prefix in patterns)
        if key == "item_catalog":
            valid = valid and [line for line in lines if line.startswith("第 ") and line.endswith(" 页")] == [
                f"第 1/{request['expected_pages']} 页"]
    else:
        valid = False
    if valid:
        return {"outcome": "success", "reason": "v28_exact_live_fields"}
    if "物品 " in text and "不存在，请检查名称是否正确！" in text:
        return {"outcome": "business_rejection", "reason": "item_missing"}
    return {"outcome": "unclassified", "reason": "v28_reply_mismatch"}


def check_invariants(context, managers, manifest, results):
    checked = base.check_invariants(context, managers, manifest, results)
    errors, seen = list(checked["errors"]), set()
    if transport._snapshot(context.paths) != manifest["live_read"]["snapshot"]:
        errors.append("V28 full bounded inventory/BuffInfo hashes changed")
    profiles = shared._rows(context.paths.game_db,
                            f"SELECT user_id,level,level_up_rate,root_type FROM user_xiuxian "
                            f"WHERE user_id IN ({shared._marks(manifest['actors'])}) ORDER BY user_id", manifest["actors"])
    if profiles != manifest["live_read"]["profiles"]:
        errors.append("V28 live probability profile inputs changed")
    generation = bounded._generation(context.paths.game_db)
    if generation != {key: value + checked["fresh_training"] for key, value in manifest["initial_generation"].items()}:
        errors.append("V28 exact exp/stone generation mismatch")
    counts = Counter(row["request"]["key"] for row in results)
    phase = results[0]["request"]["phase_label"] if results else None
    expected = Counter(item_detail=43, item_catalog=43, breakthrough_probability=42) if phase == "pure" else Counter(
        item_detail=32, item_catalog=32, breakthrough_probability=32, stone_training=32)
    if results and (counts != expected or len(results) != 128):
        errors.append("V28 command counts differ from frozen128request phase")
    for row in results:
        request = row["request"]
        classification = classify_result(request, row)
        if classification != {"outcome": row["outcome"], "reason": row["reason"]} or classification["outcome"] != "success":
            errors.append("V28 unexpected reply/outcome")
        if request["key"] != "stone_training":
            identity = request["key"], request["actor"]
            if identity in seen:
                errors.append("V28 duplicate display cooldown identity")
            seen.add(identity)
    return {**checked, "ok": not errors, "errors": errors,
            "live_read_checks": {"per_command": dict(counts), "generation": generation,
                                 "snapshot": manifest["live_read"]["snapshot"]},
            "invariants": checked["invariants"] + "; full bounded inventory/BuffInfo hashes, live probability fields, exact item/order/count/page replies and generation"}
