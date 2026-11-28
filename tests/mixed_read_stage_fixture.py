"""V23 composes populated actors with multi-round cultivation and fate-root reads.

The existing outer harness owns real matcher dispatch, one shared instance/DB,
phase draining/restoration, SQL diagnostics and all resource guards.
"""
from __future__ import annotations

from collections import Counter
from copy import deepcopy
import json
import random

from tests import mixed_rank_gameplay_fixture as base


shared = base.shared
PARTICIPANTS = base.PARTICIPANTS
MESSAGE_BASE = 896_000_000
ROOT_LEVEL = 4
COMMANDS = (("mind_state", "我的状态"), ("cultivation_value", "我的修为"),
            ("asset_overview", "我的资产总览"), ("stone_training", "灵石修炼"),
            ("dual_cultivation", "双修"))


def phase_specs():
    return [{"label": mode, "mode": mode, "requests": 128,
             "concurrency": cap, "arrival_rate": rate, "order": "read-stage-v23"}
            for mode, cap, rate in (("pure", 10, 20), ("mixed", 20, 20),
                                    ("contention", 20, 35))]


def seed(context, managers, *, participants=PARTICIPANTS):
    from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_buff import partner
    from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils.numeric_bind import compute_fate_root_rate

    manifest = base.seed(context, managers, participants=participants)
    roots = partner.jsondata.root_data()
    rate = compute_fate_root_rate(ROOT_LEVEL, roots["永恒道果"]["type_speeds"],
                                  roots["命运道果"]["type_speeds"])
    multiplier = float(rate * partner.jsondata.level_data()[base.LEVEL]["spend"])
    actors = manifest["actors"]
    names = {actor: f"SyntheticReadStage{index:03d}" for index, actor in enumerate(actors)}
    with shared._uow(context.paths.game_db) as uow:
        for actor in actors:
            power = int(manifest["initial"][actor]["exp"] * multiplier + .5)
            uow.execute("UPDATE user_xiuxian SET user_name=?,root_type='命运道果',root_level=?,power=? "
                        "WHERE user_id=?", (names[actor], ROOT_LEVEL, power, actor))
            # A real no-skill stage keeps the gain oracle small; inventories
            # remain the populated 20-item rows/actor created by the base seed.
            uow.execute("UPDATE BuffInfo SET main_buff=0,blessed_spot=0 WHERE user_id=?", (actor,))
            manifest["initial"][actor]["power"] = power
        if uow.query_one("SELECT operation_id FROM partner_cultivation_operations "
                         "WHERE operation_id LIKE 'relation:cultivation:896%' LIMIT 1"):
            raise ValueError("V23 cultivation receipt namespace is not empty")
    with shared._uow(context.paths.player_db) as uow:
        for actor in actors:
            uow.execute("INSERT INTO partner(user_id,partner_id,affection) VALUES(?,NULL,0) "
                        "ON CONFLICT(user_id) DO UPDATE SET partner_id=NULL,affection=0", (actor,))
            uow.execute("INSERT INTO partner_two_exp_usage(user_id,used_count) VALUES(?,0) "
                        "ON CONFLICT(user_id) DO UPDATE SET used_count=0", (actor,))
            uow.execute("INSERT INTO status(user_id,two_exp_protect) VALUES(?,'off') "
                        "ON CONFLICT(user_id) DO UPDATE SET two_exp_protect='off'", (actor,))
        statistics = {str(row["user_id"]): int(row["value"] or 0) for row in uow.query_all(
            'SELECT user_id,"双修次数" AS value FROM statistics '
            f"WHERE user_id IN ({shared._marks(actors)})", actors)}
    # Seeded randomness freezes the real event distribution; no business result
    # or settlement is replaced, and cooldowns/protection checks remain active.
    partner.runtime_random = random.Random(20261127)
    manifest.update(fixture_version="mixed-read-stage-v23", phase_specs=phase_specs(),
                    names=names, power_multiplier=multiplier, root_level=ROOT_LEVEL,
                    root_rate=rate, realm_rate=partner.jsondata.level_data()[base.LEVEL]["spend"],
                    initial_dual_statistics=statistics)
    manifest["limits"] += [
        "384 requests; eight optimized matchers total, four sampled targets plus unchanged assets; no all674/41 claim",
        "all128 actors have live fate-root level4, 2560 backpack rows and real Buff/impart rows; no nonempty main skill in this load",
        "mixed/contention each16 distinct 3-round dual-cultivation operations and16 dual-field stone writes; separate writer groups",
        "contention shares personal display/writer identities, not simultaneous write/write settlement capacity; actual overlap is reported",
        "dual-cultivation uses real direct off-protection/name lookup; invited/partner/special/cap/replay boundaries use direct regressions",
        "seeded RNG preserves all real random branches/rules; gains independently checked against all 3-round normal/special combinations",
        "activity8192 population inherited for setup reuse is not exercised and supports no new activity-performance claim",
        "no exact-rank traffic here; global rank invalidation/cold-rank and sustained50success/s remain separate coverage gaps",
    ]
    manifest.pop("seed_sha256", None)
    manifest["seed_sha256"] = base._digest(manifest)
    return manifest


def request_plan(manifest, mode, count, phase_number=0):
    specs = phase_specs()
    if (not 0 <= phase_number < len(specs) or mode != specs[phase_number]["mode"] or count != 128
            or len(manifest["actors"]) != 128 or len(set(manifest["actors"])) != 128):
        raise ValueError("invalid frozen read-stage plan")
    actors, result, counters = manifest["actors"], [], Counter()
    schedule = (["cultivation_value", "mind_state", "cultivation_value", "asset_overview"]
                if mode == "pure" else
                ["cultivation_value", "mind_state", "asset_overview", "dual_cultivation",
                 "cultivation_value", "mind_state", "asset_overview", "stone_training"])
    for index in range(count):
        key = schedule[index % len(schedule)]
        offset = counters[key]
        command = dict(COMMANDS)[key]
        extra = {}
        if key == "dual_cultivation":
            start = 0 if mode == "mixed" else 64
            actor, target = actors[start + 2 * offset + 1], actors[start + 2 * offset]
            extra = {"target": target, "rounds": 3}
            text = f"双修 {manifest['names'][target]} 3次"
        elif key == "stone_training":
            actor = actors[(32 if mode == "mixed" else 48) + offset]
            text = f"灵石修炼 {base.REQUESTED_STONE}"
            extra = {"requested_stone": base.REQUESTED_STONE, "exp_gain": base.EXP_GAIN}
        else:
            start = (64 if mode == "pure" else 0 if mode == "mixed" else 32) if key == "cultivation_value" else phase_number * 32
            actor, text = actors[start + offset], command
            extra = {"success_markers": dict((name, markers) for name, _, markers in shared.READ_SPECS)[key]}
        result.append({"key": key, "command": command, "input_text": text, "actor": actor,
                       "number": MESSAGE_BASE + phase_number * 100_000 + index,
                       "event_type": "group", "phase_label": mode,
                       "performance_role": "unchanged_control" if key == "asset_overview" else "optimized_target",
                       **extra})
        counters[key] += 1
    return result


def classify_result(request, result):
    if request["key"] != "dual_cultivation":
        return base.classify_result(request, result)
    checked = shared.classify_result({**request, "success_markers": []}, result)
    if checked["outcome"] in {"boundary", "timeout", "exception", "cooldown"}:
        return checked
    text = "\n".join(map(str, result.get("replies", ())))
    if text.count("获得修为：") == 2 and "双修未开始" not in text and "后续双修停止" not in text:
        return {"outcome": "success", "reason": "fresh_three_round_cultivation"}
    return {"outcome": "unclassified", "reason": "unexpected_cultivation_reply"}


def _possible_gains(first, second):
    # No skill/blessing, nonpartner and far below cap: each round is base .005,
    # optionally multiplied by the original special-event 1.5 before flooring.
    states = {(first, second)}
    for _ in range(3):
        states = {(a + int(int((a + b) * .005) * multiplier),
                   b + int(int((a + b) * .005) * multiplier))
                  for a, b in states for multiplier in (1, 1.5)}
    return {(a - first, b - second) for a, b in states}


def check_invariants(context, managers, manifest, results):
    actors = manifest["actors"]
    planned = {f"relation:cultivation:{row.get('request', row)['number']}:"
               f"{row.get('request', row)['actor']}:{row.get('request', row)['target']}": row.get("request", row)
               for row in results if row.get("request", row)["key"] == "dual_cultivation"
               and row.get("outcome") == "success"}
    receipts = shared._rows(context.paths.game_db, "SELECT * FROM partner_cultivation_operations "
                            "WHERE operation_id LIKE 'relation:cultivation:896%'")
    adjusted, errors, usage = deepcopy(manifest), [], Counter()
    if {row["operation_id"] for row in receipts} != set(planned):
        errors.append("cultivation receipts do not match successful request identities")
    for receipt in receipts:
        request = planned.get(receipt["operation_id"])
        if request is None:
            continue
        first, second = request["actor"], request["target"]
        a, b = manifest["initial"][first]["exp"], manifest["initial"][second]["exp"]
        payload = json.loads(receipt["payload"])
        gains = (int(receipt["exp_1"]), int(receipt["exp_2"]))
        if (len(payload) != 25 or payload[:4] != [first, second, a, b]
                or payload[4:7] != [*gains, 3] or payload[17:25] != [0, 0, None, None, None, None, None, "off"]
                or gains not in _possible_gains(a, b) or int(receipt["used_count"]) != 3
                or int(receipt["affection_1"]) != 0 or int(receipt["affection_2"]) != 0):
            errors.append("cultivation request/reward/usage/protection identity mismatch")
        for index, (actor, gain) in enumerate(zip((first, second), gains)):
            values = adjusted["initial"][actor]
            values["exp"] += gain
            values["power"] = int(round(values["exp"] * manifest["root_rate"] * manifest["realm_rate"], 0))
            usage[actor] += 3
            if payload[7 + index] != values["power"]:
                errors.append("cultivation receipt power mismatch")
            expected_attributes = [min(1000 + values["exp"] // 10, values["exp"] // 2),
                                   min(1000 + values["exp"] // 20, values["exp"]), values["exp"] // 10]
            if payload[9 + index * 3:12 + index * 3] != expected_attributes:
                errors.append("cultivation receipt combat projection mismatch")
    base_check = base.check_invariants(context, managers, adjusted, results)
    errors.extend(base_check["errors"])
    for row in shared._rows(context.paths.player_db, "SELECT user_id,used_count FROM partner_two_exp_usage "
                            f"WHERE user_id IN ({shared._marks(actors)})", actors):
        if int(row["used_count"]) != usage[str(row["user_id"])]:
            errors.append("cultivation usage mismatch")
    statistics = {str(row["user_id"]): int(row["value"] or 0) for row in shared._rows(
        context.paths.player_db, 'SELECT user_id,"双修次数" AS value FROM statistics '
        f"WHERE user_id IN ({shared._marks(actors)})", actors)}
    if any(statistics.get(actor, 0) != manifest["initial_dual_statistics"].get(actor, 0) + usage[actor]
           for actor in actors):
        errors.append("cultivation statistics mismatch")
    if any(row["partner_id"] is not None or int(row["affection"] or 0) != 0 for row in shared._rows(
            context.paths.player_db, "SELECT partner_id,affection FROM partner "
            f"WHERE user_id IN ({shared._marks(actors)})", actors)):
        errors.append("nonpartner cultivation changed relationship/affection")
    profiles = shared._rows(context.paths.game_db, "SELECT user_id,root_type,root_level,user_stamina "
                            f"FROM user_xiuxian WHERE user_id IN ({shared._marks(actors)})", actors)
    for row in profiles:
        actor = str(row["user_id"])
        spent = sum(10 for request in planned.values() if request["actor"] == actor)
        if (row["root_type"] != "命运道果" or int(row["root_level"]) != ROOT_LEVEL
                or int(row["user_stamina"]) != 100 - spent):
            errors.append("root or stamina live value mismatch")
    return {**base_check, "ok": not errors, "errors": errors,
            "dual_receipts": len(receipts), "dual_rounds": sum(usage.values()) // 2,
            "scope": "live exp/stone/power/usage/stamina/relations/statistics, receipt identities, populated inventory, sole1M and DB integrity; finite direct-off path"}
