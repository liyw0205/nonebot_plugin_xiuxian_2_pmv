"""Real cooperative boss writes on the retained cold-rank fixture."""
from __future__ import annotations

from collections import Counter
from datetime import datetime
import hashlib
import json
from pathlib import Path

from tests import mixed_cold_rank_fixture as cold


player = cold.player
shared = cold.shared
activity = cold.activity
PARTICIPANTS = cold.PARTICIPANTS
BOSS_WRITES = 16
HISTORY_ROWS = 8192
BOSS_KEY = "activity_boss_attack"
LEDGER_ACTION = "activity.activity_boss.fight_cooperative_boss"
OPERATION_PATTERN = "activity:boss-coop:%:890%"


def phase_specs():
    return [{**row, "requests": row["requests"] + (32 if row["mode"] != "pure" else 0),
             "order": row["order"] + "-activity-write"} for row in cold.phase_specs()]


def _digest_query(uow, sql, params=(), *, limit=65536):
    digest, count = hashlib.sha256(), 0
    cursor = uow.execute(sql, params)
    try:
        while rows := cursor.fetchmany(256):
            for row in rows:
                count += 1
                if count > limit:
                    raise ValueError("activity write snapshot exceeded its row bound")
                digest.update(json.dumps(dict(row), sort_keys=True, ensure_ascii=True,
                                         separators=(",", ":")).encode("ascii") + b"\n")
    finally:
        cursor.close()
    return {"rows": count, "sha256": digest.hexdigest()}


def _select_writers(manifest, boss_rows, profiles, activity_config, multiplier):
    excluded = {row["actor"] for phase, spec in enumerate(cold.phase_specs())
                for row in cold.request_plan(manifest, spec["mode"], spec["requests"], phase)
                if row["key"] == "activity_boss_board"}
    positions = {str(row["user_id"]): index for index, row in enumerate(boss_rows)}
    values = {str(row["user_id"]): int(row["total_damage"]) for row in boss_rows}
    max_hp = int(activity_config["max_hp"])
    damage, actors = {}, []
    for actor in manifest["groups"]["training_low"]:
        if actor in excluded or actor not in profiles or actor not in positions:
            continue
        if any(manifest["score_evidence"][actor][field]["storage"] != "integer"
               for field in ("exp", "stone")):
            continue
        amount = min(max(1, int(max(0, int(profiles[actor]["atk"]))
                                * float(activity_config.get("atk_ratio") or .1) * multiplier)),
                     max(1, int(max_hp * float(activity_config.get("hit_hp_cap_ratio") or .01))))
        index = positions[actor]
        if index < activity.BOSS_DISPLAY_LIMIT or values[actor] <= 0:
            continue
        if int(boss_rows[index - 1]["total_damage"]) - values[actor] <= amount:
            continue
        actors.append(actor)
        damage[actor] = amount
        if len(actors) == BOSS_WRITES:
            break
    if len(actors) != BOSS_WRITES:
        raise ValueError("activity writes need 16 safe integer actors outside personal boss reads/TOP15")
    return actors, damage, excluded


def _snapshot(uow, key, actors, historical_max_id):
    marks = shared._marks(actors)
    return {
        "sign": _digest_query(uow, "SELECT * FROM activity_user ORDER BY user_id"),
        "sign_logs": _digest_query(uow, "SELECT * FROM activity_sign_log ORDER BY id"),
        "other_damage": _digest_query(uow,
            "SELECT * FROM activity_boss_damage WHERE activity_key<>? ORDER BY activity_key,user_id", (key,)),
        "nonwriter_damage": _digest_query(uow,
            f"SELECT * FROM activity_boss_damage WHERE activity_key=? AND user_id NOT IN ({marks}) ORDER BY user_id",
            (key, *actors)),
        "historical_logs": _digest_query(uow,
            "SELECT * FROM activity_boss_fight_log WHERE id<=? ORDER BY id", (historical_max_id,)),
        "other_states": _digest_query(uow,
            "SELECT * FROM activity_boss_state WHERE activity_key<>? ORDER BY activity_key", (key,)),
        "milestones": uow.query_all("SELECT * FROM activity_boss_milestone ORDER BY activity_key,milestone_key"),
        "items": _digest_query(uow,
            "SELECT * FROM activity_item_inventory ORDER BY activity_key,user_id,item_id"),
        "writer_damage": {str(row["user_id"]): dict(row) for row in uow.query_all(
            f"SELECT * FROM activity_boss_damage WHERE activity_key=? AND user_id IN ({marks}) ORDER BY user_id",
            (key, *actors))},
        "state": uow.query_one("SELECT * FROM activity_boss_state WHERE activity_key=?", (key,)),
    }


def _compatibility_hit_snapshot(paths, json_snapshot):
    from nonebot_plugin_xiuxian_2.compatibility import commands

    root = Path(paths.game_db).parent.resolve()
    configured = commands._hit_path()
    if configured is None:
        raise ValueError("activity compatibility diagnostic path is unavailable")
    path = Path(configured).resolve()
    if not path.is_relative_to(root) or path.name != "compatibility_hits.json":
        raise ValueError("activity compatibility diagnostic is outside the restored fixture root")
    relative = str(path.relative_to(root.parent))
    existed = path.is_file()
    raw = path.read_bytes() if existed else None
    baseline = json.loads(raw.decode("utf-8")) if raw is not None else {}
    if (not isinstance(baseline, dict) or any(not isinstance(key, str)
            or type(value) is not int or value < 0 for key, value in baseline.items())):
        raise ValueError("activity compatibility diagnostic requires a nonnegative integer counter dict")
    digest = hashlib.sha256(raw).hexdigest() if raw is not None else None
    if json_snapshot.get(relative) != digest:
        raise ValueError("activity compatibility diagnostic differs from the original JSON snapshot")
    return {"relative_path": relative, "baseline": baseline, "existed": existed,
            "sha256": digest, "allowed_delta_key": "feature:activity"}


def _expected_json_snapshot(manifest, requests):
    expected = dict(manifest["json_snapshot"])
    saved = manifest["activity_write"]["compatibility_hits"]
    count = sum(request["key"] == BOSS_KEY for request in requests)
    if count:
        counters = dict(saved["baseline"])
        counters["feature:activity"] = counters.get("feature:activity", 0) + count
        encoded = json.dumps(counters, ensure_ascii=False, sort_keys=True).encode("utf-8")
        expected[saved["relative_path"]] = hashlib.sha256(encoded).hexdigest()
    return expected


def seed(context, managers, *, participants=PARTICIPANTS):
    from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_activity import activity_config
    from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_activity.activity_rules import get_gameplay_activities

    manifest = cold.seed(context, managers, participants=participants)
    compatibility = _compatibility_hit_snapshot(context.paths, manifest["json_snapshot"])
    config = activity_config._load_config_projection_readonly()
    runtime = activity_config.activity_runtime_state(config)
    bosses = [row for row in get_gameplay_activities(config) if row.get("type") == "activity_boss"]
    if (len(bosses) != 1 or bosses[0].get("mode") not in {"cooperative", "both"}
            or not activity_config.activity_state(bosses[0])[0]
            or not runtime.get("ok") or "boss" not in runtime.get("features", ())):
        raise ValueError("activity write fixture requires the existing live cooperative boss rules")
    boss = bosses[0]
    key, today = str(boss["key"]), datetime.now().strftime("%Y-%m-%d")
    multiplier = max(0.0, float(runtime.get("multiplier") or 1.0))
    _sign, boss_rows = activity._ranked_rows(context.paths, key)
    with shared._uow(context.paths.game_db) as uow:
        profiles = {str(row["user_id"]): dict(row) for row in uow.query_all(
            f"SELECT user_id,atk,user_name FROM user_xiuxian WHERE user_id IN ({shared._marks(manifest['actors'])})",
            manifest["actors"])}
        actors, damage, excluded = _select_writers(manifest, boss_rows, profiles, boss, multiplier)
        for table in ("activity_boss_settlement_operations", "operation_ledger"):
            if uow.query_one(f"SELECT operation_id FROM {table} WHERE operation_id LIKE ? LIMIT 1",
                             (OPERATION_PATTERN,)):
                raise ValueError("activity write operation namespace is not empty")
        used = uow.query_one(
            "SELECT COUNT(*) AS n FROM activity_boss_fight_log WHERE activity_key=? "
            f"AND user_id IN ({shared._marks(actors)}) AND fight_date=? "
            "AND source IN ('coop','world_boss','item')", (key, *actors, today))
        if used["n"]:
            raise ValueError("selected activity writers already used today's real challenge quota")
        max_hp = max(1, int(boss["max_hp"]))
        state = uow.query_one("SELECT * FROM activity_boss_state WHERE activity_key=?", (key,))
        hp = max_hp if state is None else max(0, int(state["hp_left"] or 0))
        if state is not None and int(state["max_hp"]) != max_hp:
            hp = int(max_hp * hp / max(1, int(state["max_hp"])))
        if hp <= sum(damage.values()):
            raise ValueError("existing boss HP cannot support the bounded real attacks")
        population = [str(row["user_id"]) for row in boss_rows]
        count = int(uow.query_one("SELECT COUNT(*) AS n FROM activity_boss_fight_log")["n"])
        if count > HISTORY_ROWS:
            raise ValueError("activity fight history already exceeds the 8192-row fixture bound")
        uow.executemany(
            "INSERT INTO activity_boss_fight_log(activity_key,user_id,damage,fight_date,source,create_time) "
            "VALUES(?,?,1,'2000-01-01','coop','2000-01-01 00:00:00')",
            ((key, population[index % len(population)]) for index in range(HISTORY_ROWS - count)))
        last_id = int(uow.query_one("SELECT COALESCE(MAX(id),0) AS n FROM activity_boss_fight_log")["n"])
        snapshot = _snapshot(uow, key, actors, last_id)
    manifest["activity_write"] = {
        "key": key, "boss_name": str(boss["boss_name"]), "actors": actors,
        "damage": damage, "profiles": {actor: profiles[actor] for actor in actors},
        "excluded_personal_readers": sorted(excluded), "max_hp": max_hp, "initial_hp": hp,
        "daily_limit": max(1, int(boss.get("daily_fight_limit") or 3)), "date": today,
        "milestones": list(boss.get("server_milestones") or ()), "snapshot": snapshot,
        "historical_max_id": last_id, "historical_rows": HISTORY_ROWS,
        "compatibility_hits": compatibility,
        "rank_proof": "each distinct writer gains less than its strict next-higher score gap; TOP15 and every rank unchanged",
    }
    manifest.update(fixture_version="mixed-activity-write-v16", phase_specs=phase_specs())
    manifest["feature_table_rows"]["activity_boss_fight_log"] = HISTORY_ROWS
    manifest["limits"] = [line for line in manifest["limits"] if line !=
        "cold-rank remainder scans and global invalidation measured; activity sign/damage mutations and index write cost unmeasured"] + [
        "pure=128 requests; mixed/contention=160 with original 56 profiles/32 training/36 activity reads retained",
        "16 real cooperative boss writes plus 8 mentor/8 partner reads; no sign, item-raid or world-boss writes",
        "8192 synthetic historical fight logs on an old date; no claim that historical damage reconstructs current totals",
        "cooperative damage/rules/attributes remain live; distinct writer gaps preserve ranks and public TOP15",
        "scenario includes cooperative boss damage/index write cost; sign, item-raid, world-boss and reward writes remain unmeasured",
        "only compatibility_hits.json feature:activity may increment once per planned boss request; all other diagnostic keys/business JSON unchanged",
    ]
    manifest.pop("seed_sha256", None)
    manifest["seed_sha256"] = activity._digest(manifest)
    return manifest


def request_plan(manifest, mode, count, phase_number=0):
    specs = phase_specs()
    if (not 0 <= phase_number < len(specs) or mode != specs[phase_number]["mode"]
            or count != specs[phase_number]["requests"]):
        raise ValueError("invalid frozen activity write plan")
    base = cold.request_plan(manifest, mode, cold.phase_specs()[phase_number]["requests"], phase_number)
    if mode == "pure":
        return base
    write = manifest["activity_write"]
    if (len(write["actors"]) != BOSS_WRITES or len(set(write["actors"])) != BOSS_WRITES
            or set(write["damage"]) != set(write["actors"])):
        raise ValueError("activity write plan needs 16 distinct frozen writers")
    extras = []
    for index, actor in enumerate(write["actors"]):
        extras.append({"key": BOSS_KEY, "command": "活动讨伐", "input_text": "活动讨伐",
                       "actor": actor, "expected_damage": write["damage"][actor],
                       "expected_boss_name": write["boss_name"], "expected_max_hp": write["max_hp"],
                       "expected_remaining": write["daily_limit"] - 1,
                       "expected_daily_limit": write["daily_limit"]})
        partner = bool(index % 2)
        reader = manifest["groups"]["training_low"][index // 2] if partner else manifest["groups"]["exact_blob"][index // 2]
        command = "我的道侣" if partner else ("我的徒弟" if index % 4 else "我的师徒")
        extras.append({"key": "my_partner" if partner else "my_mentor", "command": command,
                       "input_text": command, "actor": reader,
                       "success_markers": ["我的道侣", "结契信息"] if partner else
                           ["我的徒弟" if command == "我的徒弟" else "师父", "今日剩余传功次数"],
                       "expected_relation": player.relation._relation_expectation(manifest, reader)})
    plan = []
    for index, row in enumerate(base):
        plan.append(dict(row))
        if index % 4 == 3:
            plan.append(extras[index // 4])
    return [{**row, "number": 890_000_000 + phase_number * 100_000 + index,
             "event_type": "group", "phase_label": mode} for index, row in enumerate(plan)]


def classify_result(request, result):
    if request["key"] != BOSS_KEY:
        return cold.classify_result(request, result)
    base = shared.classify_result(request, result)
    if base["outcome"] in {"boundary", "timeout", "exception", "cooldown", "business_rejection"}:
        return base
    text = "\n".join(map(str, result.get("replies", ()))).replace("\r\n", "\n").replace("\r", "\n")
    from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils.utils import number_to

    if (f"【{request['expected_boss_name']}】" in text
            and f"造成伤害 {number_to(request['expected_damage'])}（单次上限为首领血量 " in text
            and "首领剩余 " in text and f" / {number_to(request['expected_max_hp'])}（" in text
            and f"今日剩余挑战 {request['expected_remaining']} 次" in text):
        return {"outcome": "success", "reason": "real_cooperative_boss_damage"}
    for marker, reason in (("今日挑战次数已用完", "daily_fight_limit"),
                           ("首领已被全服击破", "boss_defeated"),
                           ("首领已被击退", "boss_defeated"),
                           ("讨伐未计入：首领状态已更新，请重新讨伐", "boss_state_changed")):
        if marker in text:
            return {"outcome": "business_rejection", "reason": reason}
    return {"outcome": "unclassified", "reason": "unexpected_cooperative_boss_response"}


def _operation(request):
    return f"activity:boss-coop:{request['actor']}:{request['number']}"


def _check_boss_rows(saved, current, requests, receipts, logs, ledgers):
    from nonebot_plugin_xiuxian_2.infrastructure.database import request_hash

    errors = []
    expected = {_operation(request): request for request in requests}
    if (len(expected) != len(requests) or len({row["actor"] for row in requests}) != len(requests)
            or any(row["actor"] not in saved["actors"]
                   or row["expected_damage"] != saved["damage"].get(row["actor"]) for row in requests)):
        errors.append("boss requests are not distinct permitted actors with their frozen live damage")
    if {row["operation_id"] for row in receipts} != set(expected):
        errors.append("boss receipts do not match all planned fresh cooperative attacks")
    if len(logs) != len(requests):
        errors.append("boss fight logs do not match fresh cooperative attacks")
    expected_damage = Counter({request["actor"]: request["expected_damage"] for request in requests})
    log_damage = Counter()
    for row in logs:
        if row["activity_key"] != saved["key"] or row["fight_date"] != saved["date"] or row["source"] != "coop":
            errors.append("boss fight log identity/date/source mismatch")
        log_damage[str(row["user_id"])] += int(row["damage"])
    if log_damage != expected_damage:
        errors.append("boss fight log actor/damage mismatch")
    if Counter(str(row["user_id"]) for row in logs) != Counter(row["actor"] for row in requests):
        errors.append("boss fight logs are not one per distinct writer")
    by_hp = []
    for row in receipts:
        request = expected.get(row["operation_id"])
        if request is None:
            continue
        payload = json.loads(row["payload"])
        milestones = [[str(item.get("key") or ""), float(item.get("hp_percent", 0))]
                      for item in saved["milestones"]]
        if (payload != [request["actor"], saved["key"], saved["max_hp"], saved["daily_limit"],
                        request["expected_damage"], saved["date"], "coop", milestones, None, 0]
                or row["damage"] != request["expected_damage"] or row["max_hp"] != saved["max_hp"]
                or row["fight_count"] != 1 or row["inventory"] is not None):
            errors.append("boss settlement receipt identity/damage/quota mismatch")
        by_hp.append(row)
    hp = saved["initial_hp"]
    for row in sorted(by_hp, key=lambda row: -int(row["hp_left"])):
        hp -= int(row["damage"])
        if int(row["hp_left"]) != hp:
            errors.append("boss receipt HP chain is not exactly one committed damage per attack")
    if (set(current["writer_damage"]) != set(saved["actors"])
            or set(saved["snapshot"]["writer_damage"]) != set(saved["actors"])):
        errors.append("boss cumulative writer row identities changed")
    for actor, before in saved["snapshot"]["writer_damage"].items():
        after = current["writer_damage"].get(actor)
        if after is None or int(after["total_damage"]) != int(before["total_damage"]) + expected_damage[actor]:
            errors.append("boss cumulative actor damage delta mismatch")
        if not expected_damage[actor]:
            if after != before:
                errors.append("unattacked writer damage row changed")
        elif after is not None and ({key: value for key, value in after.items() if key not in {"total_damage", "update_time"}}
                                    != {key: value for key, value in before.items() if key not in {"total_damage", "update_time"}}):
            errors.append("boss cumulative damage row changed unrelated fields")
    if requests:
        if (current["state"] is None or current["state"]["hp_left"] != hp
                or current["state"]["max_hp"] != saved["max_hp"]):
            errors.append("boss state HP/max HP differs from committed receipt chain")
    elif current["state"] != saved["snapshot"]["state"]:
        errors.append("pure traffic changed boss state")
    if {row["operation_id"] for row in ledgers} != set(expected):
        errors.append("boss ledgers do not match planned cooperative attacks")
    for row in ledgers:
        outcome = json.loads(row["result_json"] or "{}")
        if (row["action"] != LEDGER_ACTION or row["status"] != "applied"
                or outcome.get("status") != "applied" or outcome.get("action") != LEDGER_ACTION
                or outcome.get("operation_id") != row["operation_id"]
                or outcome.get("consumed") != {} or outcome.get("granted") != {}):
            errors.append("boss operation ledger outcome mismatch")
        request = expected.get(row["operation_id"])
        if request is not None and row["request_hash"] != request_hash({"user_id": request["actor"]}):
            errors.append("boss operation ledger request hash mismatch")
    milestones = {(row["activity_key"], row["milestone_key"]): row
                  for row in saved["snapshot"]["milestones"]}
    unlocked = {(saved["key"], str(item.get("key") or f"p{float(item.get('hp_percent', 0))}"))
                for item in saved["milestones"]
                if requests and float(item.get("hp_percent", 0)) > 0
                and 100.0 * hp / saved["max_hp"] <= float(item["hp_percent"])}
    actual_milestones = {(row["activity_key"], row["milestone_key"]): row for row in current["milestones"]}
    if (set(actual_milestones) != set(milestones) | unlocked
            or any(actual_milestones.get(key) != row for key, row in milestones.items())):
        errors.append("boss milestone unlock delta mismatch")
    return errors


def _check_boss_replies(results, receipts, ledgers):
    from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils.utils import number_to

    errors = []
    by_operation = {row["operation_id"]: row for row in receipts}
    ledger_messages = {row["operation_id"]: json.loads(row["result_json"] or "{}").get("data", {}).get("message")
                       for row in ledgers}
    for result in results:
        request = result.get("request", result)
        if request["key"] != BOSS_KEY:
            continue
        operation = _operation(request)
        receipt = by_operation.get(operation)
        text = "\n".join(map(str, result.get("replies", ()))).replace("\r\n", "\n").replace("\r", "\n")
        message = ledger_messages.get(operation)
        if message:
            message = str(message).replace("\r\n", "\n").replace("\r", "\n")
        if (receipt is None or f"首领剩余 {number_to(receipt['hp_left'])} / " not in text
                or not message or message not in text):
            errors.append("boss public reply differs from its own committed HP/ledger result")
    return errors


def check_invariants(context, managers, manifest, results):
    requests = [row.get("request", row) for row in results]
    expected_manifest = {**manifest, "json_snapshot": _expected_json_snapshot(manifest, requests)}
    checked = player.check_invariants(context, managers, expected_manifest, results)
    errors = list(checked["errors"])
    if cold._background_snapshot(context.paths.game_db, manifest["cold_rank_background"]["actors"]) != manifest["cold_rank_background"]["snapshot"]:
        errors.append("activity writes changed cold-rank nonparticipant scores")
    if cold._natal_snapshot(context.paths, manifest["actors"]) != manifest["natal_snapshot"]:
        errors.append("activity writes changed natal display rows")
    saved = manifest["activity_write"]
    with shared._uow(context.paths.game_db, read_only=True) as uow:
        current = _snapshot(uow, saved["key"], saved["actors"], saved["historical_max_id"])
        receipts = uow.query_all("SELECT * FROM activity_boss_settlement_operations WHERE operation_id LIKE ?", (OPERATION_PATTERN,))
        logs = uow.query_all("SELECT * FROM activity_boss_fight_log WHERE id>? ORDER BY id", (saved["historical_max_id"],))
        ledgers = uow.query_all("SELECT operation_id,action,request_hash,status,result_json FROM operation_ledger WHERE operation_id LIKE ?", (OPERATION_PATTERN,))
    for field in ("sign", "sign_logs", "other_damage", "nonwriter_damage", "historical_logs", "other_states", "items"):
        if current[field] != saved["snapshot"][field]:
            errors.append(f"activity writes changed protected {field}")
    boss_requests = [request for request in requests if request["key"] == BOSS_KEY]
    errors.extend(_check_boss_rows(saved, current, boss_requests, receipts, logs, ledgers))
    errors.extend(_check_boss_replies(results, receipts, ledgers))
    counts = Counter()
    for row in results:
        request = row.get("request", row)
        counts[request["key"]] += 1
        if request["key"] in {BOSS_KEY, "profile_info", *(item[0] for item in activity.ACTIVITY_COMMANDS)}:
            classified = classify_result(request, row)
            if row.get("outcome") != "success" or classified["outcome"] != "success":
                errors.append(f"activity write scenario reply oracle failed: {request['key']} {request.get('number')}")
    if results:
        pure = results[0].get("request", results[0]).get("phase_label") == "pure"
        if (len(results) != (128 if pure else 160) or counts["profile_info"] != 56
                or counts["stone_training"] != (0 if pure else 32)
                or counts[BOSS_KEY] != (0 if pure else BOSS_WRITES)
                or any(counts[key] != activity.ACTIVITY_PER_COMMAND_PER_PHASE for key, *_ in activity.ACTIVITY_COMMANDS)):
            errors.append("activity write scenario frozen path counts changed")
    return {**checked, "ok": not errors, "errors": errors,
            "activity_write_checks": {"per_command": dict(counts), "fresh_boss_attacks": len(receipts),
                "historical_log_rows": saved["historical_rows"], "new_fight_logs": len(logs),
                "compatibility_hit_delta": {"feature:activity": len(boss_requests)},
                "rank_proof": saved["rank_proof"], "runtime_oracle_sql": 0},
            "invariants": checked["invariants"] + "; natal/background unchanged; exact boss HP/damage/quota/log/receipt/ledger deltas; sign/nonwriter ranks/TOP15 protected; exact planned feature:activity diagnostic delta with every other JSON hash protected"}
