"""Frozen distinct-score viewers and real dual-field gameplay on one 1M DB."""
from __future__ import annotations

from collections import Counter
import hashlib
import json

from tests import mixed_command_fixture as shared

PARTICIPANTS = 128
ACTIVITY_POPULATION = 8192
REQUESTED_STONE = 1000
EXP_GAIN = 100
LEVEL = "元神境初期"
PREFIX = "training:89"
READS = tuple(spec for spec in shared.READ_SPECS if spec[0] in {
    "profile_info", "asset_overview", "mind_state", "cultivation_value", "backpack", "activity_rank",
})


def phase_specs():
    specs = [{"mode": "cold", "concurrency": limit, "requests": 128, "arrival_rate": 10}
             for limit in (1, 5, 10, 20)]
    specs += [{"mode": "readmostly", "concurrency": limit, "requests": 128, "arrival_rate": 20}
              for limit in (5, 10)]
    specs += [{"mode": "mixed", "concurrency": limit, "requests": 128, "arrival_rate": 20}
              for limit in (1, 5, 10, 20)]
    specs += [{"mode": "contention", "concurrency": 10, "requests": 128, "arrival_rate": 20}]
    specs += [{"mode": "mixed", "concurrency": 20, "requests": 256, "arrival_rate": rate}
              for rate in (35, 50)]
    return specs


def _digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, ensure_ascii=True).encode()).hexdigest()


def seed(context, managers, *, participants=PARTICIPANTS):
    if participants != PARTICIPANTS:
        raise ValueError("rank gameplay requires exactly 128 existing participants")
    from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils.item_json import Items
    from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils.data_source import jsondata
    from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils.xiuxian2_handle import OtherSet
    from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_config import XiuConfig
    from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_activity import activity_config
    from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils.shared_read_cache import process_read_cache

    paths = context.paths
    ids = [1 + index * 999_999 // (participants - 1) for index in range(participants)]
    users = shared._rows(paths.game_db,
        f"SELECT id,user_id FROM user_xiuxian WHERE id IN ({shared._marks(ids)}) ORDER BY id", ids)
    actors = [str(row["user_id"]) for row in users]
    if len(actors) != participants or len(set(actors)) != participants:
        raise ValueError("rank gameplay participant IDs are missing")
    if shared._rows(paths.game_db, "SELECT COUNT(*) AS n FROM user_xiuxian")[0]["n"] != 1_000_000:
        raise ValueError("rank gameplay requires the sole retained 1M fixture")
    population = actors + [str(row["user_id"]) for row in shared._rows(paths.game_db,
        f"SELECT user_id FROM user_xiuxian WHERE user_id NOT IN ({shared._marks(actors)}) "
        "ORDER BY id LIMIT ?", (*actors, ACTIVITY_POPULATION - participants))]
    if len(set(population)) != ACTIVITY_POPULATION:
        raise ValueError("activity population does not contain 8192 distinct existing users")
    cap = int(OtherSet().set_closing_type(LEVEL)) * XiuConfig().closing_exp_upper_limit
    multiplier = float(jsondata.root_data()["天灵根"]["type_speeds"] * jsondata.level_data()[LEVEL]["spend"])
    initial = {actor: {"exp": 832_000 + index * 256, "stone": 500_000_000 + index * 1_000_037}
               for index, actor in enumerate(actors)}
    for values in initial.values():
        values["power"] = int(values["exp"] * multiplier + .5)
    if any(row["exp"] + 128 * EXP_GAIN >= cap for row in initial.values()):
        raise ValueError("frozen gameplay seed would hit the actual cultivation cap")
    catalog, items = Items(), []
    for category in ("法器", "防具", "功法", "丹药", "药材"):
        selected = sorted(catalog.get_data_by_item_type((category,)), key=int)[:4]
        if len(selected) != 4:
            raise ValueError("frozen equipment catalog is incomplete")
        items.extend((int(item), catalog.get_data_by_item_id(int(item))) for item in selected)
    config_path = activity_config.CONFIG_PATH.resolve()
    if not config_path.is_relative_to(paths.game_db.parent.resolve()) or activity_config.CONFIG_DB_PATH.exists():
        raise ValueError("activity projection is outside the isolated restore contract")
    original = activity_config._load_config_projection_readonly()
    original_sha = hashlib.sha256(config_path.read_bytes()).hexdigest() if config_path.is_file() else None
    config = shared._activity_seed_config(original)
    activity_config._save_config_projection(config)
    keys = list(dict.fromkeys([activity_config._activity_config_key(config), *(
        str(row["key"]) for row in config.get("gameplay_activities", ()) if row.get("key"))]))
    with shared._uow(paths.game_db) as uow:
        if uow.query_one("SELECT operation_id FROM stone_training_operations WHERE operation_id LIKE ? LIMIT 1", (PREFIX + "%",)):
            raise ValueError("rank gameplay operation namespace is not empty")
        if uow.query_one("SELECT operation_id FROM operation_ledger WHERE operation_id LIKE ? LIMIT 1", (PREFIX + "%",)):
            raise ValueError("rank gameplay ledger namespace is not empty")
        for actor in actors:
            values = initial[actor]
            uow.execute("UPDATE user_xiuxian SET exp=?,stone=?,power=?,level=?,root_type='天灵根',"
                        "hp=1000,mp=1000,atk=100,user_stamina=100 WHERE user_id=?",
                        (values["exp"], values["stone"], values["power"], LEVEL, actor))
            uow.execute("DELETE FROM back WHERE user_id=?", (actor,))
            weapon = armor = 0
            for item, definition in items:
                equipped = 0
                if definition.get("item_type") == "法器" and not weapon:
                    weapon, equipped = item, 1
                elif definition.get("item_type") == "防具" and not armor:
                    armor, equipped = item, 1
                uow.execute("INSERT INTO back(user_id,goods_id,goods_name,goods_type,goods_num,bind_num,state,"
                            "create_time,update_time,all_num) VALUES(?,?,?,?,20,0,?,?,?,20)",
                            (actor, item, definition["name"], definition["type"], equipped, shared.ANCHOR, shared.ANCHOR))
            if uow.query_one("SELECT user_id FROM BuffInfo WHERE user_id=?", (actor,)) is None:
                uow.execute("INSERT INTO BuffInfo(user_id,faqi_buff,armor_buff) VALUES(?,?,?)", (actor, weapon, armor))
            else:
                uow.execute("UPDATE BuffInfo SET faqi_buff=?,armor_buff=? WHERE user_id=?", (weapon, armor, actor))
        uow.executemany("INSERT INTO activity_user(user_id,sign_days,total_sign_days,last_sign_date,create_time,update_time) "
            "VALUES(?,?,?,?,?,?) ON CONFLICT(user_id) DO UPDATE SET sign_days=excluded.sign_days,"
            "total_sign_days=excluded.total_sign_days,last_sign_date=excluded.last_sign_date",
            ((actor, index % 7 + 1, index + 10, "2026-01-01", shared.ANCHOR, shared.ANCHOR)
             for index, actor in enumerate(population)))
        for key in keys:
            for table, fields, score in (
                ("activity_point_balance", ("points", "total_points"), lambda i: (i * 13 + 100, i * 17 + 200)),
                ("activity_pass_balance", ("exp", "total_exp", "level"), lambda i: (i * 19 + 100, i * 23 + 200, i % 10)),
                ("activity_boss_damage", ("total_damage",), lambda i: (i * 1234 + 100,)),
            ):
                columns = ",".join(fields)
                updates = ",".join(field + "=excluded." + field for field in fields)
                uow.executemany(f"INSERT INTO {table}(activity_key,user_id,{columns},update_time) "
                    f"VALUES(?,?,{shared._marks(fields)},?) ON CONFLICT(activity_key,user_id) DO UPDATE SET {updates},update_time=excluded.update_time",
                    ((key, actor, *score(index), shared.ANCHOR) for index, actor in enumerate(population)))
    managers[0]._clear_read_cache()
    process_read_cache.clear()
    manifest = {"fixture_version": "rank-gameplay-v2", "users": 1_000_000, "actors": actors,
        "initial": initial, "level": LEVEL, "exp_cap": cap, "power_multiplier": multiplier,
        "inventory": shared._inventory(paths.game_db, actors), "activity_keys": keys,
        "activity_population": len(population), "activity_population_sha256": _digest(population),
        "activity_config_sha256": _digest(config), "activity_original_config_sha256": _digest(original),
        "activity_original_projection_sha256": original_sha,
        "activity_projection_sha256": hashlib.sha256(config_path.read_bytes()).hexdigest(),
        "limits": ["128 distinct-score viewers; 8192 populated activity users; not all feature tables",
                   "cold means result-cache miss, not cold OS page cache", "50/s is short offered-load exploration, not sustained success capacity"]}
    manifest["feature_table_rows"] = {table: shared._rows(paths.game_db, f'SELECT COUNT(*) AS n FROM "{table}"')[0]["n"]
        for table in ("back", "BuffInfo", "activity_user", "activity_point_balance", "activity_pass_balance", "activity_boss_damage")}
    manifest["seed_sha256"] = _digest(manifest)
    return manifest


def request_plan(manifest, mode, count, phase_number=0):
    if mode not in {"cold", "readmostly", "mixed", "contention"} or not 8 <= count <= 256 or not 0 <= phase_number < 90:
        raise ValueError("invalid frozen rank gameplay plan")
    result, reads, writes = [], 0, 0
    actors = manifest["actors"]
    for index in range(count):
        write = mode == "readmostly" and index % 10 == 9 or mode in {"mixed", "contention"} and index % 2 == 1
        if write:
            actor = actors[0] if mode == "contention" else actors[writes % len(actors)]
            row = {"key": "stone_training", "command": "灵石修炼", "input_text": f"灵石修炼 {REQUESTED_STONE}",
                   "requested_stone": REQUESTED_STONE, "exp_gain": EXP_GAIN}
            writes += 1
        else:
            actor = actors[reads % len(actors)]
            key, command, markers = shared.READ_SPECS[0] if mode == "cold" else READS[reads % len(READS)]
            row = {"key": key, "command": command, "input_text": command, "success_markers": list(markers)}
            reads += 1
        result.append({**row, "actor": actor, "number": 890_000_000 + phase_number * 100_000 + index,
                       "event_type": "group"})
    return result


def classify_result(request, result):
    if request["key"] != "stone_training":
        return shared.classify_result(request, result)
    base = shared.classify_result(request, result)
    if base["outcome"] in {"boundary", "timeout", "exception", "cooldown"}:
        return base
    text = "\n".join(map(str, result.get("replies", ())))
    if "修炼结束，共增加修为：100,消耗灵石：1000" in text and "无需重复提交" not in text:
        return {"outcome": "success", "reason": "fresh_dual_field_training"}
    for marker, reason in (("你的灵石还不够呢", "stone_insufficient"), ("当前修为已到达上限", "exp_capped"),
                           ("修为与操作前不一致", "exp_changed"), ("灵石余额与操作前不一致", "stone_changed")):
        if marker in text:
            return {"outcome": "business_rejection", "reason": reason}
    return {"outcome": "unclassified", "reason": "unexpected_training_response"}


def check_invariants(context, managers, manifest, results):
    paths, actors = context.paths, manifest["actors"]
    errors, successes, writes = [], {}, Counter()
    planned, outcomes = {}, {}
    for result in results:
        request = result.get("request", result)
        if request["key"] != "stone_training":
            continue
        operation = f"training:{request['number']}:stone:{request['actor']}"
        planned[operation] = request
        outcomes[operation] = result
        if result.get("outcome") == "success":
            successes[operation] = request
            writes[request["actor"]] += 1
    receipts = shared._rows(paths.game_db, "SELECT operation_id,payload,result_json FROM stone_training_operations WHERE operation_id LIKE ?", (PREFIX + "%",))
    if {row["operation_id"] for row in receipts} != set(successes):
        errors.append("training receipts do not match fresh successful requests")
    powers = {}
    for receipt in receipts:
        request = successes.get(receipt["operation_id"])
        if request is None:
            continue
        saved = json.loads(receipt["result_json"])
        powers.setdefault(request["actor"], []).append(saved.get("power"))
        if (json.loads(receipt["payload"]) != [request["actor"], REQUESTED_STONE, manifest["exp_cap"], manifest["power_multiplier"]]
                or saved.get("exp_gain") != EXP_GAIN or saved.get("stone_cost") != REQUESTED_STONE):
            errors.append("training receipt identity/reward mismatch")
    rows = shared._rows(paths.game_db, f"SELECT user_id,exp,stone,power FROM user_xiuxian WHERE user_id IN ({shared._marks(actors)})", actors)
    for row in rows:
        actor = str(row["user_id"])
        initial = manifest["initial"][actor]
        if (row["exp"] != initial["exp"] + EXP_GAIN * writes[actor]
                or row["stone"] != initial["stone"] - REQUESTED_STONE * writes[actor]
                or row["stone"] < 0):
            errors.append(f"training exact balance mismatch for {actor}")
        expected_power = int((initial["exp"] + EXP_GAIN * writes[actor]) * manifest["power_multiplier"] + .5) if writes[actor] else initial["power"]
        if row["power"] != expected_power or (writes[actor] and max(powers.get(actor, [None])) != expected_power):
            errors.append(f"training power/receipt mismatch for {actor}")
    ledgers = shared._rows(paths.game_db, "SELECT operation_id,status,result_json FROM operation_ledger WHERE operation_id LIKE ? AND action='buff.stone_training'", (PREFIX + "%",))
    applied = {row["operation_id"]: row for row in ledgers if row["status"] == "applied"}
    if set(applied) != set(successes) or any(row["operation_id"] not in planned for row in ledgers):
        errors.append("training ledger identity/outcome mismatch")
    for operation, ledger in applied.items():
        saved = json.loads(ledger["result_json"])
        receipt = next((json.loads(row["result_json"]) for row in receipts if row["operation_id"] == operation), {})
        if (saved.get("status") != "applied" or saved.get("operation_id") != operation
                or saved.get("action") != "buff.stone_training"
                or saved.get("data", {}).get("exp_gain") != EXP_GAIN
                or saved.get("data", {}).get("stone_cost") != REQUESTED_STONE
                or saved.get("data", {}).get("power") != receipt.get("power")):
            errors.append(f"training ledger reward mismatch: {operation}")
    for ledger in ledgers:
        if ledger["status"] == "applied":
            continue
        result = outcomes.get(ledger["operation_id"], {})
        saved = json.loads(ledger["result_json"] or "{}")
        data = saved.get("data", {})
        if (ledger["status"] != "rejected" or result.get("outcome") != "business_rejection"
                or result.get("reason") != (data.get("reason") or data.get("status"))):
            errors.append("training rejected ledger does not match a specific business refusal")
    if shared._inventory(paths.game_db, actors) != manifest["inventory"]:
        errors.append("read/training traffic changed inventory")
    if len(rows) != len(actors) or shared._rows(paths.game_db, "SELECT COUNT(*) AS n FROM user_xiuxian")[0]["n"] != 1_000_000:
        errors.append("fixture user identity/count changed")
    for database in (paths.game_db, paths.player_db):
        checks = shared._rows(database, "PRAGMA quick_check")
        if any(list(row.values()) != ["ok"] for row in checks):
            errors.append("fixture quick_check failed")
    return {"ok": not errors, "errors": errors, "fresh_training": len(successes),
            "receipts": len(receipts), "applied_ledgers": len(applied), "invariants":
            "exact exp/stone/power deltas, actor/receipt/ledger identity and rewards, inventory unchanged, sole 1M user count"}
