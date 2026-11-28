"""Populated sect reads and real practice writes on the retained sole 1M DB.

Setup and bounded consistency scans run outside measured traffic. The outer
driver owns snapshot restore, dispatch, drain, resource guards and transport.
"""
from __future__ import annotations

from collections import Counter
from copy import deepcopy
import hashlib
import json
import re

from tests import mixed_command_fixture as shared


PARTICIPANTS = 128
POPULATION = 8192
SECT_COUNT = 128
MEMBERS_PER_SECT = 64
MAX_TABLE_ROWS = 32768
WALLET = 1_000_000_000
BACKGROUND_WALLET = 10**30
MATERIALS = 8_000_000_000
MESSAGE_BASE = 895_000_000
RECEIPT_PREFIX = "sect:895%"
MUTABLE_USER_FIELDS = ("stone", "atkpractice", "hppractice", "mppractice")
MEMBER_POSITION = 11
COMMANDS = (
    ("my_sect", "我的宗门"),
    ("sect_buildings", "宗门建设"),
    ("sect_attack_upgrade", "升级攻击修炼"),
    ("sect_health_upgrade", "升级元血修炼"),
    ("sect_mana_upgrade", "升级灵海修炼"),
    ("asset_overview", "我的资产总览"),
)
PRACTICES = {
    "sect_attack_upgrade": ("attack", "atkpractice", "attack_practice_upgrade", "攻击"),
    "sect_health_upgrade": ("health", "hppractice", "health_practice_upgrade", "元血"),
    "sect_mana_upgrade": ("mana", "mppractice", "mana_practice_upgrade", "灵海"),
}
FROZEN_TABLES = {
    "game_db": ("back", "BuffInfo", "bank_accounts", "sect_task_state", "operation_ledger",
                "economy_log", "sect_task_claim_operations", "sect_task_settlement_operations"),
    "player_db": ("statistics", "arena", "training", "status"),
    "trade_db": ("guishi_info",),
    "impart_db": ("xiuxian_impart",),
}


def phase_specs():
    return [{"label": mode, "mode": mode, "requests": 128,
             "concurrency": cap, "arrival_rate": rate, "order": "sect-shared-v21"}
            for mode, cap, rate in (("pure", 10, 20), ("mixed", 20, 35),
                                    ("contention", 20, 50))]


def _encoded(value):
    return json.dumps(value, sort_keys=True, ensure_ascii=True, separators=(",", ":"),
                      default=lambda value: {"blob": bytes(value).hex()}).encode("ascii")


def _digest(value):
    return hashlib.sha256(_encoded(value)).hexdigest()


def _json_row(row):
    return {key: {"sqlite_blob_hex": bytes(value).hex()} if isinstance(value, (bytes, bytearray, memoryview))
            else value for key, value in dict(row).items()}


def _raw_value(value):
    if isinstance(value, dict) and set(value) == {"sqlite_blob_hex"}:
        return bytes.fromhex(value["sqlite_blob_hex"])
    return value


def _number(value):
    from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils.utils import number_to
    return number_to(value)


def _quote(name):
    return '"' + str(name).replace('"', '""') + '"'


def _stream_signature(cursor, *, max_rows=MAX_TABLE_ROWS):
    digest, count = hashlib.sha256(), 0
    digest.update(_encoded([column[0] for column in cursor.description]) + b"\n")
    try:
        while rows := cursor.fetchmany(128):
            for row in rows:
                count += 1
                if count > max_rows:
                    return {"excluded": "row_bound", "rows_at_least": count}
                digest.update(_encoded(dict(row)) + b"\n")
    finally:
        cursor.close()
    return {"rows": count, "sha256": digest.hexdigest()}


def _table_signature(uow, table, *, where="", parameters=()):
    name = _quote(table)
    columns = uow.query_all(f"PRAGMA table_info({name})")
    # A declared key also supports WITHOUT ROWID tables and avoids reliance on
    # incidental query plans when the runtime adds a controlled index.
    keys = [row["name"] for row in sorted(columns, key=lambda row: int(row["pk"] or 0))
            if int(row["pk"] or 0)]
    ordering = ",".join(map(_quote, keys)) if keys else "rowid"
    cursor = uow.connection.execute(
        f"SELECT * FROM {name} {where} ORDER BY {ordering} LIMIT ?",
        (*parameters, MAX_TABLE_ROWS + 1),
    )
    return _stream_signature(cursor)


def _frozen_tables(paths):
    snapshots, exclusions = {}, {}
    for database_name, selected in FROZEN_TABLES.items():
        with shared._uow(getattr(paths, database_name), read_only=True) as uow:
            tables = uow.query_all("SELECT name FROM sqlite_master WHERE type='table' "
                                   "AND name NOT LIKE 'sqlite_%' ORDER BY name")
            for row in tables:
                table = row["name"]
                key = database_name + ":" + table
                if database_name == "game_db" and table in {
                    "user_xiuxian", "sects", "sect_practice_operations", "xiuxian_numeric_rank_generation"
                }:
                    exclusions[key] = "checked by explicit selected-row/receipt/balance/generation oracle"
                    continue
                if table not in selected:
                    exclusions[key] = "outside the declared bounded feature-table snapshot"
                    continue
                signature = _table_signature(uow, table)
                if "excluded" in signature:
                    exclusions[key] = signature
                else:
                    snapshots[key] = signature
            if database_name == "game_db":
                snapshots[database_name + ":existing_practice_receipts"] = _table_signature(
                    uow, "sect_practice_operations", where="WHERE operation_id NOT LIKE ?",
                    parameters=(RECEIPT_PREFIX,),
                )
    return {"tables": snapshots, "exclusions": exclusions,
            "message_db": "transport queue is drained and accounted by the outer driver"}


def _selected_users(database, population_ids, actors):
    from nonebot_plugin_xiuxian_2.core.numeric import normalize_user_row

    actor_set = set(actors)
    selected, background = {}, hashlib.sha256()
    cursor_rows, background_rows = 0, 0
    with shared._uow(database, read_only=True) as uow:
        cursor = uow.connection.execute(
            f"SELECT * FROM user_xiuxian WHERE id IN ({shared._marks(population_ids)}) ORDER BY id",
            population_ids,
        )
        background.update(_encoded([column[0] for column in cursor.description]) + b"\n")
        try:
            while rows := cursor.fetchmany(128):
                for row in rows:
                    cursor_rows += 1
                    if cursor_rows > len(population_ids):
                        raise ValueError("sect selected-user projection exceeded its declared bound")
                    row = dict(row)
                    # The baseline materializes these complete rows. Reject a
                    # nonrepresentable fixture currency before dispatching load.
                    normalize_user_row(row)
                    actor = str(row["user_id"])
                    if actor in actor_set:
                        if actor in selected:
                            raise ValueError("sect participant identity is not unique")
                        selected[actor] = _json_row(row)
                    else:
                        background_rows += 1
                        background.update(_encoded(row) + b"\n")
        finally:
            cursor.close()
    if cursor_rows != len(population_ids) or set(selected) != actor_set:
        raise ValueError("sect selected-user projection is incomplete")
    return {"actors": selected,
            "background": {"rows": background_rows, "sha256": background.hexdigest()}}


def _assign_members(actors, population, sect_ids, *, members_per_sect=MEMBERS_PER_SECT):
    if (not actors or len(set(actors)) != len(actors)
            or len(set(population)) != len(population) or population[:len(actors)] != actors
            or len(sect_ids) < 4 or len(set(sect_ids)) != len(sect_ids)
            or len(population) != len(sect_ids) * members_per_sect):
        raise ValueError("invalid bounded sect population")
    members = {sect: [] for sect in sect_ids}
    for index, actor in enumerate(actors):
        members[sect_ids[index % 4]].append(actor)
    background = iter(population[len(actors):])
    for sect in sect_ids:
        if len(members[sect]) > members_per_sect:
            raise ValueError("sect actors exceed the declared membership capacity")
        members[sect].extend(next(background) for _ in range(members_per_sect - len(members[sect])))
    if next(background, None) is not None:
        raise ValueError("sect population has unused background users")
    return members


def _period():
    from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_sect
    return xiuxian_sect.sect_application.current_task_period()


def _rules():
    from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_sect

    config = xiuxian_sect.config
    unit, cost = int(config["等级建设度"]), int(config["LEVLECOST"]["0"])
    tasks = [(name, dict(value)) for name, value in sorted(config["宗门任务"].items())
             if isinstance(value, dict) and isinstance(value.get("desc"), str) and value["desc"]]
    positions = xiuxian_sect.jsondata.sect_config_data()
    member = str(MEMBER_POSITION)
    if (unit <= 0 or cost <= 0 or not tasks or "0" not in positions or member not in positions
            or positions["0"].get("max_count") != 1 or positions[member].get("max_count") != 0):
        raise ValueError("sect shared scenario requires valid live practice/task/position config")
    return {"scale_unit": unit, "stone_cost": cost, "materials_cost": int(cost * 10),
            "contribution": unit * 100, "tasks": tasks,
            "position_titles": {key: positions[key]["title"] for key in ("0", member)}}


def _generation(database):
    rows = shared._rows(database, "SELECT generation,exp_generation,stone_generation "
                        "FROM xiuxian_numeric_rank_generation WHERE singleton=1")
    if len(rows) != 1:
        raise ValueError("sect shared scenario requires real exact-rank generation state")
    return {key: int(value) for key, value in rows[0].items()}


def _sects(database):
    rows = shared._rows(database, "SELECT rowid AS _fixture_rowid,* FROM sects ORDER BY rowid LIMIT ?",
                        (MAX_TABLE_ROWS + 1,))
    if len(rows) > MAX_TABLE_ROWS:
        raise ValueError("sect table exceeds the bounded complete projection")
    return {str(row["sect_id"]): _json_row(row) for row in rows}


def _read_oracles(actors, user_rows, sect_rows, rules, tasks):
    from nonebot_plugin_xiuxian_2.core.numeric import as_int_like

    ordered = sorted((row for row in sect_rows.values() if row["sect_owner"] is not None),
                     key=lambda row: -as_int_like(_raw_value(row["sect_scale"])))
    ranks = {int(row["sect_id"]): index for index, row in enumerate(ordered, 1)}
    result = {}
    for actor in actors:
        user = user_rows[actor]
        sect = sect_rows[str(user["sect_id"])]
        scale = as_int_like(sect["sect_scale"])
        result[actor] = {
            "sect_id": int(user["sect_id"]), "sect_name": sect["sect_name"],
            "user_name": user["user_name"],
            "owner_name": user_rows[str(sect["sect_owner"])]["user_name"],
            "position": rules["position_titles"][str(user["sect_position"] or 0)],
            "owner": str(sect["sect_owner"]) == actor,
            "rank": ranks[int(user["sect_id"])], "members": MEMBERS_PER_SECT,
            "max_members": min(20 + scale // 50_000_000, 100),
            "scale": _number(scale), "scale_level": scale // rules["scale_unit"],
            "contribution": _number(rules["contribution"]),
            "treasury": _number(int(sect["sect_used_stone"])),
            "power": _number(int(sect["combat_power"])),
            "task": tasks[actor][0], "task_desc": tasks[actor][1]["desc"],
        }
    return result


def seed(context, managers, *, participants=PARTICIPANTS):
    from nonebot_plugin_xiuxian_2.core.numeric import bind_sqlite_param
    from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils.shared_read_cache import process_read_cache

    if participants != PARTICIPANTS:
        raise ValueError("sect shared requires exactly 128 existing participants")
    database, rules, period = context.paths.game_db, _rules(), _period()
    ids = [1 + index * 999_999 // (participants - 1) for index in range(participants)]
    selected = shared._rows(database, "SELECT id,user_id FROM user_xiuxian "
                            f"WHERE id IN ({shared._marks(ids)}) ORDER BY id", ids)
    actors = [str(row["user_id"]) for row in selected]
    if len(actors) != participants or len(set(actors)) != participants:
        raise ValueError("sect shared indexed participant identities are missing or duplicated")
    users = int(shared._rows(database, "SELECT COUNT(*) AS n FROM user_xiuxian")[0]["n"])
    if users != 1_000_000:
        raise ValueError("sect shared requires the sole retained 1M synthetic fixture")
    background = shared._rows(database, "SELECT id,user_id FROM user_xiuxian "
                              f"WHERE id NOT IN ({shared._marks(ids)}) ORDER BY id LIMIT ?",
                              (*ids, POPULATION - participants))
    population = actors + [str(row["user_id"]) for row in background]
    population_ids = [int(row["id"]) for row in selected + background]
    sect_ids = tuple(8_950_001 + index for index in range(SECT_COUNT))
    members = _assign_members(actors, population, sect_ids)
    actor_set, actor_index = set(actors), {actor: index for index, actor in enumerate(actors)}
    actor_tasks = {}
    if (WALLET < 3 * rules["stone_cost"] or MATERIALS < 32 * rules["materials_cost"]):
        raise ValueError("live sect costs exceed the frozen scenario's sufficient resources")
    with shared._uow(database) as uow:
        if uow.query_one("SELECT operation_id FROM sect_practice_operations WHERE operation_id LIKE ? LIMIT 1",
                         (RECEIPT_PREFIX,)):
            raise ValueError("sect shared receipt namespace is not empty")
        if uow.query_one(f"SELECT sect_id FROM sects WHERE sect_id IN ({shared._marks(sect_ids)}) LIMIT 1",
                         sect_ids):
            raise ValueError("sect shared reserved sect namespace is not empty")
        if uow.query_one(f"SELECT user_id FROM user_xiuxian WHERE sect_id IN ({shared._marks(sect_ids)}) LIMIT 1",
                         sect_ids):
            raise ValueError("sect shared reserved sect IDs already have dangling members")
        for index, sect_id in enumerate(sect_ids):
            group = members[sect_id]
            scale = max(4_000_000_000, rules["scale_unit"] * 100) + index * 50_000_000
            if min(20 + scale // 50_000_000, 100) < len(group):
                raise ValueError("sect shared seed exceeds the real membership limit")
            uow.execute("INSERT INTO sects(sect_id,sect_name,sect_owner,sect_scale,sect_used_stone,"
                        "sect_materials,join_open,closed,combat_power,sect_fairyland,elixir_room_level) "
                        "VALUES(?,?,?,?,?,?,1,0,?,0,0)",
                        (sect_id, f"SyntheticSectShared{index:03d}", group[0], scale,
                         2_000_000_000 + index, MATERIALS, 100_000 + index))
            for member_index, actor in enumerate(group):
                position = 0 if member_index == 0 else MEMBER_POSITION
                stone = WALLET if actor in actor_set else BACKGROUND_WALLET + index * MEMBERS_PER_SECT + member_index
                changed = uow.execute("UPDATE user_xiuxian SET sect_id=?,sect_position=?,sect_contribution=?,"
                                      "stone=? WHERE user_id=?", (sect_id, position, rules["contribution"],
                                       bind_sqlite_param(stone, exact=True), actor))
                if changed.rowcount != 1:
                    raise ValueError("sect shared member identity is not unique")
                if actor in actor_set:
                    changed = uow.execute("UPDATE user_xiuxian SET atkpractice=0,hppractice=0,"
                                          "mppractice=0 WHERE user_id=?", (actor,))
                    if changed.rowcount != 1:
                        raise ValueError("sect shared actor identity is not unique")
                task = rules["tasks"][(index * MEMBERS_PER_SECT + member_index) % len(rules["tasks"])]
                if actor in actor_set:
                    actor_tasks[actor] = task
                uow.execute("INSERT INTO sect_task_state(user_id,sect_id,task_key,task_data,period,status,"
                            "progress,target,accepted_at,updated_at,completed_at) VALUES(?,?,?,?,?,'accepted',"
                            "0,1,?,?,NULL) ON CONFLICT(user_id,period) DO UPDATE SET sect_id=excluded.sect_id,"
                            "task_key=excluded.task_key,task_data=excluded.task_data,status='accepted',progress=0,"
                            "target=1,accepted_at=excluded.accepted_at,updated_at=excluded.updated_at,completed_at=NULL",
                            (actor, sect_id, task[0], json.dumps(task[1], sort_keys=True, ensure_ascii=False),
                             period, period + " 00:00:00", period + " 00:00:00"))
    selected_snapshot = _selected_users(database, population_ids, actors)
    sect_rows = _sects(database)
    manifest = {
        "fixture_version": "mixed-sect-shared-v21", "users": users, "actors": actors,
        "population_ids": population_ids, "population_sha256": _digest(population),
        "population": POPULATION, "sect_ids": list(sect_ids), "members_per_sect": MEMBERS_PER_SECT,
        "hot_groups": [[actor for actor in actors if actor_index[actor] % 4 == index]
                       for index in range(4)],
        "wallet": WALLET, "materials": MATERIALS, "rules": rules, "period": period,
        "phase_specs": phase_specs(), "initial_users": selected_snapshot,
        "initial_sects": sect_rows, "initial_generation": _generation(database),
        "oracle": _read_oracles(actors, selected_snapshot["actors"], sect_rows, rules, actor_tasks),
        "frozen": _frozen_tables(context.paths),
        "feature_table_rows": {
            "sects": len(sect_rows), "seeded_sects": SECT_COUNT, "seeded_members": POPULATION,
            "sect_task_state": int(shared._rows(database, "SELECT COUNT(*) AS n FROM sect_task_state")[0]["n"]),
        },
        "limits": [
            "128 real actors; 128 sects x64 existing synthetic members and 8192 current-period real-config tasks",
            "128 actor wallets1e9;8064 background wallets exact1e30+ordinal, not retained ambiguous REAL currency",
            "no new main users and no duplicate million-row fixture; all phase requests share one instance and DB",
            "pure128 reads; mixed/contention128 each contain32 practice writes and32 unchanged real asset controls",
            "mixed distinct-user writes; contention32 writes share one sect and11 users across three different matchers",
            "each user/matcher appears at most once per phase; real10s cooldown remains enabled",
            "no exact-rank profile requests, reward settlement, task settlement, membership or sect-scale writes in traffic",
            "128 actor full rows and8064 background member full-row hash checked; remaining991808 users lack traffic full-column checks",
            "outer driver verifies original1M exp/stone digest after restore; this does not prove traffic full-column invariance",
            "declared nonuser tables frozen only within32768rows/table; other/oversized tables explicitly excluded; messageDB owned by driver",
            "bounded setup/check scans and restore IO are outside measured traffic; no request-time oracle SQL",
            "short384-request representative sample is not all674 coverage or sustained50 successful requests/s",
        ],
    }
    if _period() != period:
        raise ValueError("sect task period changed during setup; do not compare across days")
    manifest["seed_sha256"] = _digest(manifest)
    process_read_cache.clear()
    for manager in managers:
        for name in ("_clear_read_cache", "_clear_field_list_cache"):
            clear = getattr(manager, name, None)
            if callable(clear):
                clear()
    return manifest


def _operation(request):
    action = PRACTICES[request["key"]][2]
    return f"sect:{request['number']}:{action}:{request['sect_id']}"


def request_plan(manifest, mode, count, phase_number=0):
    specs = phase_specs()
    if (not 0 <= phase_number < len(specs) or mode != specs[phase_number]["mode"]
            or count != specs[phase_number]["requests"] or len(manifest["actors"]) != PARTICIPANTS):
        raise ValueError("invalid frozen sect shared plan")
    commands, writes, result = dict(COMMANDS), list(PRACTICES), []
    actors, hot = manifest["actors"], manifest["hot_groups"][0]
    if len(hot) != 32 or len(set(actors)) != PARTICIPANTS or len(set(hot)) != 32:
        raise ValueError("invalid frozen sect shared actor groups")
    for index in range(count):
        if mode == "pure":
            key = "my_sect" if index % 2 == 0 else "sect_buildings"
            actor = actors[index]
        else:
            slot, ordinal = index % 4, index // 4
            key = ("my_sect", "sect_buildings", "asset_overview", writes[ordinal % 3])[slot]
            # Mixed writers are actors0..31; contention uses actors32,36..72.
            # Their user/matcher keys never collide across the short phases.
            actor = (hot[8 + ordinal // 3] if mode == "contention" and slot == 3 else
                     hot[8 + ordinal % 11] if mode == "contention" and slot == 2 else
                     actors[ordinal] if slot == 3 else actors[32 + (ordinal * 3 + slot) % 96])
        command = commands[key]
        expected = deepcopy(manifest["oracle"][actor])
        row = {"key": key, "command": command,
               "input_text": command + " 1" if key in PRACTICES else command,
               "actor": actor, "number": MESSAGE_BASE + phase_number * 100_000 + index,
               "event_type": "group", "phase_label": mode, "sect_id": expected["sect_id"],
               "performance_role": "unchanged_control" if key == "asset_overview" else "optimized_target",
               "expected_sect": expected}
        result.append(row)
    by_actor = Counter(row["actor"] for row in result if row["key"] in PRACTICES)
    by_sect = Counter(row["sect_id"] for row in result if row["key"] in PRACTICES)
    for row in result:
        row["expected_sect"]["materials_choices"] = sorted({_number(manifest["materials"] - index *
            manifest["rules"]["materials_cost"]) for index in range(by_sect[row["sect_id"]] + 1)})
        row["expected_sect"]["wallet_choices"] = sorted({_number(manifest["wallet"] - index *
            manifest["rules"]["stone_cost"]) for index in range(by_actor[row["actor"]] + 1)})
        if row["key"] in PRACTICES:
            row["stone_cost"] = manifest["rules"]["stone_cost"]
            row["materials_cost"] = manifest["rules"]["materials_cost"]
    return result


def _reply_text(result):
    text = "\n".join(map(str, result.get("replies", ()))).replace("\r\n", "\n").replace("\r", "\n")
    return re.sub(r"(?m)^\[CQ:at,qq=[0-9]+\][ \t]*", "", text)


def _line(text, value):
    return len(re.findall(r"(?m)^" + re.escape(value) + r"\s*$", text)) == 1


def _field(text, label, value):
    return (len(re.findall(r"(?m)^" + re.escape(label) + r"\s*$", text)) == 1
            and len(re.findall(r"(?m)^" + re.escape(label) + r"\n> " + re.escape(str(value)) + r"\s*$", text)) == 1)


def classify_result(request, result):
    classified = shared.classify_result(request, result)
    if classified["outcome"] in {"boundary", "timeout", "exception", "cooldown"}:
        return classified
    text, key = _reply_text(result), request["key"]
    expected = request["expected_sect"]
    if key in PRACTICES:
        if "已经完成" in text or "无需重复" in text:
            return {"outcome": "unclassified", "reason": "unexpected_practice_replay"}
        label = PRACTICES[key][3]
        valid = (_line(text, "升级成功！") and _field(text, f"道友当前{label}修炼等级", 1)
                 and _field(text, "消耗灵石", _number(request["stone_cost"]) + "枚")
                 and _field(text, "消耗宗门资材", _number(request["materials_cost"])))
        if not valid:
            rejections = (("宗门资材不足", "materials_insufficient"),
                          ("灵石不够", "stone_insufficient"),
                          ("灵石余额不足", "stone_insufficient"),
                          ("玩家灵石不足", "stone_insufficient"),
                          ("修炼等级已变化", "level_changed"),
                          ("所选修炼等级与升级前不一致", "level_changed"),
                          ("贡献度修炼等级", "contribution_limit"),
                          ("不满足使用资材", "position_forbidden"),
                          ("已达到当前宗门", "sect_level_limit"))
            for marker, reason in rejections:
                if marker in text:
                    return {"outcome": "business_rejection", "reason": reason}
    elif key == "my_sect":
        valid = (all(_field(text, label, value) for label, value in (
            ("宗门名讳", expected["sect_name"]), ("宗门编号", expected["sect_id"]),
            ("宗主", expected["owner_name"]), ("道友职位", expected["position"]),
            ("宗门状态", "开放加入"),
            ("宗门人数", f'{expected["members"]}/{expected["max_members"]}'),
            ("宗门建设度", expected["scale"]), ("宗门排名", expected["rank"]),
            ("宗门贡献度", expected["contribution"]), ("宗门战力", expected["power"]),
            ("炼体堂", "暂无"), ("宗门丹房", "暂无")))
            and _line(text, f'**{expected["user_name"]}所在宗门**')
            and sum(_field(text, "宗门拥有资材", value) for value in expected["materials_choices"]) == 1)
        if expected["owner"]:
            valid = valid and _line(text, f'宗门储备：{expected["treasury"]}枚灵石')
    elif key == "sect_buildings":
        valid = (all(_line(text, value) for value in (
            "【宗门建设】", f'宗门：{expected["sect_name"]}（{expected["members"]}/{expected["max_members"]}人）',
            f'建设度：{expected["scale"]}', f'宗门储备：{expected["treasury"]}灵石',
            f'个人贡献：{expected["contribution"]}', "丹房：未建设", "炼体堂：未建设",
            f'修炼上限：{expected["scale_level"]}级', f'{expected["task"]}：{expected["task_desc"]}'))
            and len(re.findall(r"(?m)^宗门资材：.*$", text)) == 1
            and sum(_line(text, f"宗门资材：{value}") for value in expected["materials_choices"]) == 1)
    elif key == "asset_overview":
        valid = (_line(text, "【我的资产总览】")
                 and _line(text, f'宗门：{expected["sect_name"]}（贡献 {expected["contribution"]}）')
                 and len(re.findall(r"(?m)^灵石：.*$", text)) == 1
                 and sum(_line(text, f"灵石：{value}") for value in expected["wallet_choices"]) == 1)
    else:
        valid = False
    return ({"outcome": "success", "reason": "exact_sect_reply"} if valid else
            {"outcome": "unclassified", "reason": "sect_shared_reply_mismatch"})


def _expected_changes(manifest, results, receipts):
    users, sects = deepcopy(manifest["initial_users"]["actors"]), deepcopy(manifest["initial_sects"])
    planned, successful, errors = {}, set(), []
    for result in results:
        request = result.get("request", result)
        outcome = result.get("outcome")
        if isinstance(outcome, dict):
            outcome = outcome.get("outcome")
        if outcome is None:
            outcome = classify_result(request, result)["outcome"]
        if outcome in {"boundary", "timeout", "exception", "unclassified", "not_sent", "resource_aborted"}:
            errors.append(f"sect request {request['number']} has outcome {outcome}")
        if request["key"] not in PRACTICES:
            continue
        operation = _operation(request)
        if operation in planned:
            errors.append("sect plan repeats an operation ID")
        planned[operation] = request
        if outcome == "success":
            successful.add(operation)
    if len(receipts) != len({row["operation_id"] for row in receipts}):
        errors.append("sect practice receipt identity is not unique")
    if {row["operation_id"] for row in receipts} != successful:
        errors.append("sect practice receipts do not exactly match successful writes")
    for receipt in receipts:
        request = planned.get(receipt["operation_id"])
        if request is None:
            errors.append("unexpected sect practice receipt")
            continue
        practice, column, _, _ = PRACTICES[request["key"]]
        expected = {"user_id": request["actor"], "sect_id": request["sect_id"], "practice_type": practice,
                    "from_level": 0, "to_level": 1, "stone_cost": request["stone_cost"],
                    "materials_cost": request["materials_cost"]}
        actual = {key: str(receipt[key]) if key in {"user_id", "practice_type"} else int(receipt[key])
                  for key in expected}
        if actual != expected or receipt["operation_id"] not in successful:
            errors.append("sect practice receipt payload differs from its real request")
            continue
        actor = request["actor"]
        users[actor]["stone"] -= request["stone_cost"]
        users[actor][column] += 1
        sects[str(request["sect_id"])]["sect_materials"] -= request["materials_cost"]
    target_sects = {str(row["sect_id"]) for row in users.values()}
    if (any(row["stone"] < 0 or any(row[column] not in {0, 1} for column in MUTABLE_USER_FIELDS[1:])
            for row in users.values()) or any(int(sects[key]["sect_materials"] or 0) < 0 for key in target_sects)):
        errors.append("sect expected balances/levels are invalid")
    return users, sects, len(successful), errors


def check_invariants(context, managers, manifest, results):
    errors = []
    receipts = shared._rows(context.paths.game_db, "SELECT operation_id,user_id,sect_id,practice_type,"
                            "from_level,to_level,stone_cost,materials_cost FROM sect_practice_operations "
                            "WHERE operation_id LIKE ? ORDER BY operation_id LIMIT ?",
                            (RECEIPT_PREFIX, 129))
    users, sects, successes, mutation_errors = _expected_changes(manifest, results, receipts)
    errors.extend(mutation_errors)
    current = _selected_users(context.paths.game_db, manifest["population_ids"], manifest["actors"])
    if current["actors"] != users:
        errors.append("sect actor full rows differ from exact committed practice receipts")
    if current["background"] != manifest["initial_users"]["background"]:
        errors.append("sect background member full rows changed")
    if _sects(context.paths.game_db) != sects:
        errors.append("complete sect rows differ from exact practice material debits")
    if _frozen_tables(context.paths) != manifest["frozen"]:
        errors.append("bounded frozen feature/receipt tables or their exclusion inventory changed")
    generation = _generation(context.paths.game_db)
    expected_generation = {key: value + (successes if key in {"generation", "stone_generation"} else 0)
                           for key, value in manifest["initial_generation"].items()}
    if generation != expected_generation:
        errors.append("sect practice writes changed incorrect numeric rank generations")
    if _period() != manifest["period"]:
        errors.append("sect task period changed across the frozen scenario")
    count = int(shared._rows(context.paths.game_db, "SELECT COUNT(*) AS n FROM user_xiuxian")[0]["n"])
    if count != manifest["users"]:
        errors.append("sect traffic changed the1M main user count")
    return {"ok": not errors, "errors": errors, "successful_practice_writes": successes,
            "practice_receipts": len(receipts), "generation_after": generation,
            "full_actor_rows": len(users), "background_full_rows": current["background"]["rows"],
            "frozen_tables": sorted(manifest["frozen"]["tables"]),
            "exclusions": manifest["frozen"]["exclusions"],
            "checks": ["exact permanent receipt payloads and success correspondence", "full selected user rows",
                       "all sect fields and precise shared materials", "frozen bounded feature tables",
                       "no negative balances or repeated field upgrades", "exact generation deltas",
                       "current task period", "1M main user count"],
            "limits": "remaining991808 users have no traffic full-column check; original1M rank digest restored by driver"}
