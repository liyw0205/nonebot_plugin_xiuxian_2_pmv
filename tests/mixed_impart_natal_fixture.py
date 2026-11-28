"""Populated natal/impart reads and real training on the retained sole 1M DB.

Compose existing participant, bag and activity setup. New checks are independent
of the runtime training result and preserve full selected rows across stores.
"""
from __future__ import annotations

from copy import deepcopy
import json
import re

from tests import mixed_rank_gameplay_fixture as base
from tests import mixed_sect_shared_fixture as bounded


shared = base.shared
PARTICIPANTS = base.PARTICIPANTS
MESSAGE_BASE = 898_000_000
STONE_PREFIX = "training:898%"
IMPART_PREFIX = "impart-training:898%"
COMMANDS = (
    ("natal_info", "我的本命法宝"), ("natal_exists", "觉醒本命法宝"),
    ("world_info", "虚神界信息"), ("world_training", "虚神界修炼"),
    ("asset_overview", "我的资产总览"), ("stone_training", "灵石修炼"),
)
NATAL_DEFAULTS = {
    "form": 1, "name": "", "level": 1, "exp": 17, "max_exp": 100,
    "effect1_type": 1, "effect1_base_value": .025, "effect1_level": 1,
    "effect2_type": 0, "effect2_base_value": 0., "effect2_level": 0,
    "effect3_type": 0, "effect3_base_value": 0., "effect3_level": 0,
    "fate_revive_count": 0, "immortal_revive_count": 0, "invincible_gain_count": 0,
    "nirvana_revive_count": 0, "soul_return_revive_count": 0, "charge_status": 0,
    "soul_summon_count": "{}", "enlightenment_count": "{}",
}
DAILY_DEFAULTS = {"pk_num": 7, "impart_num": 10, "exp_used": 0, "exp_count": 0,
                  "exp_load": 0, "exp_gain": 0}


def phase_specs():
    return [{"label": mode, "mode": mode, "requests": 128,
             "concurrency": cap, "arrival_rate": rate, "order": "impart-natal-v24"}
            for mode, cap, rate in (("pure", 10, 20), ("mixed", 20, 20),
                                    ("contention", 20, 35))]


def _text(value):
    value = value.replace("\r\n", "\n").replace("\r", "\n")
    return re.sub(r"^\[CQ:at,qq=[0-9]+\][ \t]*", "", value).strip()


def _full_rows(database, table, actors):
    rows = shared._rows(database, f'SELECT * FROM "{table}" WHERE user_id IN ({shared._marks(actors)}) '
                        "ORDER BY user_id", actors)
    result = {str(row["user_id"]): bounded._json_row(row) for row in rows}
    if len(rows) != len(actors) or len(result) != len(actors):
        raise ValueError(f"V24 requires exactly one complete {table} row per actor")
    return result


def _frozen(paths):
    selected = {
        "game_db": ("back", "BuffInfo", "sects", "activity_user", "activity_point_balance",
                    "activity_pass_balance", "activity_boss_damage", "stone_training_operations",
                    "operation_ledger", "natal_training_operations", "natal_reawaken_operations",
                    "natal_effect_upgrade_operations", "natal_engraving_operations", "natal_forget_operations",
                    "impart_training_operations"),
        "player_db": ("statistics", "natal_treasure", "natal_awaken_operations", "avatar",
                      "impart_pk_state", "impart_project_members", "impart_project_meta"),
        "impart_db": ("impart_cards", "impart_battle_batch_operations"),
    }
    signatures, exclusions = {}, {}
    for name, tables in selected.items():
        with shared._uow(getattr(paths, name), read_only=True) as uow:
            existing = {row["name"] for row in uow.query_all(
                "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")}
            for table in sorted(existing):
                key = name + ":" + table
                if table not in tables:
                    exclusions[key] = "outside declared bounded frozen tables; selected user/impart/daily rows checked separately"
                    continue
                prefix = (STONE_PREFIX if table in {"stone_training_operations", "operation_ledger"} else
                          IMPART_PREFIX if table == "impart_training_operations" else None)
                signature = bounded._table_signature(
                    uow, table, where="WHERE operation_id NOT LIKE ?" if prefix else "",
                    parameters=(prefix,) if prefix else ())
                (exclusions if "excluded" in signature else signatures)[key] = signature
    return {"tables": signatures, "exclusions": exclusions}


def _natal_text(row, rules):
    value = row["effect1_base_value"] + (row["effect1_level"] - 1) * rules["bleed_growth"]
    periodic = rules["periodic_base"] + row["level"] * rules["periodic_growth"]
    return (f"【{row['name']}】  总等级：{row['level']}/{rules['max_level']}\n"
            f"  └─ 养成进度：{row['exp']}/{row['max_exp']}\n"
            f"  └─ {rules['bleed_name']} Lv.{row['effect1_level']}/{rules['max_effect']}：{round(value * 100, 2)}%\n"
            f"\n  ◎ 道韵：每4回合对所有敌方造成当前生命 {round(periodic * 100, 2)}% 的真实伤害。")


def _world_info_text(impart, daily, rules):
    # Fixed real LV6 keeps the realm label independently checkable.
    return (f"现位于：元神幻境（LV 6）\n虚神界修炼时间：{impart['exp_day']} 分钟\n"
            f"修炼效率：{int(rules['lv_bonus'] * 100)}% \n"
            f"今日神魂承载：{daily['exp_load']}%/100%\n今日可探索次数：{daily['impart_num']}\n"
            f"今日可对决次数：{daily['pk_num']}\n思恋结晶：{int(impart['stone_num_exact'])}")


def _training_oracle(manifest, actor):
    rules, initial = manifest["training_rules"], manifest["initial"][actor]
    daily = manifest["initial_daily"][actor]
    impart = manifest["initial_impart"][actor]
    if daily != {"user_id": actor, **DAILY_DEFAULTS}:
        raise ValueError("V24 independent gain oracle requires the frozen fresh daily state")
    rate = (rules["root_rate"] * rules["realm_rate"] * (1 + rules["main_rate"])
            * (1 + rules["main_closing"]) * (1 + rules["card_bonus"]) * (1 + rules["lv_bonus"]))
    per_minute = int(rules["closing_exp"] * rate)
    gain = min(per_minute, manifest["exp_cap"] - initial["exp"])
    if gain <= 0 or impart["exp_day"] < 1:
        raise ValueError("V24 seed would reject real one-minute virtual training")
    exp, power = initial["exp"] + gain, int(round((initial["exp"] + gain) * rules["root_rate"] * rules["realm_rate"]))
    efficiency = int((rules["root_rate"] + rules["main_rate"] + rules["main_closing"]
                      + rules["card_bonus"] + rules["lv_bonus"]) * 100)
    reply = (f"虚神界修炼结束，共修炼1分钟，本次增加修为：{bounded._number(gain)}（修炼效率：{efficiency}%）\n"
             f"神魂承载：0% -> 5%（本次修炼+5%，实际+5%）\n今日虚神界修炼收益：{bounded._number(gain)}\n今日修炼次数：1次")
    return {"gain": gain, "power": power, "reply": reply,
            "saved": [impart["exp_day"] - 1, exp, 1, 1, 5, gain]}


def seed(context, managers, *, participants=PARTICIPANTS):
    from nonebot_plugin_xiuxian_2.core.numeric import bind_sqlite_param, normalize_user_row
    from nonebot_plugin_xiuxian_2.features.impart.repository import ImpartRepository
    from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_impart_pk as world
    from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_natal_treasure.natal_data import NatalTreasure
    from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_natal_treasure import natal_config
    from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils.item_json import Items
    from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils.shared_read_cache import process_read_cache
    from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils.xiuxian2_handle import XIUXIAN_IMPART_BUFF

    manifest = base.seed(context, managers, participants=participants)
    actors, paths = manifest["actors"], context.paths
    catalog = Items()
    main_id = min(map(int, catalog.get_data_by_item_type(("功法",))))
    main = catalog.get_data_by_item_id(main_id)
    # Perform real compatibility repair once outside measured traffic; batch
    # complete rows thereafter so every measured read starts with valid schema.
    NatalTreasure(actors[0])._ensure_record()
    with shared._uow(paths.player_db) as uow:
        uow.execute("CREATE TABLE IF NOT EXISTS impart_pk_daily(user_id TEXT PRIMARY KEY,"
                    "pk_num INTEGER NOT NULL DEFAULT 7,impart_num INTEGER NOT NULL DEFAULT 10,"
                    "exp_used INTEGER NOT NULL DEFAULT 0,exp_count INTEGER NOT NULL DEFAULT 0,"
                    "exp_load INTEGER NOT NULL DEFAULT 0,exp_gain INTEGER NOT NULL DEFAULT 0)")
        fields = tuple(NATAL_DEFAULTS)
        assignments = ",".join(f'"{field}"=excluded."{field}"' for field in fields)
        for index, actor in enumerate(actors):
            natal = {**NATAL_DEFAULTS, "name": f"SyntheticNatalV24{actor}", "level": 1 + index % 3,
                     "effect1_level": 1 + index % 3,
                     "effect1_base_value": natal_config.EFFECT_BASE_AND_GROWTH[natal_config.NatalEffectType.BLEED]["min_value"]}
            uow.execute(f'INSERT INTO natal_treasure(user_id,{",".join(fields)}) '
                        f'VALUES(?,{shared._marks(fields)}) ON CONFLICT(user_id) DO UPDATE SET {assignments}',
                        (actor, *(natal[field] for field in fields)))
            uow.execute("INSERT INTO impart_pk_daily(user_id,pk_num,impart_num,exp_used,exp_count,exp_load,exp_gain) "
                        "VALUES(?,7,10,0,0,0,0) ON CONFLICT(user_id) DO UPDATE SET pk_num=7,impart_num=10,"
                        "exp_used=0,exp_count=0,exp_load=0,exp_gain=0", (actor,))
            # Prevent legacy JSON initialization during the measured daily read.
            world.impart_pk.data[actor] = world.impart_pk._default_user_data(index + 1)
    repository = ImpartRepository(paths.impart_db)
    for actor in actors:
        repository.state(actor, ensure=True)
    with shared._uow(paths.impart_db) as uow:
        for index, actor in enumerate(actors):
            count = int(uow.query_one("SELECT COUNT(*) AS n FROM xiuxian_impart WHERE user_id=?", (actor,))["n"])
            if count != 1:
                raise ValueError("V24 preserves legacy duplicates; requires one selected canonical impart row")
            crystals = 10**30 + index
            resets = {**XIUXIAN_IMPART_BUFF._default_fields, "impart_lv": 6, "exp_day": 600 + index,
                      "stone_num": bind_sqlite_param(crystals, exact=True), "stone_num_exact": str(crystals),
                      "stone_num_precision_state": "exact"}
            fields = tuple(resets)
            uow.execute("UPDATE xiuxian_impart SET " + ",".join(f'"{field}"=?' for field in fields)
                        + " WHERE user_id=?", (*(resets[field] for field in fields), actor))
    with shared._uow(paths.game_db) as uow:
        for table, prefix in (("impart_training_operations", IMPART_PREFIX),
                              ("stone_training_operations", STONE_PREFIX), ("operation_ledger", STONE_PREFIX)):
            if uow.query_one(f'SELECT operation_id FROM "{table}" WHERE operation_id LIKE ? LIMIT 1', (prefix,)):
                raise ValueError("V24 reserved training receipt namespace is not empty")
        uow.executemany("UPDATE BuffInfo SET main_buff=?,blessed_spot=0 WHERE user_id=?", ((main_id, actor) for actor in actors))
        # This fixture measures training, not an unrelated active closing session.
        uow.executemany("UPDATE user_cd SET type=0 WHERE user_id=?", ((actor,) for actor in actors))
    root = world.jsondata.root_data()["天灵根"]["type_speeds"]
    realm = world.jsondata.level_data()[manifest["level"]]["spend"]
    rules = {"closing_exp": world.XiuConfig().closing_exp, "root_rate": root, "realm_rate": realm,
             "main_id": main_id, "main_rate": main["ratebuff"], "main_closing": main["clo_exp"],
             "card_bonus": 0., "lv_bonus": .30}
    if (world.XU_SOUL_LOAD_LIMIT, world.XU_SOUL_LOAD_PER_USE, world.XU_SOUL_LOAD_TIME_STEP) != (100, 5, 150):
        raise ValueError("V24 independent training oracle requires the real frozen soul-load rules")
    if world.xu_impart_lv_bonus(6) != 6 * .05 or world.effective_xu_impart_exp_up(0) != 0:
        raise ValueError("V24 independent level/card oracle no longer matches configuration")
    rules["lv_bonus"] = 6 * .05
    rows = _full_rows(paths.game_db, "user_xiuxian", actors)
    for row in rows.values():
        normalize_user_row(row)
    manifest.update(fixture_version="mixed-impart-natal-v24", phase_specs=phase_specs(), training_rules=rules,
                    initial_users=rows, initial_impart=_full_rows(paths.impart_db, "xiuxian_impart", actors),
                    initial_daily=_full_rows(paths.player_db, "impart_pk_daily", actors),
                    initial_natal=_full_rows(paths.player_db, "natal_treasure", actors),
                    frozen=_frozen(paths), initial_generation=bounded._generation(paths.game_db),
                    legacy_daily_sha256=bounded._digest(world.impart_pk.data),
                    natal_rules={"max_level": natal_config.MAX_TREASURE_LEVEL,
                                 "max_effect": natal_config.MAX_EFFECT_LEVEL_ALL_EFFECTS,
                                 "bleed_growth": natal_config.EFFECT_BASE_AND_GROWTH[natal_config.NatalEffectType.BLEED]["growth"],
                                 "bleed_name": natal_config.EFFECT_NAME_MAP[natal_config.NatalEffectType.BLEED],
                                 "periodic_base": natal_config.PERIODIC_TRUE_DAMAGE_BASE,
                                 "periodic_growth": natal_config.PERIODIC_TRUE_DAMAGE_GROWTH_PER_LEVEL})
    manifest["training_oracles"] = {actor: _training_oracle(manifest, actor) for actor in actors}
    manifest["feature_table_rows"].update({"natal_selected": len(actors), "impart_selected": len(actors), "daily_selected": len(actors)})
    manifest["limits"].extend([
        "V24:128 complete natal, canonical impart and daily records; real exact1e30+ordinal crystal balances",
        "384 requests/version across6matchers:304success+80already_awakened refusals; never a natal write",
        "pure128reads; mixed/contention each16virtual training+16stone training+96reads;32distinct writers per phase",
        "contention overlaps virtual info readers with16virtual writers; this is read/write, not repeated same-user write/write",
        "virtual training changes exp/exp_day/daily and receipts; stone control changes exp/stone and ledger; independent gains and full selected rows checked",
        "all selected statistics frozen: authoritative virtual training SqlRepository has no statistics increment",
        "legacy JSON daily entries populated before traffic; real rules/CD/DI/handlers preserved, only transport mocked",
        "selected canonical rows only; legacy duplicate reconciliation, missing-row repair, natal mutation and other virtualworld actions not runtime sampled",
        "bounded frozen tables32768rows; unselected user full columns and excluded tables unproven; outer1M exp/stone digest restored",
        "short representative sample is not all674matchers,41background entries or sustained50success/s",
    ])
    manifest.pop("seed_sha256", None)
    manifest["seed_sha256"] = bounded._digest(manifest)
    managers[0]._clear_read_cache()
    process_read_cache.clear()
    return manifest


def request_plan(manifest, mode, count, phase_number=0):
    specs = phase_specs()
    if (not 0 <= phase_number < len(specs) or mode != specs[phase_number]["mode"]
            or count != 128 or len(manifest["actors"]) != PARTICIPANTS
            or len(set(manifest["actors"])) != PARTICIPANTS):
        raise ValueError("invalid frozen V24 phase plan")
    actors, commands, result = manifest["actors"], dict(COMMANDS), []
    distribution = (("natal_info", 32), ("natal_exists", 32), ("world_info", 32), ("asset_overview", 32)) if mode == "pure" else (
        ("natal_info", 24), ("natal_exists", 24), ("world_info", 32), ("world_training", 16),
        ("stone_training", 16), ("asset_overview", 16))
    queues = {key: list(range(amount)) for key, amount in distribution}
    while any(queues.values()):
        for key, _ in distribution:
            if not queues[key]:
                continue
            ordinal = queues[key].pop(0)
            if key == "natal_info":
                actor = actors[(0, 64, 104)[phase_number] + ordinal]
            elif key == "natal_exists":
                actor = actors[(32, 88, 0)[phase_number] + ordinal]
            elif key == "world_info":
                actor = actors[96 + ordinal % 16] if mode == "contention" else actors[phase_number * 32 + ordinal]
            elif key == "world_training":
                actor = actors[(64 if mode == "mixed" else 96) + ordinal]
            elif key == "stone_training":
                actor = actors[(80 if mode == "mixed" else 112) + ordinal]
            else:
                actor = actors[(96 if mode == "mixed" else 112 if mode == "contention" else 64) + ordinal % (16 if phase_number else 32)]
            command = commands[key]
            request = {"key": key, "command": command, "input_text": command, "actor": actor,
                       "number": MESSAGE_BASE + phase_number * 100_000 + len(result), "event_type": "group",
                       "phase_label": mode, "performance_role": "unchanged_control" if key in {"asset_overview", "stone_training"} else "optimized_target"}
            if key == "stone_training":
                request.update(input_text=command + " 1000", requested_stone=1000, exp_gain=100)
            elif key == "world_training":
                request.update(input_text=command + " 1", training=deepcopy(manifest["training_oracles"][actor]))
            elif key.startswith("natal_"):
                desc = _natal_text(manifest["initial_natal"][actor], manifest["natal_rules"])
                request["expected_text"] = desc if key == "natal_info" else (
                    "你已拥有本命法宝：\n\n" + desc + "\n\n如需重新随机重塑，请发送【重塑本命法宝】。\n"
                    "注意：重塑会重置法宝等级、所有道纹等级和所有复活/无敌次数统计。")
            elif key == "world_info":
                impart, daily = manifest["initial_impart"][actor], manifest["initial_daily"][actor]
                states = [(impart, daily)]
                if mode == "contention":
                    gain = manifest["training_oracles"][actor]["gain"]
                    states.append(({**impart, "exp_day": impart["exp_day"] - 1},
                                   {**daily, "exp_used": 1, "exp_count": 1, "exp_load": 5, "exp_gain": gain}))
                request["expected_texts"] = [_world_info_text(i, d, manifest["training_rules"]) for i, d in states]
            else:
                wallet = manifest["initial"][actor]["stone"]
                request["wallet_choices"] = [bounded._number(wallet)]
                if mode == "contention":
                    request["wallet_choices"].append(bounded._number(wallet - 1000))
            result.append(request)
    return result


def classify_result(request, result):
    classified = shared.classify_result(request, result)
    if classified["outcome"] in {"boundary", "timeout", "exception", "cooldown"}:
        return classified
    key, replies = request["key"], result.get("replies", ())
    text = _text(replies[0]) if len(replies) == 1 and isinstance(replies[0], str) else None
    if key == "stone_training":
        return (base.classify_result(request, result) if text else
                {"outcome": "unclassified", "reason": "v24_training_reply_count"})
    if key == "natal_exists" and text == _text(request["expected_text"]):
        return {"outcome": "business_rejection", "reason": "already_awakened"}
    valid = False
    if text:
        if key == "natal_info":
            valid = text == _text(request["expected_text"])
        elif key == "world_info":
            valid = text in [_text(value) for value in request["expected_texts"]]
        elif key == "world_training":
            valid = text == _text(request["training"]["reply"])
        elif key == "asset_overview":
            valid = text.splitlines()[0] == "【我的资产总览】" and sum(
                bounded._line(text, "灵石：" + value) for value in set(request["wallet_choices"])) == 1
    return ({"outcome": "success", "reason": "exact_v24_reply"} if valid else
            {"outcome": "unclassified", "reason": "v24_reply_mismatch"})


def _operation(request):
    return (f"training:{request['number']}:stone:{request['actor']}" if request["key"] == "stone_training" else
            f"impart-training:{request['number']}:{request['actor']}")


def _expected_changes(manifest, results, world_receipts, stone_receipts, ledgers):
    users, impart, daily = (deepcopy(manifest[key]) for key in ("initial_users", "initial_impart", "initial_daily"))
    successes, errors = {}, []
    for result in results:
        request = result.get("request", result)
        classified = classify_result(request, result)
        outcome = classified["outcome"]
        if (result.get("outcome") not in (None, outcome, classified)
                or outcome not in {"success", "business_rejection"}
                or outcome == "business_rejection" and request["key"] != "natal_exists"):
            errors.append(f"V24 unexpected outcome for {request['number']}: {classified}")
        if request["key"] not in {"world_training", "stone_training"} or outcome != "success":
            continue
        operation = _operation(request)
        if operation in successes:
            errors.append("V24 repeated write operation")
        successes[operation] = request
    expected_world = {key for key, row in successes.items() if row["key"] == "world_training"}
    expected_stone = set(successes) - expected_world
    for label, rows, expected in (("world", world_receipts, expected_world), ("stone", stone_receipts, expected_stone)):
        if len(rows) != len({row["operation_id"] for row in rows}) or {row["operation_id"] for row in rows} != expected:
            errors.append(f"V24 {label} receipts do not exactly match successful requests")
    for receipt in world_receipts:
        request = successes.get(receipt["operation_id"])
        if request is None or request["key"] != "world_training":
            continue
        actor, expected = request["actor"], request["training"]
        saved = json.loads(receipt["result_json"])
        if json.loads(receipt["payload"]) != [actor, 1] or saved != expected["saved"]:
            errors.append("V24 virtual training receipt identity/gain/daily mismatch")
        users[actor]["exp"] += expected["gain"]
        users[actor]["power"] = expected["power"]
        impart[actor]["exp_day"] -= 1
        daily[actor].update(exp_used=1, exp_count=1, exp_load=5, exp_gain=expected["gain"])
    stone_by_id = {row["operation_id"]: row for row in stone_receipts}
    for operation in expected_stone:
        request, receipt = successes[operation], stone_by_id.get(operation)
        actor = request["actor"]
        if receipt is None:
            continue
        saved = json.loads(receipt["result_json"])
        power = int((manifest["initial"][actor]["exp"] + 100) * manifest["power_multiplier"] + .5)
        if (json.loads(receipt["payload"]) != [actor, 1000, manifest["exp_cap"], manifest["power_multiplier"]]
                or saved.get("exp_gain") != 100 or saved.get("stone_cost") != 1000 or saved.get("power") != power):
            errors.append("V24 stone training receipt identity/gain/power mismatch")
        users[actor]["exp"] += 100
        users[actor]["stone"] -= 1000
        users[actor]["power"] = power
    if len(ledgers) != len(expected_stone) or {row["operation_id"] for row in ledgers} != expected_stone:
        errors.append("V24 stone ledger identity mismatch")
    for ledger in ledgers:
        saved = json.loads(ledger["result_json"])
        receipt = stone_by_id.get(ledger["operation_id"])
        data = json.loads(receipt["result_json"]) if receipt else {}
        if (ledger["status"] != "applied" or saved.get("status") != "applied"
                or saved.get("action") != "buff.stone_training" or saved.get("operation_id") != ledger["operation_id"]
                or any(saved.get("data", {}).get(key) != data.get(key) for key in ("exp_gain", "stone_cost", "power"))):
            errors.append("V24 stone ledger reward mismatch")
    if any(row["stone"] < 0 for row in users.values()) or any(row["exp_day"] < 0 for row in impart.values()):
        errors.append("V24 negative balance/time")
    return users, impart, daily, len(expected_world), len(expected_stone), errors


def check_invariants(context, managers, manifest, results):
    from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_impart_pk as world

    paths, actors = context.paths, manifest["actors"]
    world_receipts = shared._rows(paths.game_db, "SELECT operation_id,payload,result_json FROM impart_training_operations "
                                  "WHERE operation_id LIKE ? ORDER BY operation_id LIMIT 129", (IMPART_PREFIX,))
    stone_receipts = shared._rows(paths.game_db, "SELECT operation_id,payload,result_json FROM stone_training_operations "
                                  "WHERE operation_id LIKE ? ORDER BY operation_id LIMIT 129", (STONE_PREFIX,))
    ledgers = shared._rows(paths.game_db, "SELECT operation_id,status,result_json FROM operation_ledger "
                           "WHERE operation_id LIKE ? ORDER BY operation_id LIMIT 129", (STONE_PREFIX,))
    users, impart, daily, world_success, stone_success, errors = _expected_changes(
        manifest, results, world_receipts, stone_receipts, ledgers)
    for database, table, expected in ((paths.game_db, "user_xiuxian", users),
                                      (paths.impart_db, "xiuxian_impart", impart),
                                      (paths.player_db, "impart_pk_daily", daily),
                                      (paths.player_db, "natal_treasure", manifest["initial_natal"])):
        if _full_rows(database, table, actors) != expected:
            errors.append(f"V24 complete selected {table} rows differ from exact successful receipts")
    if _frozen(paths) != manifest["frozen"]:
        errors.append("V24 bounded frozen tables/statistics/inventory or exclusions changed")
    if bounded._digest(world.impart_pk.data) != manifest["legacy_daily_sha256"]:
        errors.append("V24 legacy daily JSON state changed during traffic")
    if shared._inventory(paths.game_db, actors) != manifest["inventory"]:
        errors.append("V24 training/read traffic changed populated inventory")
    if int(shared._rows(paths.game_db, "SELECT COUNT(*) AS n FROM user_xiuxian")[0]["n"]) != 1_000_000:
        errors.append("V24 changed sole1M main user count")
    generation = bounded._generation(paths.game_db)
    initial = manifest["initial_generation"]
    if (generation["exp_generation"] - initial["exp_generation"] != world_success + stone_success
            or generation["stone_generation"] - initial["stone_generation"] != stone_success):
        errors.append("V24 exact exp/stone rank generation deltas mismatch")
    return {"ok": not errors, "errors": errors, "fresh_world_training": world_success,
            "fresh_stone_training": stone_success, "world_receipts": len(world_receipts),
            "stone_receipts": len(stone_receipts), "full_selected_rows_per_store": len(actors),
            "frozen_tables": sorted(manifest["frozen"]["tables"]), "exclusions": manifest["frozen"]["exclusions"],
            "generation_after": generation,
            "invariants": "exact independent gains/receipts, full selected user/impart/daily/natal rows, live balances/time, frozen stats/inventory, legacy daily and rank-generation deltas"}
