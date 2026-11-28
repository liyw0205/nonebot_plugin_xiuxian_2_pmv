"""Bounded real sect/package rewards; one retained synthetic database.

Shop reads exercise checked-profile reuse. Rewards and receipts are checked independently;
random accessory instances are validated rather than compared by a frozen hash.
"""
from __future__ import annotations

from collections import Counter
from copy import deepcopy
from datetime import datetime
import json
import random
import re

from tests import mixed_sect_shared_fixture as sect
from tests import mixed_shared_query_fixture as transport


shared = sect.shared
PARTICIPANTS = 128
EXPECTED_USERS = 1_000_000
MESSAGE_BASE = 899_000_000
COMMANDS = (("shop_view", "宗门商店"), ("elixir_claim", "宗门丹药领取"),
            ("ordinary_package", "使用"), ("accessory_package", "使用"))
PACKAGE_IDS = {"ordinary_package": 18050, "accessory_package": 18126}
SEED_TAG = "reward-batch-v30"
AFFIX_RANGES = {"气血": (.02, .05), "抗暴": (.01, .03), "防御": (.01, .03),
                "会心": (.01, .03), "会心伤害": (.02, .05), "攻击": (.02, .05), "速度": (4, 9)}


def phase_specs():
    return [{"label": mode, "mode": mode, "requests": 96, "concurrency": cap,
             "arrival_rate": rate, "order": SEED_TAG}
            for mode, cap, rate in (("pure", 1, 10), ("mixed", 10, 20), ("contention", 20, 35))]


def _namespace(value):
    return str(value).startswith(("sect:899", "package-reward:899"))


def _fixed_rewards(package):
    if package.get("roll", 0):
        raise ValueError("V30 requires fixed genuine catalog packages")
    result, index = [], 1
    while f"name_{index}" in package:
        kind = package.get(f"type_{index}")
        kind = ("技能" if kind in {"功法", "辅修功法", "神通", "身法", "瞳术"} else
                "装备" if kind in {"法器", "防具"} else kind)
        result.append({"item_id": package.get(f"buff_{index}"), "name": package[f"name_{index}"],
                       "item_type": kind, "quantity": int(package.get(f"amount_{index}", 1)),
                       "quality": int(package.get(f"quality_{index}", 1))})
        index += 1
    if not result or any(row["quantity"] <= 0 or row["item_id"] is None for row in result):
        raise ValueError("V30 package rewards must be positive real item quantities")
    return result


class _RequestRandom:
    """Seed only sect reward draws, without replacing any business result."""
    def __init__(self):
        self.streams = {}

    def choice(self, values):
        from nonebot_plugin_xiuxian_2.xiuxian.on_compat import _CURRENT_EVENT
        event = _CURRENT_EVENT.get()
        number = int(getattr(event, "message_id", 0))
        if not MESSAGE_BASE <= number < MESSAGE_BASE + 300_000:
            raise ValueError("V30 random source used outside its isolated request namespace")
        if number not in self.streams:
            if len(self.streams) >= 96:
                raise ValueError("V30 random stream bound exceeded")
            self.streams[number] = random.Random(f"{SEED_TAG}:{number}")
        return self.streams[number].choice(values)


def _claim_rewards(manifest, number):
    rule, definitions = manifest["reward_batch"]["claim"], manifest["reward_batch"]["definitions"]
    rng = random.Random(f"{SEED_TAG}:{number}")
    counts = Counter(rng.choice(rule["candidates"]) for _ in range(rule["give_num"] - 1))
    result = [[1999, definitions["1999"]["name"], "丹药", 1]]
    result.extend([int(item), definitions[str(item)]["name"], "丹药", amount] for item, amount in counts.items())
    return result


def _claim_reply(rewards):
    return "道友成功领取到丹药:渡厄丹 1 枚!\n" + "".join(
        f"道友成功领取到丹药：{name} {amount} 枚!\n" for _, name, _, amount in rewards[1:])


def _claim_candidates(definitions, threshold):
    result = []
    for item, definition in definitions.items():
        try:
            rank = int(definition["rank"])
        except (KeyError, TypeError, ValueError, OverflowError):
            continue
        if str(item) != "1999" and threshold <= rank <= threshold + 40:
            result.append(str(item))
    return result


def _initial_accessory(actor, ordinal, definition):
    return {"uid": f"seed_v30_{actor}_{ordinal}", "item_id": int(definition["id"]),
            "name": definition["name"], "part": definition["part"], "set_type": definition["set_type"],
            "quality": 1, "affixes": [{"type": "气血", "value": .02}, {"type": "防御", "value": .01}],
            "locked_affixes": [], "wash_count": 0}


def _seed_rows(paths, actors, batch):
    """The bounded setup is also exercised with a small isolated SQLite schema."""
    from nonebot_plugin_xiuxian_2.features.accessory_package.repository import (
        AccessoryPackageGameRepository, AccessoryPackagePlayerRepository)
    from nonebot_plugin_xiuxian_2.features.package_reward.repository import PackageRewardRepository
    from nonebot_plugin_xiuxian_2.features.sect.migrations import apply_sect_elixir
    from nonebot_plugin_xiuxian_2.infrastructure.database.ledger import OperationLedger, OutboxStore

    definitions, candidates = batch["definitions"], batch["claim"]["candidates"]
    with shared._uow(paths.game_db) as uow:
        apply_sect_elixir(uow)
        PackageRewardRepository().ensure_schema(uow)
        AccessoryPackageGameRepository().ensure_schema(uow)
        OperationLedger().ensure_schema(uow)
        OutboxStore().ensure_schema(uow)
        for actor in actors:
            changed = uow.execute("UPDATE user_xiuxian SET level='元神境初期',sect_elixir_get=0 WHERE user_id=?", (actor,))
            if changed.rowcount != 1:
                raise ValueError("V30 actor does not identify exactly one existing synthetic user")
            if uow.query_one("SELECT user_id FROM user_cd WHERE user_id=?", (actor,)) is None:
                uow.execute("INSERT INTO user_cd(user_id) VALUES(?)", (actor,))
            selected = {18050, 18126, *(int(row["item_id"]) for row in batch["ordinary_rewards"])}
            if actor in actors[:24]:
                selected |= {1999, *map(int, candidates)}
            for item in sorted(selected):
                definition = definitions[str(item)]
                count = 8 if item in PACKAGE_IDS.values() else 7
                uow.execute("INSERT INTO back(user_id,goods_id,goods_name,goods_type,goods_num,bind_num,state,"
                            "all_num,create_time,update_time) VALUES(?,?,?,?,?,2,0,?,?,?) "
                            "ON CONFLICT(user_id,goods_id) DO UPDATE SET goods_name=excluded.goods_name,"
                            "goods_type=excluded.goods_type,goods_num=excluded.goods_num,bind_num=2,state=0,"
                            "all_num=excluded.all_num,create_time=excluded.create_time,update_time=excluded.update_time",
                            (actor, item, definition["name"], definition["type"], count, count, shared.ANCHOR, shared.ANCHOR))
        uow.executemany("UPDATE sects SET elixir_room_level=2 WHERE sect_id=?", ((value,) for value in batch["claim"]["sect_ids"]))
    with shared._uow(paths.player_db) as uow:
        AccessoryPackagePlayerRepository().ensure_schema(uow)
        for actor in actors:
            bag = [_initial_accessory(actor, ordinal, batch["accessory_definitions"][ordinal % 4]) for ordinal in range(32)]
            uow.execute("INSERT INTO player_accessory(user_id,equipped,bag) VALUES(?,?,?) "
                        "ON CONFLICT(user_id) DO UPDATE SET equipped=excluded.equipped,bag=excluded.bag",
                        (actor, json.dumps({slot: None for slot in ("手镯", "戒指", "手环", "项链")}), json.dumps(bag)))


def _all_rows(database, table):
    rows = shared._rows(database, f'SELECT * FROM "{table}" ORDER BY rowid LIMIT ?', (sect.MAX_TABLE_ROWS + 1,))
    if len(rows) > sect.MAX_TABLE_ROWS:
        raise ValueError(f"V30 {table} exceeds the bounded full-row snapshot")
    return [sect._json_row(row) for row in rows]


def _frozen(paths):
    result, exclusions = {}, {}
    special = {"game_db": {"user_xiuxian", "user_cd", "back"}, "player_db": {"player_accessory"}}
    for database_name in ("game_db", "player_db", "impart_db"):
        with shared._uow(getattr(paths, database_name), read_only=True) as uow:
            tables = uow.query_all("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
            for row in tables:
                table, where, params = row["name"], "", ()
                if table in special.get(database_name, set()):
                    continue
                columns = {row["name"] for row in uow.query_all(f'PRAGMA table_info("{table}")')}
                field = "operation_id" if "operation_id" in columns else "event_id" if "event_id" in columns else None
                if field:
                    where = f'WHERE "{field}" NOT LIKE ? AND "{field}" NOT LIKE ?'
                    params = ("sect:899%", "package-reward:899%")
                signature = sect._table_signature(uow, table, where=where, parameters=params)
                (exclusions if "excluded" in signature else result)[database_name + ":" + table] = signature
    return {"tables": result, "exclusions": exclusions}


def seed(context, managers, *, participants=PARTICIPANTS):
    from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_sect as runtime
    from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils.item_json import Items
    from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_config import convert_rank, added_ranks
    from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils.shared_read_cache import process_read_cache

    manifest = sect.seed(context, managers, participants=participants)
    actors, catalog, config = manifest["actors"], Items(), runtime.config
    package_defs = {key: dict(catalog.get_data_by_item_id(item)) for key, item in PACKAGE_IDS.items()}
    ordinary, accessory = (_fixed_rewards(package_defs[key]) for key in ("ordinary_package", "accessory_package"))
    if [(row["item_id"], row["quantity"]) for row in ordinary] != [(15103, 1), (15006, 2), (1998, 2)]:
        raise ValueError("V30 genuine18050 package definition changed")
    if [(row["item_id"], row["quantity"], row["quality"], row["item_type"]) for row in accessory] != [
            (item, 1, 1, "饰品") for item in (19001, 19002, 19003, 19004)]:
        raise ValueError("V30 genuine18126 package definition changed")
    room = config["宗门丹房参数"]["elixir_room_level"]["2"]
    threshold = max(convert_rank("元神境初期")[0] - int(room["give_level"]["rank_up"]) - added_ranks(), 16)
    candidates = _claim_candidates(catalog.get_data_by_item_type(("丹药",)), threshold)
    required = {1999, 18050, 18126, *map(int, candidates), *(int(row["item_id"]) for row in ordinary)}
    definitions = {str(item): dict(catalog.get_data_by_item_id(item)) for item in sorted(required)}
    if not candidates or int(room["give_level"]["give_num"]) <= 2:
        raise ValueError("V30 must exercise real multi-item elixir draws")
    actor_sects = sorted({int(manifest["initial_users"]["actors"][actor]["sect_id"]) for actor in actors[:24]})
    if len(actor_sects) != 4:
        raise ValueError("V30 claims must share four real seeded sects")
    accessory_definitions = [{"id": row["item_id"], **catalog.get_data_by_item_id(row["item_id"])} for row in accessory]
    batch = {"definitions": definitions, "package_names": {key: value["name"] for key, value in package_defs.items()},
             "ordinary_rewards": [{key: value for key, value in row.items() if key != "quality"} for row in ordinary],
             "accessory_definitions": accessory_definitions,
             "claim": {"candidates": candidates, "give_num": int(room["give_level"]["give_num"]),
                       "contribution_required": int(config["宗门丹房参数"]["领取贡献度要求"]),
                       "materials_required": int(room["level_up_cost"]["建设度"]), "sect_ids": actor_sects}}
    _seed_rows(context.paths, actors, batch)
    profiles = sect._selected_users(context.paths.game_db, manifest["population_ids"], actors)
    for actor in actors[:24]:
        if int(profiles["actors"][actor]["sect_contribution"]) < batch["claim"]["contribution_required"]:
            raise ValueError("V30 contribution seed does not satisfy real eligibility")
    for row in sect._sects(context.paths.game_db).values():
        if int(row["sect_id"]) in actor_sects and int(row["sect_materials"]) < batch["claim"]["materials_required"]:
            raise ValueError("V30 maintenance seed does not satisfy real eligibility")
    shop = []
    for item, rule in sorted(config["商店商品"].items(), key=lambda row: int(row[0]))[:5]:
        definition = catalog.get_data_by_item_id(item)
        limit = int(rule["weekly_limit"])
        shop.append(["编号：" + str(item), "名称：" + definition["name"],
                     "价格：" + sect._number(rule["cost"]) + "贡献度",
                     "每周限购：" + (f"{limit}/{limit}个" if limit > 0 else "不限购")])
    if len(shop) != 5:
        raise ValueError("V30 needs five genuine first-page shop entries")
    # Seed no shop purchases: both the live repository and legacy fallback must
    # be empty for these isolated synthetic actors before recording the oracle.
    for actor in actors:
        progress = runtime.sect_application.weekly_shop_progress(actor, item_ids=[lines[0][3:] for lines in shop],
            fallback=runtime.get_sect_weekly_purchases_snapshot(actor))
        if any(int(value) for value in progress.values()):
            raise ValueError("V30 selected actors already have weekly shop purchases")
    batch.update(shop=shop, shop_pages=(len(config["商店商品"]) + 4) // 5,
                 initial_back=_all_rows(context.paths.game_db, "back"),
                 initial_accessories=_all_rows(context.paths.player_db, "player_accessory"),
                 initial_cd=shared._rows(context.paths.game_db,
                     f"SELECT * FROM user_cd WHERE user_id IN ({shared._marks(actors)}) ORDER BY user_id", actors),
                 initial_users=profiles, initial_sects=sect._sects(context.paths.game_db),
                 initial_generation=sect._generation(context.paths.game_db), frozen=_frozen(context.paths),
                 random_contract="sect choice uses request-message seeded Random; accessory UID/affixes remain real random")
    manifest.update(fixture_version=SEED_TAG, phase_specs=phase_specs(), reward_batch=batch)
    runtime.runtime_random = _RequestRandom()
    manifest["limits"] = [
        "V30:288requests/version;96shop pure, then24each shop/claim/ordinary package/accessory package per phase",
        "128actors on sole1M fixture,8192 existing members/128sects; claims use24members sharing4sects",
        "genuine18050three-item rewards and18126four-accessory rewards;32existing valid accessories/actor",
        "contention:each package24requests uses6actors x4;24claims use distinct actors, cooldowns unchanged",
        "seeded sect choices preserve real candidates/draw computation; accessory UID/affixes remain random",
        "accessory package has no ordinary rewards; timing covers package-row snapshot/cross-store work, not mixed fixed-item+accessory reward scans",
        "sect100draw searches checked by focused regression only; no new timed search or V29repeat",
        "bounded full back/player-accessory rows; all other bounded tables frozen outside this operation namespace",
        "claim timestamp writes allowed only in selected user_cd.last_check_info_time; exact actor/background user rows frozen otherwise",
        "full original snapshot restored only after phase drain/invariants; no mock rewards/real accounts/network; not all674matchers or sustained50success/s",
    ]
    manifest["feature_table_rows"].update(back=len(batch["initial_back"]), player_accessory=len(batch["initial_accessories"]))
    for name in ("rules", "oracle", "frozen", "initial_users", "initial_sects", "initial_generation", "hot_groups"):
        manifest.pop(name, None)
    manifest.pop("seed_sha256", None)
    manifest["seed_sha256"] = sect._digest(manifest)
    process_read_cache.clear()
    for manager in managers:
        for name in ("_clear_read_cache", "_clear_field_list_cache"):
            clear = getattr(manager, name, None)
            if callable(clear):
                clear()
    return manifest


def _operation(request):
    if request["key"] == "elixir_claim":
        return f"sect:{request['number']}:elixir_claim:{request['actor']}"
    return f"package-reward:{request['number']}:{request['actor']}:{PACKAGE_IDS[request['key']]}"


def request_plan(manifest, mode, count, phase_number=0):
    specs, actors = phase_specs(), manifest["actors"]
    if (phase_number not in range(3) or mode != specs[phase_number]["mode"] or count != 96
            or len(actors) != PARTICIPANTS or len(set(actors)) != PARTICIPANTS):
        raise ValueError("invalid frozen V30 plan")
    result, ordinals = [], Counter()
    for index in range(count):
        key, command = COMMANDS[0] if mode == "pure" else COMMANDS[index % 4]
        ordinal = ordinals[key]
        actor = (actors[ordinal] if mode == "pure" or key == "elixir_claim" else
                 actors[96 + ordinal] if key == "shop_view" else
                 actors[(32 if key == "ordinary_package" else 64) + (ordinal % 6 if mode == "contention" else ordinal)])
        row = {"key": key, "command": command, "input_text": command, "actor": actor,
               "number": MESSAGE_BASE + phase_number * 100_000 + index, "event_type": "group", "phase_label": mode,
               "performance_role": "checked_profile_reuse" if key == "shop_view" else "optimized_reward_target"}
        batch = manifest["reward_batch"]
        if key == "shop_view":
            row.update(expected_shop=batch["shop"], expected_pages=batch["shop_pages"])
        elif key == "elixir_claim":
            row["expected_rewards"] = _claim_rewards(manifest, row["number"])
            row["expected_reply"] = _claim_reply(row["expected_rewards"])
        else:
            row["package_name"] = batch["package_names"][key]
            row["input_text"] += " " + row["package_name"] + " 1"
            row["expected_rewards"] = batch["ordinary_rewards"] if key == "ordinary_package" else batch["accessory_definitions"]
        result.append(row)
        ordinals[key] += 1
    return result


def _text(result):
    text = transport._reply_text(result)
    return re.sub(r"^\[CQ:at,qq=[0-9]+\][ \t]*", "", text).strip()


def classify_result(request, result):
    general = shared.classify_result(request, result)
    if general["outcome"] in {"boundary", "timeout", "exception", "cooldown"}:
        return general
    try:
        text = _text(result)
    except ValueError:
        return {"outcome": "unclassified", "reason": "v30_invalid_envelope"}
    key, lines = request["key"], text.splitlines()
    if key == "shop_view":
        expected = request["expected_shop"]
        valid = text.count(f"【宗门商店】第1/{request['expected_pages']}页") == 1 and all(
            [line for line in lines if line.startswith(prefix)] == [row[index] for row in expected]
            for index, prefix in enumerate(("编号：", "名称：", "价格：", "每周限购：")))
    elif key == "elixir_claim":
        valid = text == request["expected_reply"].strip()
    elif key == "ordinary_package":
        expected = [f"道友打开了 1 个 {request['package_name']}：", *(
            f"获得 {row['name']} x{row['quantity']}" for row in request["expected_rewards"])]
        valid = lines == expected
    elif key == "accessory_package":
        expected = [f"道友打开了 1 个 {request['package_name']}：", *(
            f"获得饰品 {row['name']} x1（一阶）" for row in request["expected_rewards"])]
        valid = lines == expected
    else:
        valid = False
    if valid:
        return {"outcome": "success", "reason": "v30_exact_reward_reply"}
    for marker, reason in (("道友已经领取过了", "already_claimed"), ("背包中没有足够的", "package_insufficient"),
                           ("饰品背包容量不足", "accessory_full"), ("不满足领取条件", "contribution_insufficient")):
        if marker in text:
            return {"outcome": "business_rejection", "reason": reason}
    return {"outcome": "unclassified", "reason": "v30_reply_mismatch"}


def _valid_accessory(value, definition):
    if (not isinstance(value, dict)
            or set(value) != {"uid", "item_id", "name", "part", "set_type", "quality", "affixes", "locked_affixes", "wash_count"}
            or not isinstance(value["uid"], str) or not value["uid"] or value["quality"] != 1
            or value["item_id"] != int(definition["id"]) or value["name"] != definition["name"]
            or value["part"] != definition["part"] or value["set_type"] != definition["set_type"]
            or value["locked_affixes"] != [] or value["wash_count"] != 0
            or not isinstance(value["affixes"], list) or len(value["affixes"]) != 2):
        return False
    kinds = []
    for affix in value["affixes"]:
        if not isinstance(affix, dict) or set(affix) != {"type", "value"} or affix["type"] not in AFFIX_RANGES:
            return False
        kind, number = affix["type"], affix["value"]
        if (type(number) not in (int, float) or not AFFIX_RANGES[kind][0] <= number <= AFFIX_RANGES[kind][1]
                or kind == "速度" and number != int(number)):
            return False
        kinds.append(kind)
    return len(set(kinds)) == 2


def _receipt_rows(paths, database_name, table):
    field = "aggregate_id" if table == "domain_outbox" else "operation_id"
    return [{**row, "operation_id": row[field]} for row in _all_rows(getattr(paths, database_name), table)
            if _namespace(row[field])]


def _expected(manifest, results, receipts):
    """Independent exact allowed changes; no live reads or runtime resolvers."""
    batch, errors = manifest["reward_batch"], []
    back = {(str(row["user_id"]), int(row["goods_id"])): deepcopy(row) for row in batch["initial_back"]}
    users = deepcopy(batch["initial_users"]["actors"])
    accessories = {str(row["user_id"]): deepcopy(row) for row in batch["initial_accessories"]}
    requests, expected_ids, totals = {}, {name: set() for name in (
        "claim", "ordinary", "game_accessory", "player_accessory", "ledger", "audit", "outbox")}, Counter()
    for row in results:
        request = row["request"]
        classified = classify_result(request, row)
        if classified != {"outcome": row.get("outcome"), "reason": row.get("reason")} or classified["outcome"] != "success":
            errors.append("V30 reply/status differs from independently expected success")
        if request["number"] in requests:
            errors.append("V30 repeated request identity")
        requests[request["number"]] = request
        if request["key"] == "shop_view":
            continue
        operation, key, actor = _operation(request), request["key"], request["actor"]
        expected_ids["ledger"].add(operation)
        expected_ids["audit"].add(operation)
        totals[key] += 1
        if key == "elixir_claim":
            expected_ids["claim"].add(operation)
            if users[actor]["sect_elixir_get"] != 0:
                errors.append("V30 repeated daily claim")
            users[actor]["sect_elixir_get"] = 1
            for item, _, _, amount in request["expected_rewards"]:
                target = back[(actor, int(item))]
                target["goods_num"] += amount
                target["bind_num"] += amount
                target["update_time"] = "<claim_timestamp>"
        else:
            package = back[(actor, PACKAGE_IDS[key])]
            package["goods_num"] -= 1
            package["bind_num"] = min(package["bind_num"], package["goods_num"])
            if key == "ordinary_package":
                expected_ids["ordinary"].add(operation)
                for reward in batch["ordinary_rewards"]:
                    target = back[(actor, int(reward["item_id"]))]
                    target["goods_num"] += reward["quantity"]
                    target["bind_num"] += reward["quantity"]
            else:
                expected_ids["game_accessory"].add(operation)
                expected_ids["player_accessory"].add(operation)
                expected_ids["outbox"].add(operation)
    for name, expected in expected_ids.items():
        rows = receipts[name]
        if len(rows) != len(expected) or {row["operation_id"] for row in rows} != expected:
            errors.append(f"V30 exact {name} receipt identities differ")
    indexed = {name: {row["operation_id"]: row for row in rows} for name, rows in receipts.items()}
    for request in requests.values():
        if request["key"] == "shop_view":
            continue
        operation, key, actor = _operation(request), request["key"], request["actor"]
        ledger = indexed["ledger"].get(operation, {})
        encoded = json.loads(ledger.get("result_json") or "{}")
        action = {"elixir_claim": "sect.claim_elixir", "ordinary_package": "package_reward.open", "accessory_package": "accessory_package.open"}[key]
        if ledger.get("status") != "applied" or encoded.get("status") != "applied" or encoded.get("action") != action or encoded.get("operation_id") != operation:
            errors.append("V30 ledger status/action/identity mismatch")
        audit = indexed["audit"].get(operation, {})
        if (audit.get("status") != "applied" or audit.get("action") != action
                or any(json.loads(audit.get(field + "_json") or "{}") != encoded.get(field, {})
                       for field in ("before", "after", "consumed", "granted"))):
            errors.append("V30 successful audit differs from committed ledger")
        if key == "elixir_claim":
            receipt = indexed["claim"].get(operation, {})
            rewards = request["expected_rewards"]
            payload = [actor, int(users[actor]["sect_id"]), batch["claim"]["contribution_required"], batch["claim"]["materials_required"], rewards]
            if (json.loads(receipt.get("rewards") or "[]") != rewards or json.loads(receipt.get("payload") or "[]") != payload
                    or encoded.get("data") != {"status": "applied", "rewards": rewards}):
                errors.append("V30 claim receipt payload/reward mismatch")
        elif key == "ordinary_package":
            receipt = indexed["ordinary"].get(operation, {})
            data = encoded.get("data", {}).get("package_reward", {})
            if (str(receipt.get("user_id")) != actor or receipt.get("package_id") != 18050 or receipt.get("quantity") != 1
                    or json.loads(receipt.get("rewards_json") or "[]") != batch["ordinary_rewards"]
                    or data != {"operation_id": operation, "user_id": actor, "package_id": 18050,
                                "quantity": 1, "rewards": batch["ordinary_rewards"]}):
                errors.append("V30 ordinary package receipt/reward mismatch")
        else:
            game, player = (indexed[name].get(operation, {}) for name in ("game_accessory", "player_accessory"))
            added = json.loads(game.get("accessories_json") or "[]")
            if (game.get("status") != "applied" or player.get("status") != "applied"
                    or str(game.get("user_id")) != actor or str(player.get("user_id")) != actor
                    or game.get("package_id") != 18126 or game.get("quantity") != 1
                    or json.loads(game.get("rewards_json") or "[]") != []
                    or json.loads(player.get("accessories_json") or "[]") != added
                    or encoded.get("data") != {"user_id": actor, "package_id": 18126, "quantity": 1, "accessories": added}
                    or len(added) != 4 or any(not _valid_accessory(value, definition) for value, definition in zip(added, batch["accessory_definitions"]))):
                errors.append("V30 accessory cross-store receipt/reward mismatch")
            outbox = indexed["outbox"].get(operation, {})
            payload = json.loads(outbox.get("payload_json") or "{}")
            if (outbox.get("status") != "sent" or outbox.get("event_id") != operation + ":" + action
                    or outbox.get("aggregate_type") != "accessory_package" or outbox.get("event_type") != action
                    or payload.get("user_id") != actor or payload.get("package_id") != 18126
                    or payload.get("quantity") != 1 or payload.get("rewards") != [] or payload.get("accessories") != added):
                errors.append("V30 accessory outbox missing, pending or mismatched")
            current = json.loads(accessories[actor]["bag"])
            current.extend(added)
            accessories[actor]["bag"] = json.dumps(current)
    if any(row["goods_num"] < 0 or not 0 <= row["bind_num"] <= row["goods_num"] for row in back.values()):
        errors.append("V30 negative/invalid inventory balance")
    for row in accessories.values():
        bag = json.loads(row["bag"])
        if len({value["uid"] for value in bag}) != len(bag) or len(bag) > 1000:
            errors.append("V30 accessory duplicate uid or capacity exceeded")
    return back, users, accessories, totals, errors


def check_invariants(context, managers, manifest, results):
    paths, actors, batch = context.paths, manifest["actors"], manifest["reward_batch"]
    receipts = {key: _receipt_rows(paths, database, table) for key, database, table in (
        ("claim", "game_db", "sect_elixir_claim_operations"), ("ordinary", "game_db", "package_reward_operations"),
        ("game_accessory", "game_db", "accessory_package_operations"),
        ("player_accessory", "player_db", "accessory_package_operations"), ("ledger", "game_db", "operation_ledger"),
        ("audit", "game_db", "operation_audit"), ("outbox", "game_db", "domain_outbox"))}
    back, users, accessories, totals, errors = _expected(manifest, results, receipts)
    actual = {(str(row["user_id"]), int(row["goods_id"])): row for row in _all_rows(paths.game_db, "back")}
    for identity, row in actual.items():
        if identity in back and back[identity].get("update_time") == "<claim_timestamp>":
            try:
                datetime.fromisoformat(str(row["update_time"]))
            except (ValueError, TypeError):
                errors.append("V30 invalid claim inventory timestamp")
            row["update_time"] = "<claim_timestamp>"
    if actual != back:
        errors.append("V30 complete inventory rows differ from exact item/consume/binding deltas")
    current = sect._selected_users(paths.game_db, manifest["population_ids"], actors)
    if current != {"actors": users, "background": batch["initial_users"]["background"]}:
        errors.append("V30 complete selected/background user rows changed unexpectedly")
    if sect._sects(paths.game_db) != batch["initial_sects"]:
        errors.append("V30 eligibility-only sect resources changed")
    actual_accessories = {str(row["user_id"]): row for row in _all_rows(paths.player_db, "player_accessory")}
    for row in [*actual_accessories.values(), *accessories.values()]:
        row["bag"] = json.loads(row["bag"])
        row["equipped"] = json.loads(row["equipped"])
    # Concurrent successful requests may commit in another order; all concrete
    # instances must occur exactly once, while preexisting rows remain exact.
    for actor, expected in accessories.items():
        observed = actual_accessories.get(actor)
        if observed:
            observed["bag"] = sorted(observed["bag"], key=lambda value: value["uid"])
            expected["bag"] = sorted(expected["bag"], key=lambda value: value["uid"])
    if actual_accessories != accessories:
        errors.append("V30 full player accessory rows do not match committed instances")
    cd = shared._rows(paths.game_db, f"SELECT * FROM user_cd WHERE user_id IN ({shared._marks(actors)}) ORDER BY user_id", actors)
    initial_cd = deepcopy(batch["initial_cd"])
    claiming = {row["request"]["actor"] for row in results if row["request"]["key"] == "elixir_claim"}
    for before, after in zip(initial_cd, cd):
        if str(after["user_id"]) in claiming:
            try:
                datetime.fromisoformat(str(after["last_check_info_time"]))
            except (TypeError, ValueError):
                errors.append("V30 invalid claim activity timestamp")
            after["last_check_info_time"] = before["last_check_info_time"]
    if cd != initial_cd:
        errors.append("V30 cooldown state changed outside declared activity timestamps")
    if _frozen(paths) != batch["frozen"]:
        errors.append("V30 bounded unrelated tables or preexisting operation rows changed")
    if sect._generation(paths.game_db) != batch["initial_generation"]:
        errors.append("V30 display/item-only rewards unexpectedly invalidated exact exp/stone ranks")
    counts = Counter(row["request"]["key"] for row in results)
    if results:
        pure = results[0]["request"]["phase_label"] == "pure"
        expected = Counter(shop_view=96) if pure else Counter({key: 24 for key, _ in COMMANDS})
        if counts != expected or len(results) != 96:
            errors.append("V30 frozen96request command counts differ")
    if int(shared._rows(paths.game_db, "SELECT COUNT(*) AS n FROM user_xiuxian")[0]["n"]) != EXPECTED_USERS:
        errors.append("V30 changed sole1M user count")
    return {"ok": not errors, "errors": errors, "per_command": dict(counts), "fresh_writes": sum(totals.values()),
            "fresh_elixir_claims": totals["elixir_claim"], "fresh_ordinary_packages": totals["ordinary_package"],
            "fresh_accessory_packages": totals["accessory_package"], "actual_distinct_claim_items": sum(
                len(row["request"].get("expected_rewards", ())) for row in results if row["request"]["key"] == "elixir_claim"),
            "invariants": "exact independent reward/receipt/ledger identities, complete inventory/bindings and selected/background users, cross-store accessory instances, unchanged sect eligibility resources/generations, bounded unrelated tables"}
