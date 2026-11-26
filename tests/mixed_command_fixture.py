"""Deterministic, bounded participants and checks for mixed real commands.

Setup is excluded from measured traffic. The caller restores the one original
synthetic snapshot before each seed and drains all requests before checking.
"""
from __future__ import annotations

from collections import Counter
from copy import deepcopy
import hashlib
import json


ANCHOR = "2026-01-01 00:00:00"
WALLET = 500_000_000
DEPOSIT = 1_000
PRICE = 600_000
PREFIX = "codex-mixed-v1:"
STAT_KEYS = (
    "鬼市存入灵石", "仙肆购买次数", "仙肆购买数量", "仙肆消费灵石",
    "仙肆售出次数", "仙肆售出数量", "仙肆收入灵石",
)
READ_SPECS = (
    ("profile_info", "我的修仙信息", ("道号", "修为排行", "灵石排行")),
    ("asset_overview", "我的资产总览", ("我的资产总览", "灵石")),
    ("mind_state", "我的状态", ("我的状态", "气血")),
    ("cultivation_value", "我的修为", ("我的修为", "境界")),
    ("backpack", "我的背包", ("背包",)),
    ("pill_backpack", "丹药背包", ("丹药",)),
    ("herb_backpack", "药材背包", ("药材",)),
    ("equipment", "我的装备", ("装备",)),
    ("realm_rank", "排行榜", ("位面境界排行榜",)),
    ("stone_rank", "灵石排行榜", ("位面灵石排行榜",)),
    ("activity_rank", "活动排行", ("排行榜",)),
)
COOLDOWN_MARKERS = (
    "慢...慢一..点", "冷静一下，还有", "让我歇口气，还有", "耐心一点哦，还有",
    "别急嘛~还有", "稍等一下下啦，还有", "时间还没到，还有",
)


def _uow(database, *, read_only=False):
    from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork
    return DatabaseUnitOfWork(database, read_only=read_only)


def _rows(database, sql, params=()):
    with _uow(database, read_only=True) as uow:
        return uow.query_all(sql, params)


def _marks(values):
    return ",".join("?" for _ in values)


def _statistics(database, actors):
    rows = _rows(database, "SELECT name FROM sqlite_master WHERE type='table' AND name='statistics'")
    result = {actor: {key: 0 for key in STAT_KEYS} for actor in actors}
    if not rows:
        return result
    columns = {str(row["name"]) for row in _rows(database, 'PRAGMA table_info("statistics")')}
    keys = [key for key in STAT_KEYS if key in columns]
    if not keys:
        return result
    fields = ",".join('"' + key + '"' for key in keys)
    for row in _rows(database, f"SELECT user_id,{fields} FROM statistics WHERE user_id IN ({_marks(actors)})", actors):
        result[str(row["user_id"])].update({key: int(row.get(key) or 0) for key in keys})
    return result


def _inventory(database, actors):
    rows = _rows(database, "SELECT user_id,goods_id,goods_num,bind_num,state FROM back "
                 f"WHERE user_id IN ({_marks(actors)}) ORDER BY user_id,goods_id", actors)
    return [{**row, "user_id": str(row["user_id"]), "goods_id": int(row["goods_id"]),
             "goods_num": int(row["goods_num"] or 0), "bind_num": int(row["bind_num"] or 0),
             "state": int(row["state"] or 0)} for row in rows]


def _activity_seed_config(original):
    config = deepcopy(original)
    config.update(enabled=True, start_time="0", end_time="无限")
    for activity in config.get("gameplay_activities", ()):
        activity.update(enabled=True, start_time="0", end_time="无限")
    for stage in config.get("extensions", {}).get("stages", ()):
        stage.update(start_time="0", end_time="无限")
    return config


def seed(context, managers, *, participants=32):
    """Populate only 32 existing users; never insert or duplicate user rows."""
    if participants != 32:
        raise ValueError("the frozen mixed fixture requires exactly 32 participants")
    from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils.item_json import Items
    from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_activity import activity_config

    paths = context.paths
    game = managers[0]
    # The retained fixture predates the 1M expansion and has additional seed
    # users. Select by indexed row identity rather than inventing their IDs.
    participant_rows = [1, 2, 3] + [10_000 + index * 990_000 // 28 for index in range(29)]
    selected_users = _rows(
        paths.game_db,
        f"SELECT id,user_id FROM user_xiuxian WHERE id IN ({_marks(participant_rows)}) ORDER BY id",
        participant_rows,
    )
    actors = [str(row["user_id"]) for row in selected_users]
    if len(actors) != participants or len(set(actors)) != participants:
        raise ValueError("frozen indexed participant rows are not all present and unique")
    count = int(_rows(paths.game_db, "SELECT COUNT(*) AS n FROM user_xiuxian")[0]["n"])
    if count != 1_000_000:
        raise ValueError(f"mixed fixture requires the existing 1M users, got {count}")
    found = _rows(paths.game_db, f"SELECT user_id FROM user_xiuxian WHERE user_id IN ({_marks(actors)})", actors)
    if {str(row["user_id"]) for row in found} != set(actors):
        raise ValueError("frozen participant IDs are not all present in the synthetic fixture")
    catalog = Items()
    selected = []
    for category, amount in (("法器", 4), ("防具", 4), ("功法", 4), ("丹药", 4), ("药材", 4)):
        ids = sorted(catalog.get_data_by_item_type((category,)), key=int)[:amount]
        if len(ids) != amount:
            raise ValueError(f"item catalog lacks {amount} entries for {category}")
        for item_id in ids:
            definition = catalog.get_data_by_item_id(int(item_id))
            selected.append((int(item_id), definition))
    herbs = [(item_id, definition) for item_id, definition in selected if definition["type"] == "药材"]
    trade_item_id, trade_item = herbs[0]
    seller = actors[-1]
    listings = [
        {"id": PREFIX + f"offer:{index:02d}", "quantity": 256, "seller": seller}
        for index in range(len(actors) - 1)
    ] + [{"id": PREFIX + "contended", "quantity": 8, "seller": seller}]
    config_path = activity_config.CONFIG_PATH.resolve()
    data_root = paths.game_db.parent.resolve()
    if not config_path.is_relative_to(data_root):
        raise ValueError("activity projection escapes the isolated game data directory")
    if activity_config.CONFIG_DB_PATH.exists():
        raise ValueError("existing activity event database is outside the frozen projection-only seed")
    original_activity = activity_config._load_config_projection_readonly()
    original_activity_file_sha256 = (
        hashlib.sha256(config_path.read_bytes()).hexdigest() if config_path.is_file() else None
    )
    activity = _activity_seed_config(original_activity)
    # The outer original snapshot owns this projection and restores its bytes.
    # Only availability windows change; item/reward/cooldown rules stay intact.
    activity_config._save_config_projection(activity)
    activity_key = activity_config._activity_config_key(activity)
    gameplay_keys = [str(row.get("key")) for row in activity.get("gameplay_activities", ()) if row.get("key")]
    activity_keys = list(dict.fromkeys([activity_key, *gameplay_keys]))
    with _uow(paths.game_db) as uow:
        for index, actor in enumerate(actors):
            uow.execute("UPDATE user_xiuxian SET stone=?,hp=1000,mp=1000,atk=100,user_stamina=100 WHERE user_id=?", (WALLET, actor))
            uow.execute("DELETE FROM back WHERE user_id=?", (actor,))
            weapon = armor = 0
            for item_id, definition in selected:
                equipped = 0
                if definition.get("item_type") == "法器" and not weapon:
                    weapon, equipped = item_id, 1
                elif definition.get("item_type") == "防具" and not armor:
                    armor, equipped = item_id, 1
                uow.execute(
                    "INSERT INTO back(user_id,goods_id,goods_name,goods_type,goods_num,bind_num,state,create_time,update_time,all_num) VALUES(?,?,?,?,20,0,?,?,?,20)",
                    (actor, item_id, definition["name"], definition["type"], equipped, ANCHOR, ANCHOR),
                )
            existing = uow.query_one("SELECT user_id FROM BuffInfo WHERE user_id=?", (actor,))
            if existing is None:
                uow.execute("INSERT INTO BuffInfo(user_id,faqi_buff,armor_buff) VALUES(?,?,?)", (actor, weapon, armor))
            else:
                uow.execute("UPDATE BuffInfo SET faqi_buff=?,armor_buff=? WHERE user_id=?", (weapon, armor, actor))
            uow.execute(
                "INSERT INTO activity_user(user_id,sign_days,total_sign_days,last_sign_date,create_time,update_time) VALUES(?,?,?,?,?,?) "
                "ON CONFLICT(user_id) DO UPDATE SET sign_days=excluded.sign_days,total_sign_days=excluded.total_sign_days,last_sign_date=excluded.last_sign_date,update_time=excluded.update_time",
                (actor, index % 7 + 1, index + 10, "2026-01-01", ANCHOR, ANCHOR),
            )
            for key in activity_keys:
                for table, fields, values in (
                    ("activity_point_balance", "points,total_points", (index * 13 + 100, index * 17 + 200)),
                    ("activity_pass_balance", "exp,total_exp,level", (index * 19 + 100, index * 23 + 200, index % 10)),
                    ("activity_boss_damage", "total_damage", (index * 1234 + 100,)),
                ):
                    names = fields.split(",")
                    updates = ",".join(name + "=excluded." + name for name in names)
                    uow.execute(
                        f"INSERT INTO {table}(activity_key,user_id,{fields},update_time) VALUES(?,?,{_marks(values)},?) "
                        f"ON CONFLICT(activity_key,user_id) DO UPDATE SET {updates},update_time=excluded.update_time",
                        (key, actor, *values, ANCHOR),
                    )
        for listing in listings:
            uow.execute(
                "INSERT INTO xianshi_item(id,user_id,goods_id,name,type,price,quantity) VALUES(?,?,?,?,?,?,?) "
                "ON CONFLICT(id) DO UPDATE SET user_id=excluded.user_id,goods_id=excluded.goods_id,name=excluded.name,type=excluded.type,price=excluded.price,quantity=excluded.quantity",
                (listing["id"], seller, trade_item_id, trade_item["name"], trade_item["type"], PRICE, listing["quantity"]),
            )
        if uow.query_one("SELECT operation_id FROM xianshi_operations WHERE operation_id LIKE 'xianshi-buy:88%' LIMIT 1"):
            raise ValueError("reserved mixed purchase receipt namespace is not empty")
        if uow.query_one("SELECT operation_id FROM trade_guishi_deposit_operations WHERE operation_id LIKE 'guishi-stone:deposit:88%' LIMIT 1"):
            raise ValueError("reserved mixed deposit receipt namespace is not empty")
    with _uow(paths.trade_db) as uow:
        for actor in actors:
            uow.execute("INSERT INTO guishi_info(user_id,stored_stone,items) VALUES(?,0,'{}') "
                        "ON CONFLICT(user_id) DO UPDATE SET stored_stone=0,items='{}'", (actor,))
    game._clear_read_cache()
    from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils.shared_read_cache import process_read_cache
    process_read_cache.clear()
    manifest = {
        "fixture_version": "mixed-real-commands-v1", "anchor": ANCHOR,
        "users": count, "actors": actors, "buyers": actors[:-1], "seller": seller,
        "wallet": WALLET, "deposit_amount": DEPOSIT, "price": PRICE,
        "listings": listings, "trade_item_id": trade_item_id, "trade_item_name": trade_item["name"],
        "inventory": _inventory(paths.game_db, actors), "statistics": _statistics(paths.player_db, actors),
        "activity_key": activity_key, "activity_keys": activity_keys,
        "activity_original_config_sha256": hashlib.sha256(json.dumps(original_activity, sort_keys=True, ensure_ascii=True).encode()).hexdigest(),
        "activity_original_projection_sha256": original_activity_file_sha256,
        "activity_config_sha256": hashlib.sha256(json.dumps(activity, sort_keys=True, ensure_ascii=True).encode()).hexdigest(),
        "activity_projection_sha256": hashlib.sha256(config_path.read_bytes()).hexdigest(),
        "seed_counts": {"participants": len(actors), "back_rows": 32 * len(selected), "BuffInfo_participants": 32,
                        "activity_user_participants": 32, "activity_point_balance_rows": 32 * len(activity_keys),
                        "activity_pass_balance_rows": 32 * len(activity_keys), "activity_boss_damage_rows": 32 * len(activity_keys),
                        "xianshi_listings": len(listings), "xianshi_stock": sum(row["quantity"] for row in listings)},
        "limits": ["32 populated participants; no populated million-row feature-table claim", "no exp-changing gameplay in this workload", "cooldowns and business limits remain enabled"],
    }
    manifest["feature_table_rows"] = {
        table: int(_rows(paths.game_db, f'SELECT COUNT(*) AS n FROM "{table}"')[0]["n"])
        for table in ("back", "BuffInfo", "activity_user", "activity_point_balance",
                      "activity_pass_balance", "activity_boss_damage", "xianshi_item")
    }
    manifest["seed_sha256"] = hashlib.sha256(json.dumps(manifest, sort_keys=True, ensure_ascii=True).encode()).hexdigest()
    return manifest


def request_plan(manifest, mode, count, phase_number=0):
    if mode not in {"pure", "readmostly", "mixed", "contention"} or not 1 <= count <= 4096 or not 0 <= phase_number < 90:
        raise ValueError("invalid bounded mixed request plan")
    actors, buyers = manifest["actors"], manifest["buyers"]
    result, reads, writes = [], 0, 0
    for index in range(count):
        write = mode == "contention" and index % 4 != 0 or mode == "readmostly" and index % 10 == 9 or mode == "mixed" and index % 2 == 1
        number = 880_000_000 + phase_number * 100_000 + index
        actor = actors[reads % len(actors)]
        if write:
            actor = buyers[(writes // 2) % len(buyers)] if mode != "contention" else buyers[0]
            if writes % 2 == 0:
                request = {"key": "guishi_deposit", "command": "鬼市存灵石", "input_text": f"鬼市存灵石 {DEPOSIT}", "amount": DEPOSIT}
            else:
                listing = manifest["listings"][-1] if mode == "contention" else manifest["listings"][(writes // 2) % len(buyers)]
                request = {"key": "xianshi_purchase", "command": "仙肆购买", "input_text": f"仙肆购买 {listing['id']} 1", "listing_id": listing["id"], "quantity": 1}
                if mode == "contention":
                    actor = buyers[(writes // 2) % min(4, len(buyers))]
            writes += 1
        else:
            key, command, markers = READ_SPECS[reads % len(READ_SPECS)]
            if mode == "contention":
                key, command, markers = READ_SPECS[(reads % 2)]
                actor = buyers[0]
            request = {"key": key, "command": command, "input_text": command, "success_markers": list(markers)}
            reads += 1
        result.append({**request, "actor": actor, "number": number, "event_type": "group"})
    return result


def classify_result(request, result):
    status = str(result.get("status") or "")
    text = "\n".join(str(reply) for reply in result.get("replies", ()))
    if result.get("blocked"):
        return {"outcome": "boundary", "reason": "offline_boundary"}
    if status == "timeout" or result.get("timed_out"):
        return {"outcome": "timeout", "reason": "request_timeout"}
    if result.get("errors") or status == "exception":
        return {"outcome": "exception", "reason": "handler_exception"}
    if any(marker in text for marker in COOLDOWN_MARKERS):
        return {"outcome": "cooldown", "reason": "command_cooldown"}
    if not text:
        return {"outcome": "unclassified", "reason": "no_outgoing_reply"}
    key = request["key"]
    if key == "guishi_deposit":
        if "成功存入 " in text and " 灵石到鬼市账户" in text:
            return {"outcome": "success", "reason": "deposit_completed_or_replayed"}
        reasons = (("灵石不足，存入失败", "stone_insufficient"), ("鬼市账户最多可存", "stored_cap_exceeded"), ("单次存入不可超过", "amount_capped"))
    elif key == "xianshi_purchase":
        if "成功购买 " in text and "花费 " in text or "该购买请求已经处理，无需重复提交" in text:
            return {"outcome": "success", "reason": "purchase_completed_or_replayed"}
        reasons = (("未找到仙肆ID为", "listing_missing"), ("已被其他道友购买", "listing_missing"), ("仙肆中没有足够", "stock_insufficient"), ("灵石不足", "stone_insufficient"), ("已达到数量上限", "inventory_full"), ("不能购买自己上架", "self_purchase"))
    else:
        markers = request.get("success_markers", ())
        if key in {spec[0] for spec in READ_SPECS} and markers and all(marker in text for marker in markers):
            return {"outcome": "success", "reason": "expected_read_response"}
        reasons = ()
    for marker, reason in reasons:
        if marker in text:
            return {"outcome": "business_rejection", "reason": reason}
    return {"outcome": "unclassified", "reason": "unexpected_response"}


def check_invariants(context, managers, manifest, results):
    """Compare final balances, inventory, receipts, ledger and stats exactly."""
    paths, actors = context.paths, manifest["actors"]
    errors, outcomes = [], Counter()
    requests = []
    for result in results:
        request = result.get("request", result)
        classification = result.get("outcome")
        if isinstance(classification, dict):
            classification = classification.get("outcome")
        if not classification:
            classification = classify_result(request, result)["outcome"]
        outcomes[classification] += 1
        if classification in {"unclassified", "exception", "timeout", "boundary"}:
            errors.append(f"request {request.get('number')} has outcome {classification}")
        requests.append((request, classification))
    expected_wallet = {actor: manifest["wallet"] for actor in actors}
    expected_stored = {actor: 0 for actor in actors}
    expected_stats = {actor: dict(values) for actor, values in manifest["statistics"].items()}
    expected_inventory = {(row["user_id"], row["goods_id"]): dict(row) for row in manifest["inventory"]}
    expected_listings = {row["id"]: row["quantity"] for row in manifest["listings"]}
    deposits = _rows(paths.game_db, "SELECT operation_id,user_id,amount,stored_balance FROM trade_guishi_deposit_operations WHERE operation_id LIKE 'guishi-stone:deposit:88%'")
    purchases = _rows(paths.game_db, "SELECT operation_id,listing_id,buyer_id,seller_id,goods_id,quantity,total_cost FROM xianshi_operations WHERE operation_id LIKE 'xianshi-buy:88%'")
    receipt_ids = {str(row["operation_id"]) for row in deposits + purchases}
    success_ids, planned_ids, planned_writes = set(), set(), {}
    for request, outcome in requests:
        if request.get("key") not in {"guishi_deposit", "xianshi_purchase"}:
            continue
        operation = (f"guishi-stone:deposit:{request['number']}:{request['actor']}" if request["key"] == "guishi_deposit" else f"xianshi-buy:{request['number']}:{request['listing_id']}")
        planned_ids.add(operation)
        planned_writes[operation] = request
        if outcome == "success":
            success_ids.add(operation)
        elif operation in receipt_ids:
            errors.append(f"rejected request has committed receipt {operation}")
    if receipt_ids != success_ids:
        errors.append(f"receipt/success mismatch missing={sorted(success_ids-receipt_ids)} extra={sorted(receipt_ids-success_ids)}")
    for row in deposits:
        actor, amount = str(row["user_id"]), int(row["amount"])
        planned = planned_writes.get(str(row["operation_id"]), {})
        if actor not in expected_wallet or amount != DEPOSIT or actor != str(planned.get("actor")):
            errors.append(f"unexpected deposit receipt {row['operation_id']}")
            continue
        expected_wallet[actor] -= amount
        expected_stored[actor] += amount
        expected_stats[actor]["鬼市存入灵石"] += amount
    for row in purchases:
        buyer, seller, quantity, cost = str(row["buyer_id"]), str(row["seller_id"]), int(row["quantity"]), int(row["total_cost"])
        planned = planned_writes.get(str(row["operation_id"]), {})
        if buyer not in expected_wallet or buyer != str(planned.get("actor")) or row["listing_id"] != planned.get("listing_id") or seller != manifest["seller"] or row["listing_id"] not in expected_listings or int(row["goods_id"]) != manifest["trade_item_id"] or quantity != 1 or cost != quantity * PRICE:
            errors.append(f"unexpected purchase receipt {row['operation_id']}")
            continue
        expected_wallet[buyer] -= cost
        expected_wallet[seller] += cost
        expected_listings[row["listing_id"]] -= quantity
        item = expected_inventory[(buyer, manifest["trade_item_id"])]
        item["goods_num"] += quantity
        item["bind_num"] += quantity
        for actor, values in ((buyer, {"仙肆购买次数": 1, "仙肆购买数量": quantity, "仙肆消费灵石": cost}), (seller, {"仙肆售出次数": 1, "仙肆售出数量": quantity, "仙肆收入灵石": cost})):
            for key, amount in values.items():
                expected_stats[actor][key] += amount
        ledger = _rows(paths.game_db, "SELECT user_id,action,stone_delta,item_delta FROM economy_log WHERE trace_id=? AND source='trade'", (row["operation_id"],))
        expected_actions = Counter({("xianshi_buy_cost", buyer): 1, ("xianshi_buy_item", buyer): 1, ("xianshi_seller_income", seller): 1})
        if Counter((str(entry["action"]), str(entry["user_id"])) for entry in ledger) != expected_actions:
            errors.append(f"purchase ledger count/actions differ for {row['operation_id']}")
        for entry in ledger:
            expected_delta = -cost if entry["action"] == "xianshi_buy_cost" else cost if entry["action"] == "xianshi_seller_income" else 0
            if int(entry["stone_delta"] or 0) != expected_delta:
                errors.append(f"purchase ledger stone differs for {row['operation_id']}")
            if entry["action"] == "xianshi_buy_item":
                items = json.loads(entry["item_delta"] or "[]")
                if len(items) != 1 or int(items[0].get("id", -1)) != manifest["trade_item_id"] or int(items[0].get("amount", -1)) != quantity:
                    errors.append(f"purchase ledger item differs for {row['operation_id']}")
    wallets = {str(row["user_id"]): int(row["stone"]) for row in _rows(paths.game_db, f"SELECT user_id,stone FROM user_xiuxian WHERE user_id IN ({_marks(actors)})", actors)}
    stored = {str(row["user_id"]): int(row["stored_stone"]) for row in _rows(paths.trade_db, f"SELECT user_id,stored_stone FROM guishi_info WHERE user_id IN ({_marks(actors)})", actors)}
    if wallets != expected_wallet or any(value < 0 for value in wallets.values()):
        errors.append("wallet balances differ from unique committed receipts or are negative")
    if stored != expected_stored or any(value < 0 for value in stored.values()):
        errors.append("Guishi balances differ from committed deposits or are negative")
    if sum(wallets.values()) + sum(stored.values()) != len(actors) * manifest["wallet"]:
        errors.append("wallet plus Guishi total is not conserved")
    actual_inventory = {(row["user_id"], row["goods_id"]): row for row in _inventory(paths.game_db, actors)}
    if actual_inventory != expected_inventory:
        errors.append("inventory quantities/bind flags/equipment states differ from purchase receipts")
    listings = {str(row["id"]): int(row["quantity"]) for row in _rows(paths.game_db, "SELECT id,quantity FROM xianshi_item WHERE id LIKE ?", (PREFIX + "%",))}
    if listings != {key: value for key, value in expected_listings.items() if value > 0} or any(value < 0 for value in expected_listings.values()):
        errors.append("listing stocks differ from unique purchase receipts or were oversold")
    if _statistics(paths.player_db, actors) != expected_stats:
        errors.append("trade statistics differ from unique committed receipts")
    users = int(_rows(paths.game_db, "SELECT COUNT(*) AS n FROM user_xiuxian")[0]["n"])
    if users != manifest["users"]:
        errors.append("main user row count changed")
    for database in (paths.game_db, paths.trade_db, paths.player_db):
        rows = _rows(database, "PRAGMA quick_check")
        if rows != [{"quick_check": "ok"}]:
            errors.append(f"quick_check failed for {database.name}")
    return {"ok": not errors, "errors": errors, "outcomes": dict(outcomes),
            "deposit_receipts": len(deposits), "purchase_receipts": len(purchases),
            "planned_write_operations": len(planned_ids), "successful_write_operations": len(success_ids),
            "wallet_plus_stored": sum(wallets.values()) + sum(stored.values()),
            "checks": ["wallet", "Guishi", "inventory", "listing_stock", "receipts", "purchase_economy_ledger", "trade_statistics", "main_user_count", "quick_check"]}
