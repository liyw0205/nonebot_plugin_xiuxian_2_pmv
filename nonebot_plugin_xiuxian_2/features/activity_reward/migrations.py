import hashlib
import json

from ...infrastructure.database import DatabaseUnitOfWork


def apply_activity_reward(uow: DatabaseUnitOfWork) -> None:
    uow.execute("CREATE TABLE IF NOT EXISTS activity_reward_feature_migrations (version TEXT PRIMARY KEY)")
    uow.execute("INSERT OR IGNORE INTO activity_reward_feature_migrations(version) VALUES ('activity_reward.001')")


def apply_activity_claim_all(uow: DatabaseUnitOfWork) -> None:
    uow.execute(
        "CREATE TABLE IF NOT EXISTS activity_claim_all_operations("
        "operation_id TEXT PRIMARY KEY,user_id TEXT NOT NULL,status TEXT NOT NULL,"
        "result_json TEXT NOT NULL DEFAULT '',created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,"
        "updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
    )
    uow.execute(
        "CREATE TABLE IF NOT EXISTS activity_claim_all_steps("
        "operation_id TEXT NOT NULL,step_name TEXT NOT NULL,ordinal INTEGER NOT NULL,"
        "status TEXT NOT NULL DEFAULT 'pending',attempts INTEGER NOT NULL DEFAULT 0,"
        "ok INTEGER,result_text TEXT NOT NULL DEFAULT '',error_text TEXT NOT NULL DEFAULT '',"
        "updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,PRIMARY KEY(operation_id,step_name))"
    )


def apply_activity_claim_all_legacy_receipts(uow: DatabaseUnitOfWork) -> None:
    legacy_database = uow.database.parent / "activity" / "activity.db"
    if not legacy_database.is_file():
        return

    required = {
        "activity_claim_all_operations": {"operation_id", "user_id", "status", "result_json", "created_at", "updated_at"},
        "activity_claim_all_steps": {"operation_id", "step_name", "ordinal", "status", "attempts", "ok", "result_text", "error_text", "updated_at"},
    }
    with DatabaseUnitOfWork(legacy_database, read_only=True) as legacy:
        found = {
            table: {str(row["name"]) for row in legacy.query_all(f"PRAGMA table_info({table})")}
            for table in required
        }
        if not any(found.values()):
            return
        for table, columns in required.items():
            if not columns.issubset(found[table]):
                raise RuntimeError(f"legacy activity claim-all schema incomplete: {table}")

        last_rowid = 0
        while True:
            operations = legacy.query_all(
                "SELECT rowid AS legacy_rowid,operation_id,user_id,status,result_json,created_at,updated_at "
                "FROM activity_claim_all_operations WHERE rowid>? ORDER BY rowid LIMIT 200",
                (last_rowid,),
            )
            if not operations:
                break
            for row in operations:
                operation_id = str(row["operation_id"])
                steps = legacy.query_all(
                    "SELECT operation_id,step_name,ordinal,status,attempts,ok,result_text,error_text,updated_at "
                    "FROM activity_claim_all_steps WHERE operation_id=? ORDER BY ordinal",
                    (operation_id,),
                )
                if tuple(step["step_name"] for step in steps) != ("tasks", "pass", "boss_milestone", "boss_rank"):
                    raise RuntimeError(f"legacy activity claim-all plan incomplete: {operation_id}")
                existing = uow.query_one(
                    "SELECT operation_id,user_id,status,result_json,created_at,updated_at "
                    "FROM activity_claim_all_operations WHERE operation_id=?",
                    (operation_id,),
                )
                current = {key: row[key] for key in ("operation_id", "user_id", "status", "result_json", "created_at", "updated_at")}
                if existing is not None:
                    current_steps = uow.query_all(
                        "SELECT operation_id,step_name,ordinal,status,attempts,ok,result_text,error_text,updated_at "
                        "FROM activity_claim_all_steps WHERE operation_id=? ORDER BY ordinal",
                        (operation_id,),
                    )
                    if existing != current or current_steps != steps:
                        raise RuntimeError(f"activity claim-all receipt conflict: {operation_id}")
                    continue
                uow.execute(
                    "INSERT INTO activity_claim_all_operations(operation_id,user_id,status,result_json,created_at,updated_at) "
                    "VALUES(?,?,?,?,?,?)",
                    tuple(current.values()),
                )
                uow.executemany(
                    "INSERT INTO activity_claim_all_steps(operation_id,step_name,ordinal,status,attempts,ok,result_text,error_text,updated_at) "
                    "VALUES(?,?,?,?,?,?,?,?,?)",
                    (tuple(step.values()) for step in steps),
                )
            last_rowid = int(operations[-1]["legacy_rowid"])


def apply_activity_task_claim(uow: DatabaseUnitOfWork) -> None:
    uow.execute(
        "CREATE TABLE IF NOT EXISTS activity_task_reward_claim_operations("
        "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,request_json TEXT NOT NULL,"
        "result_json TEXT NOT NULL DEFAULT '[]',result_status TEXT NOT NULL DEFAULT 'started',"
        "status TEXT NOT NULL DEFAULT 'started',created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,"
        "updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP)"
    )
    uow.execute(
        "CREATE TABLE IF NOT EXISTS activity_task_reward_claim_reservations("
        "activity_key TEXT NOT NULL,user_id TEXT NOT NULL,scope_type TEXT NOT NULL,scope_key TEXT NOT NULL,"
        "task_key TEXT NOT NULL,operation_id TEXT NOT NULL,created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,"
        "PRIMARY KEY(activity_key,user_id,scope_type,scope_key,task_key),"
        "UNIQUE(operation_id,scope_type,scope_key,task_key))"
    )
    uow.execute(
        "CREATE INDEX IF NOT EXISTS idx_activity_task_claim_reservations_operation "
        "ON activity_task_reward_claim_reservations(operation_id)"
    )


def apply_activity_task_claim_legacy_receipts(uow: DatabaseUnitOfWork) -> None:
    legacy_database = uow.database.parent / "activity" / "activity.db"
    if not legacy_database.is_file():
        return

    with DatabaseUnitOfWork(legacy_database, read_only=True) as legacy:
        columns = {str(row["name"]) for row in legacy.query_all(
            "PRAGMA table_info(activity_task_claim_operations)"
        )}
        required = {"operation_id", "payload", "result_json", "created_at"}
        if not columns:
            return
        if not required.issubset(columns):
            raise RuntimeError("legacy activity task claim schema incomplete")

        last_rowid = 0
        while True:
            rows = legacy.query_all(
                "SELECT rowid AS legacy_rowid,operation_id,payload,result_json,created_at "
                "FROM activity_task_claim_operations WHERE rowid>? ORDER BY rowid LIMIT 200",
                (last_rowid,),
            )
            if not rows:
                break
            for row in rows:
                operation_id = str(row["operation_id"])
                try:
                    payload = json.loads(str(row["payload"]))
                    result = json.loads(str(row["result_json"]))
                except (TypeError, ValueError, json.JSONDecodeError) as exc:
                    raise RuntimeError(f"invalid legacy activity task claim receipt: {operation_id}") from exc
                if (
                    not isinstance(payload, list) or len(payload) != 6
                    or not isinstance(payload[2], list) or not isinstance(result, list)
                ):
                    raise RuntimeError(f"invalid legacy activity task claim payload: {operation_id}")
                user_id, activity_key, tasks = str(payload[0]), str(payload[1]), payload[2]
                for task in tasks:
                    if not isinstance(task, list) or len(task) != 6:
                        raise RuntimeError(f"invalid legacy activity task claim task: {operation_id}")
                operation = uow.query_one(
                    "SELECT operation_id,payload,result_json,result_status,status,created_at "
                    "FROM activity_task_reward_claim_operations WHERE operation_id=?",
                    (operation_id,),
                )
                if operation is not None:
                    if (
                        str(operation["payload"]) != str(row["payload"])
                        or str(operation["result_json"]) != str(row["result_json"])
                        or str(operation["result_status"]) != "applied"
                        or str(operation["status"]) != "applied"
                    ):
                        raise RuntimeError(f"activity task claim receipt conflict: {operation_id}")
                else:
                    request = {
                        "user_id": user_id,
                        "activity_key": activity_key,
                        "legacy_import": True,
                        "max_goods_num": int(payload[5]),
                        "tasks": [],
                    }
                    uow.execute(
                        "INSERT INTO activity_task_reward_claim_operations"
                        "(operation_id,payload,request_json,result_json,result_status,status,created_at) "
                        "VALUES(?,?,?,?, 'applied','applied',?)",
                        (operation_id, row["payload"], json.dumps(request, ensure_ascii=True, separators=(",", ":")),
                         row["result_json"], row["created_at"]),
                    )
                for task in tasks:
                    identity = (activity_key, user_id, str(task[1]), str(task[2]), str(task[0]))
                    reservation = uow.query_one(
                        "SELECT operation_id FROM activity_task_reward_claim_reservations "
                        "WHERE activity_key=? AND user_id=? AND scope_type=? AND scope_key=? AND task_key=?",
                        identity,
                    )
                    if reservation is not None and str(reservation["operation_id"]) != operation_id:
                        raise RuntimeError(f"activity task claim reservation conflict: {operation_id}")
                    uow.execute(
                        "INSERT OR IGNORE INTO activity_task_reward_claim_reservations"
                        "(activity_key,user_id,scope_type,scope_key,task_key,operation_id) VALUES(?,?,?,?,?,?)",
                        (*identity, operation_id),
                    )
            last_rowid = int(rows[-1]["legacy_rowid"])


def apply_activity_pass_claim(uow: DatabaseUnitOfWork) -> None:
    uow.execute(
        "CREATE TABLE IF NOT EXISTS activity_pass_reward_claim_operations("
        "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,request_json TEXT NOT NULL,"
        "result_json TEXT NOT NULL DEFAULT '[]',result_status TEXT NOT NULL DEFAULT 'started',"
        "status TEXT NOT NULL DEFAULT 'started',created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,"
        "updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP)"
    )
    uow.execute(
        "CREATE TABLE IF NOT EXISTS activity_pass_reward_claim_reservations("
        "activity_key TEXT NOT NULL,user_id TEXT NOT NULL,level INTEGER NOT NULL,operation_id TEXT NOT NULL,"
        "created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,"
        "PRIMARY KEY(activity_key,user_id,level),UNIQUE(operation_id,level))"
    )
    uow.execute(
        "CREATE INDEX IF NOT EXISTS idx_activity_pass_claim_reservations_operation "
        "ON activity_pass_reward_claim_reservations(operation_id)"
    )


def apply_activity_pass_claim_legacy_receipts(uow: DatabaseUnitOfWork) -> None:
    legacy_database = uow.database.parent / "activity" / "activity.db"
    if not legacy_database.is_file():
        return

    with DatabaseUnitOfWork(legacy_database, read_only=True) as legacy:
        receipt_columns = {str(row["name"]) for row in legacy.query_all(
            "PRAGMA table_info(activity_pass_claim_operations)"
        )}
        receipt_required = {"operation_id", "payload", "result_json", "created_at"}
        if receipt_columns and not receipt_required.issubset(receipt_columns):
            raise RuntimeError("legacy activity pass claim schema incomplete")

        last_rowid = 0
        while receipt_columns:
            rows = legacy.query_all(
                "SELECT rowid AS legacy_rowid,operation_id,payload,result_json,created_at "
                "FROM activity_pass_claim_operations WHERE rowid>? ORDER BY rowid LIMIT 200",
                (last_rowid,),
            )
            if not rows:
                break
            for row in rows:
                operation_id = str(row["operation_id"])
                try:
                    payload = json.loads(str(row["payload"]))
                    result = json.loads(str(row["result_json"]))
                except (TypeError, ValueError, json.JSONDecodeError) as exc:
                    raise RuntimeError(f"invalid legacy activity pass receipt: {operation_id}") from exc
                if (
                    not isinstance(payload, list) or len(payload) != 7
                    or not isinstance(result, list) or not isinstance(payload[3], list)
                    or not isinstance(payload[5], list)
                ):
                    raise RuntimeError(f"invalid legacy activity pass payload: {operation_id}")
                try:
                    user_id, activity_key = str(payload[0]).strip(), str(payload[1]).strip()
                    current_level, stone, max_goods_num = int(payload[2]), int(payload[4]), int(payload[6])
                    if not user_id or not activity_key or current_level < 0 or stone < 0 or max_goods_num < 0:
                        raise ValueError
                    normalized_items = []
                    for item in payload[5]:
                        if not isinstance(item, list) or len(item) != 4:
                            raise ValueError
                        item_id, item_name, item_type, quantity = int(item[0]), str(item[1]), str(item[2]), int(item[3])
                        if item_id <= 0 or not item_name or not item_type or quantity <= 0:
                            raise ValueError
                        normalized_items.append([item_id, item_name, item_type, quantity])
                except (TypeError, ValueError, OverflowError) as exc:
                    raise RuntimeError(f"invalid legacy activity pass payload: {operation_id}") from exc
                rewards = []
                levels = set()
                for reward in result:
                    if not isinstance(reward, list) or len(reward) != 3:
                        raise RuntimeError(f"invalid legacy activity pass reward: {operation_id}")
                    level = int(reward[0])
                    if level <= 0 or level > current_level or level in levels:
                        raise RuntimeError(f"invalid legacy activity pass level: {operation_id}")
                    levels.add(level)
                    rewards.append([level, str(reward[1]), str(reward[2])])
                if not rewards or rewards != payload[3]:
                    raise RuntimeError(f"legacy activity pass result mismatch: {operation_id}")
                request = {
                    "user_id": user_id,
                    "activity_key": activity_key,
                    "current_level": current_level,
                    "levels": [
                        {"level": level, "name": name, "reward": reward, "reward_items": []}
                        for level, name, reward in rewards
                    ],
                    "stone": stone,
                    "items": normalized_items,
                    "max_goods_num": max_goods_num,
                    "rewards": rewards,
                    "legacy_import": True,
                }
                existing = uow.query_one(
                    "SELECT payload,request_json,result_json,result_status,status,created_at "
                    "FROM activity_pass_reward_claim_operations "
                    "WHERE operation_id=?",
                    (operation_id,),
                )
                request_json = json.dumps(request, ensure_ascii=True, separators=(",", ":"))
                if existing is None:
                    uow.execute(
                        "INSERT INTO activity_pass_reward_claim_operations"
                        "(operation_id,payload,request_json,result_json,result_status,status,created_at) "
                        "VALUES(?,?,?,?, 'applied','applied',?)",
                        (operation_id, row["payload"], request_json,
                         row["result_json"], row["created_at"]),
                    )
                elif (
                    str(existing["payload"]) != str(row["payload"])
                    or str(existing["request_json"]) != request_json
                    or str(existing["result_json"]) != str(row["result_json"])
                    or str(existing["result_status"]) != "applied"
                    or str(existing["status"]) != "applied"
                    or str(existing["created_at"]) != str(row["created_at"])
                ):
                    raise RuntimeError(f"activity pass claim receipt conflict: {operation_id}")
                for level in levels:
                    identity = (activity_key, user_id, level)
                    reservation = uow.query_one(
                        "SELECT operation_id FROM activity_pass_reward_claim_reservations "
                        "WHERE activity_key=? AND user_id=? AND level=?",
                        identity,
                    )
                    if reservation is not None and str(reservation["operation_id"]) != operation_id:
                        raise RuntimeError(f"activity pass reservation conflict: {operation_id}")
                    uow.execute(
                        "INSERT OR IGNORE INTO activity_pass_reward_claim_reservations"
                        "(activity_key,user_id,level,operation_id) VALUES(?,?,?,?)",
                        (*identity, operation_id),
                    )
            last_rowid = int(rows[-1]["legacy_rowid"])

        claim_columns = {str(row["name"]) for row in legacy.query_all(
            "PRAGMA table_info(activity_pass_reward_claim)"
        )}
        claim_required = {"activity_key", "user_id", "level"}
        if claim_columns and not claim_required.issubset(claim_columns):
            raise RuntimeError("legacy activity pass reward schema incomplete")
        last_rowid = 0
        while claim_columns:
            rows = legacy.query_all(
                "SELECT rowid AS legacy_rowid,activity_key,user_id,level "
                "FROM activity_pass_reward_claim WHERE rowid>? ORDER BY rowid LIMIT 200",
                (last_rowid,),
            )
            if not rows:
                break
            for row in rows:
                activity_key, user_id, level = str(row["activity_key"]), str(row["user_id"]), int(row["level"])
                identity = (activity_key, user_id, level)
                reservation = uow.query_one(
                    "SELECT operation_id FROM activity_pass_reward_claim_reservations "
                    "WHERE activity_key=? AND user_id=? AND level=?",
                    identity,
                )
                if reservation is not None:
                    owner = str(reservation["operation_id"])
                    if owner.startswith("legacy-claimed:"):
                        continue
                    operation = uow.query_one(
                        "SELECT payload,result_json,status FROM activity_pass_reward_claim_operations "
                        "WHERE operation_id=?",
                        (owner,),
                    )
                    if operation is None or str(operation["status"]) != "applied":
                        raise RuntimeError(f"activity pass reservation state conflict: {owner}")
                    receipt_payload = json.loads(str(operation["payload"]))
                    receipt_rewards = json.loads(str(operation["result_json"]))
                    if (
                        len(receipt_payload) != 7
                        or (str(receipt_payload[0]), str(receipt_payload[1])) != (user_id, activity_key)
                        or level not in {int(reward[0]) for reward in receipt_rewards}
                    ):
                        raise RuntimeError(f"activity pass claimed-level conflict: {owner}")
                    continue
                sentinel = "legacy-claimed:" + hashlib.sha256(
                    json.dumps(identity, ensure_ascii=True, separators=(",", ":")).encode("utf-8")
                ).hexdigest()
                uow.execute(
                    "INSERT INTO activity_pass_reward_claim_reservations"
                    "(activity_key,user_id,level,operation_id) VALUES(?,?,?,?)",
                    (*identity, sentinel),
                )
            last_rowid = int(rows[-1]["legacy_rowid"])


def apply_activity_boss_milestone_claim(uow: DatabaseUnitOfWork) -> None:
    uow.execute(
        "CREATE TABLE IF NOT EXISTS activity_boss_milestone_claim_operations("
        "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,request_json TEXT NOT NULL,"
        "result_json TEXT NOT NULL DEFAULT '{}',result_status TEXT NOT NULL DEFAULT 'started',"
        "status TEXT NOT NULL DEFAULT 'started',created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,"
        "updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP)"
    )
    uow.execute(
        "CREATE TABLE IF NOT EXISTS activity_boss_milestone_claim_reservations("
        "activity_key TEXT NOT NULL,user_id TEXT NOT NULL,milestone_key TEXT NOT NULL,operation_id TEXT NOT NULL,"
        "created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,"
        "PRIMARY KEY(activity_key,user_id,milestone_key),UNIQUE(operation_id,milestone_key))"
    )
    uow.execute(
        "CREATE INDEX IF NOT EXISTS idx_activity_boss_milestone_reservations_operation "
        "ON activity_boss_milestone_claim_reservations(operation_id)"
    )


def apply_activity_boss_milestone_legacy_receipts(uow: DatabaseUnitOfWork) -> None:
    legacy_database = uow.database.parent / "activity" / "activity.db"
    if not legacy_database.is_file():
        return

    with DatabaseUnitOfWork(legacy_database, read_only=True) as legacy:
        receipt_columns = {str(row["name"]) for row in legacy.query_all(
            "PRAGMA table_info(activity_boss_reward_claim_operations)"
        )}
        receipt_required = {"operation_id", "payload", "result_json", "created_at"}
        if receipt_columns and not receipt_required.issubset(receipt_columns):
            raise RuntimeError("legacy activity boss reward claim schema incomplete")

        last_rowid = 0
        while receipt_columns:
            rows = legacy.query_all(
                "SELECT rowid AS legacy_rowid,operation_id,payload,result_json,created_at "
                "FROM activity_boss_reward_claim_operations WHERE rowid>? ORDER BY rowid LIMIT 200",
                (last_rowid,),
            )
            if not rows:
                break
            for row in rows:
                operation_id = str(row["operation_id"])
                try:
                    payload = json.loads(str(row["payload"]))
                    result = json.loads(str(row["result_json"]))
                except (TypeError, ValueError, json.JSONDecodeError) as exc:
                    raise RuntimeError(f"invalid legacy activity boss reward receipt: {operation_id}") from exc
                if not isinstance(payload, list):
                    raise RuntimeError(f"invalid legacy activity boss reward payload: {operation_id}")
                if len(payload) == 5:
                    last_rowid = int(row["legacy_rowid"])
                    continue
                if len(payload) != 3 or not isinstance(payload[2], list):
                    raise RuntimeError(f"invalid legacy activity boss milestone payload: {operation_id}")
                user_id, activity_key = str(payload[0]).strip(), str(payload[1]).strip()
                pending = []
                seen = set()
                for milestone in payload[2]:
                    if not isinstance(milestone, list) or len(milestone) != 3:
                        raise RuntimeError(f"invalid legacy activity boss milestone: {operation_id}")
                    key, name, reward = map(str, milestone)
                    if not key or key in seen:
                        raise RuntimeError(f"invalid legacy activity boss milestone key: {operation_id}")
                    seen.add(key)
                    pending.append([key, name, reward])
                if (
                    not operation_id.strip() or not user_id or not activity_key or not pending
                    or not isinstance(result, dict)
                    or not isinstance(result.get("names"), list)
                    or result["names"] != [milestone[1] for milestone in pending]
                    or result.get("rank") != 0
                ):
                    raise RuntimeError(f"legacy activity boss milestone result mismatch: {operation_id}")
                request = {
                    "user_id": user_id,
                    "activity_key": activity_key,
                    "milestones": [
                        {"key": key, "name": name, "reward": reward, "reward_items": []}
                        for key, name, reward in pending
                    ],
                    "names": [row[1] for row in pending],
                    "stone": 0,
                    "items": [],
                    "max_goods_num": 0,
                    "legacy_import": True,
                }
                existing = uow.query_one(
                    "SELECT payload,request_json,result_json,result_status,status,created_at "
                    "FROM activity_boss_milestone_claim_operations WHERE operation_id=?",
                    (operation_id,),
                )
                request_json = json.dumps(request, ensure_ascii=True, separators=(",", ":"))
                if existing is None:
                    uow.execute(
                        "INSERT INTO activity_boss_milestone_claim_operations"
                        "(operation_id,payload,request_json,result_json,result_status,status,created_at) "
                        "VALUES(?,?,?,?, 'applied','applied',?)",
                        (operation_id, row["payload"], request_json, row["result_json"], row["created_at"]),
                    )
                elif (
                    str(existing["payload"]) != str(row["payload"])
                    or str(existing["request_json"]) != request_json
                    or str(existing["result_json"]) != str(row["result_json"])
                    or str(existing["result_status"]) != "applied"
                    or str(existing["status"]) != "applied"
                    or str(existing["created_at"]) != str(row["created_at"])
                ):
                    raise RuntimeError(f"activity boss milestone receipt conflict: {operation_id}")
                for key, _, _ in pending:
                    identity = (activity_key, user_id, key)
                    reservation = uow.query_one(
                        "SELECT operation_id FROM activity_boss_milestone_claim_reservations "
                        "WHERE activity_key=? AND user_id=? AND milestone_key=?",
                        identity,
                    )
                    if reservation is not None and str(reservation["operation_id"]) != operation_id:
                        raise RuntimeError(f"activity boss milestone reservation conflict: {operation_id}")
                    uow.execute(
                        "INSERT OR IGNORE INTO activity_boss_milestone_claim_reservations"
                        "(activity_key,user_id,milestone_key,operation_id) VALUES(?,?,?,?)",
                        (*identity, operation_id),
                    )
            last_rowid = int(rows[-1]["legacy_rowid"])

        claim_columns = {str(row["name"]) for row in legacy.query_all(
            "PRAGMA table_info(activity_boss_milestone_claim)"
        )}
        claim_required = {"activity_key", "user_id", "milestone_key"}
        if claim_columns and not claim_required.issubset(claim_columns):
            raise RuntimeError("legacy activity boss milestone claim schema incomplete")
        last_rowid = 0
        while claim_columns:
            rows = legacy.query_all(
                "SELECT rowid AS legacy_rowid,activity_key,user_id,milestone_key "
                "FROM activity_boss_milestone_claim WHERE rowid>? ORDER BY rowid LIMIT 200",
                (last_rowid,),
            )
            if not rows:
                break
            for row in rows:
                activity_key = str(row["activity_key"])
                user_id = str(row["user_id"])
                milestone_key = str(row["milestone_key"])
                identity = (activity_key, user_id, milestone_key)
                reservation = uow.query_one(
                    "SELECT operation_id FROM activity_boss_milestone_claim_reservations "
                    "WHERE activity_key=? AND user_id=? AND milestone_key=?",
                    identity,
                )
                if reservation is not None:
                    owner = str(reservation["operation_id"])
                    if owner.startswith("legacy-claimed:"):
                        continue
                    operation = uow.query_one(
                        "SELECT payload,result_json,status FROM activity_boss_milestone_claim_operations "
                        "WHERE operation_id=?",
                        (owner,),
                    )
                    if operation is None or str(operation["status"]) != "applied":
                        raise RuntimeError(f"activity boss milestone reservation state conflict: {owner}")
                    receipt_payload = json.loads(str(operation["payload"]))
                    receipt_result = json.loads(str(operation["result_json"]))
                    if (
                        len(receipt_payload) != 3
                        or (str(receipt_payload[0]), str(receipt_payload[1])) != (user_id, activity_key)
                        or milestone_key not in {str(item[0]) for item in receipt_payload[2]}
                        or receipt_result.get("rank") != 0
                    ):
                        raise RuntimeError(f"activity boss milestone claimed-state conflict: {owner}")
                    continue
                sentinel = "legacy-claimed:" + hashlib.sha256(
                    json.dumps(identity, ensure_ascii=True, separators=(",", ":")).encode("utf-8")
                ).hexdigest()
                uow.execute(
                    "INSERT INTO activity_boss_milestone_claim_reservations"
                    "(activity_key,user_id,milestone_key,operation_id) VALUES(?,?,?,?)",
                    (*identity, sentinel),
                )
            last_rowid = int(rows[-1]["legacy_rowid"])


def apply_activity_boss_rank_claim(uow: DatabaseUnitOfWork) -> None:
    uow.execute(
        "CREATE TABLE IF NOT EXISTS activity_boss_rank_claim_operations("
        "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,request_json TEXT NOT NULL,"
        "result_json TEXT NOT NULL DEFAULT '{}',result_status TEXT NOT NULL DEFAULT 'started',"
        "status TEXT NOT NULL DEFAULT 'started',created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,"
        "updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP)"
    )
    uow.execute(
        "CREATE TABLE IF NOT EXISTS activity_boss_rank_claim_reservations("
        "activity_key TEXT NOT NULL,user_id TEXT NOT NULL,tier_key TEXT NOT NULL,operation_id TEXT NOT NULL,"
        "created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,"
        "PRIMARY KEY(activity_key,user_id,tier_key),UNIQUE(operation_id,tier_key))"
    )
    uow.execute(
        "CREATE INDEX IF NOT EXISTS idx_activity_boss_rank_reservations_operation "
        "ON activity_boss_rank_claim_reservations(operation_id)"
    )


def apply_activity_boss_rank_legacy_receipts(uow: DatabaseUnitOfWork) -> None:
    legacy_database = uow.database.parent / "activity" / "activity.db"
    if not legacy_database.is_file():
        return

    with DatabaseUnitOfWork(legacy_database, read_only=True) as legacy:
        receipt_columns = {str(row["name"]) for row in legacy.query_all(
            "PRAGMA table_info(activity_boss_reward_claim_operations)"
        )}
        receipt_required = {"operation_id", "payload", "result_json", "created_at"}
        if receipt_columns and not receipt_required.issubset(receipt_columns):
            raise RuntimeError("legacy activity boss reward claim schema incomplete")

        last_rowid = 0
        while receipt_columns:
            rows = legacy.query_all(
                "SELECT rowid AS legacy_rowid,operation_id,payload,result_json,created_at "
                "FROM activity_boss_reward_claim_operations WHERE rowid>? ORDER BY rowid LIMIT 200",
                (last_rowid,),
            )
            if not rows:
                break
            for row in rows:
                operation_id = str(row["operation_id"])
                try:
                    payload = json.loads(str(row["payload"]))
                    result = json.loads(str(row["result_json"]))
                except (TypeError, ValueError, json.JSONDecodeError) as exc:
                    raise RuntimeError(f"invalid legacy activity boss rank receipt: {operation_id}") from exc
                if not isinstance(payload, list):
                    raise RuntimeError(f"invalid legacy activity boss rank payload: {operation_id}")
                if len(payload) == 3:
                    last_rowid = int(row["legacy_rowid"])
                    continue
                if len(payload) != 5:
                    raise RuntimeError(f"invalid legacy activity boss rank payload: {operation_id}")
                try:
                    user_id, activity_key = str(payload[0]).strip(), str(payload[1]).strip()
                    rank, tier_key, reward = int(payload[2]), str(payload[3]), str(payload[4])
                    names = result.get("names") if isinstance(result, dict) else None
                    result_rank = result.get("rank") if isinstance(result, dict) else None
                except (TypeError, ValueError, OverflowError) as exc:
                    raise RuntimeError(f"invalid legacy activity boss rank receipt: {operation_id}") from exc
                if (
                    not operation_id.strip() or not user_id or not activity_key or rank <= 0 or not tier_key
                    or not isinstance(names, list) or len(names) != 1 or not isinstance(names[0], str)
                    or not names[0] or result_rank != rank
                ):
                    raise RuntimeError(f"legacy activity boss rank result mismatch: {operation_id}")
                request = {
                    "user_id": user_id,
                    "activity_key": activity_key,
                    "tiers": [],
                    "max_goods_num": 0,
                    "rank": rank,
                    "tier_key": tier_key,
                    "name": names[0],
                    "reward": reward,
                    "reward_items": [],
                    "legacy_import": True,
                }
                existing = uow.query_one(
                    "SELECT payload,request_json,result_json,result_status,status,created_at "
                    "FROM activity_boss_rank_claim_operations WHERE operation_id=?",
                    (operation_id,),
                )
                request_json = json.dumps(request, ensure_ascii=True, separators=(",", ":"))
                if existing is None:
                    uow.execute(
                        "INSERT INTO activity_boss_rank_claim_operations"
                        "(operation_id,payload,request_json,result_json,result_status,status,created_at) "
                        "VALUES(?,?,?,?, 'applied','applied',?)",
                        (operation_id, row["payload"], request_json, row["result_json"], row["created_at"]),
                    )
                elif (
                    str(existing["payload"]) != str(row["payload"])
                    or str(existing["request_json"]) != request_json
                    or str(existing["result_json"]) != str(row["result_json"])
                    or str(existing["result_status"]) != "applied"
                    or str(existing["status"]) != "applied"
                    or str(existing["created_at"]) != str(row["created_at"])
                ):
                    raise RuntimeError(f"activity boss rank receipt conflict: {operation_id}")
                identity = (activity_key, user_id, tier_key)
                reservation = uow.query_one(
                    "SELECT operation_id FROM activity_boss_rank_claim_reservations "
                    "WHERE activity_key=? AND user_id=? AND tier_key=?",
                    identity,
                )
                if reservation is not None and str(reservation["operation_id"]) != operation_id:
                    raise RuntimeError(f"activity boss rank reservation conflict: {operation_id}")
                uow.execute(
                    "INSERT OR IGNORE INTO activity_boss_rank_claim_reservations"
                    "(activity_key,user_id,tier_key,operation_id) VALUES(?,?,?,?)",
                    (*identity, operation_id),
                )
            last_rowid = int(rows[-1]["legacy_rowid"])

        claim_columns = {str(row["name"]) for row in legacy.query_all(
            "PRAGMA table_info(activity_boss_rank_claim)"
        )}
        claim_required = {"activity_key", "user_id", "tier_key"}
        if claim_columns and not claim_required.issubset(claim_columns):
            raise RuntimeError("legacy activity boss rank claim schema incomplete")
        last_rowid = 0
        while claim_columns:
            rows = legacy.query_all(
                "SELECT rowid AS legacy_rowid,activity_key,user_id,tier_key "
                "FROM activity_boss_rank_claim WHERE rowid>? ORDER BY rowid LIMIT 200",
                (last_rowid,),
            )
            if not rows:
                break
            for row in rows:
                activity_key, user_id, tier_key = str(row["activity_key"]), str(row["user_id"]), str(row["tier_key"])
                identity = (activity_key, user_id, tier_key)
                reservation = uow.query_one(
                    "SELECT operation_id FROM activity_boss_rank_claim_reservations "
                    "WHERE activity_key=? AND user_id=? AND tier_key=?",
                    identity,
                )
                if reservation is not None:
                    owner = str(reservation["operation_id"])
                    if owner.startswith("legacy-claimed:"):
                        continue
                    operation = uow.query_one(
                        "SELECT payload,result_json,status FROM activity_boss_rank_claim_operations "
                        "WHERE operation_id=?",
                        (owner,),
                    )
                    if operation is None or str(operation["status"]) != "applied":
                        raise RuntimeError(f"activity boss rank reservation state conflict: {owner}")
                    receipt_payload = json.loads(str(operation["payload"]))
                    receipt_result = json.loads(str(operation["result_json"]))
                    if (
                        len(receipt_payload) != 5
                        or (str(receipt_payload[0]), str(receipt_payload[1]), str(receipt_payload[3]))
                        != (user_id, activity_key, tier_key)
                        or int(receipt_result.get("rank", 0)) <= 0
                    ):
                        raise RuntimeError(f"activity boss rank claimed-tier conflict: {owner}")
                    continue
                sentinel = "legacy-claimed:" + hashlib.sha256(
                    json.dumps(identity, ensure_ascii=True, separators=(",", ":")).encode("utf-8")
                ).hexdigest()
                uow.execute(
                    "INSERT INTO activity_boss_rank_claim_reservations"
                    "(activity_key,user_id,tier_key,operation_id) VALUES(?,?,?,?)",
                    (*identity, sentinel),
                )
            last_rowid = int(rows[-1]["legacy_rowid"])


__all__ = [
    "apply_activity_claim_all",
    "apply_activity_claim_all_legacy_receipts",
    "apply_activity_reward",
    "apply_activity_task_claim",
    "apply_activity_task_claim_legacy_receipts",
    "apply_activity_pass_claim",
    "apply_activity_pass_claim_legacy_receipts",
    "apply_activity_boss_milestone_claim",
    "apply_activity_boss_milestone_legacy_receipts",
    "apply_activity_boss_rank_claim",
    "apply_activity_boss_rank_legacy_receipts",
]
