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


__all__ = [
    "apply_activity_claim_all",
    "apply_activity_claim_all_legacy_receipts",
    "apply_activity_reward",
    "apply_activity_task_claim",
    "apply_activity_task_claim_legacy_receipts",
]
