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


__all__ = ["apply_activity_claim_all", "apply_activity_claim_all_legacy_receipts", "apply_activity_reward"]
