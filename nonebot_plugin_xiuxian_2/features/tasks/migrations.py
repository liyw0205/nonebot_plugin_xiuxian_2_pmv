from ...infrastructure.database import DatabaseUnitOfWork


def apply_tasks(uow: DatabaseUnitOfWork) -> None:
    uow.execute("CREATE TABLE IF NOT EXISTS tasks_feature_migrations (version TEXT PRIMARY KEY)")
    uow.execute("INSERT OR IGNORE INTO tasks_feature_migrations(version) VALUES ('legacy.tasks.001')")


def apply_task_progress(uow: DatabaseUnitOfWork) -> None:
    uow.execute("CREATE TABLE IF NOT EXISTS xiuxian_tasks(user_id TEXT PRIMARY KEY)")
    columns = {
        str(row["name"])
        for row in uow.query_all("PRAGMA table_info(xiuxian_tasks)")
    }
    for cycle in ("daily", "weekly"):
        for suffix in ("period", "progress", "claimed"):
            field = f"{cycle}_{suffix}"
            if field not in columns:
                uow.execute(f'ALTER TABLE xiuxian_tasks ADD COLUMN "{field}" TEXT')
    uow.execute(
        "CREATE TABLE IF NOT EXISTS task_progress_event_operations("
        "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,result_json TEXT NOT NULL,"
        "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
    )


def apply_task_claim(uow: DatabaseUnitOfWork) -> None:
    uow.execute(
        "CREATE TABLE IF NOT EXISTS task_reward_claim_operations("
        "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,result_json TEXT NOT NULL,"
        "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
    )
    uow.execute(
        "CREATE TABLE IF NOT EXISTS economy_log("
        "id INTEGER PRIMARY KEY AUTOINCREMENT,user_id TEXT,sect_id INTEGER,"
        "source TEXT NOT NULL,action TEXT NOT NULL,"
        "stone_delta INTEGER NOT NULL DEFAULT 0,exp_delta INTEGER NOT NULL DEFAULT 0,"
        "sect_contribution_delta INTEGER NOT NULL DEFAULT 0,"
        "sect_scale_delta INTEGER NOT NULL DEFAULT 0,"
        "sect_materials_delta INTEGER NOT NULL DEFAULT 0,"
        "item_delta TEXT NOT NULL DEFAULT '[]',detail TEXT NOT NULL DEFAULT '{}',"
        "trace_id TEXT,created_at TEXT NOT NULL)"
    )
    columns = {
        str(row["name"])
        for row in uow.query_all("PRAGMA table_info(economy_log)")
    }
    definitions = {
        "user_id": "TEXT",
        "sect_id": "INTEGER",
        "source": "TEXT NOT NULL DEFAULT ''",
        "action": "TEXT NOT NULL DEFAULT ''",
        "stone_delta": "INTEGER NOT NULL DEFAULT 0",
        "exp_delta": "INTEGER NOT NULL DEFAULT 0",
        "sect_contribution_delta": "INTEGER NOT NULL DEFAULT 0",
        "sect_scale_delta": "INTEGER NOT NULL DEFAULT 0",
        "sect_materials_delta": "INTEGER NOT NULL DEFAULT 0",
        "item_delta": "TEXT NOT NULL DEFAULT '[]'",
        "detail": "TEXT NOT NULL DEFAULT '{}'",
        "trace_id": "TEXT",
        "created_at": "TEXT NOT NULL DEFAULT ''",
    }
    for field, definition in definitions.items():
        if field not in columns:
            uow.execute(f'ALTER TABLE economy_log ADD COLUMN "{field}" {definition}')


def apply_task_claim_recovery(uow: DatabaseUnitOfWork) -> None:
    columns = {
        str(row["name"])
        for row in uow.query_all("PRAGMA table_info(task_reward_claim_operations)")
    }
    additions = {
        "request_json": "TEXT NOT NULL DEFAULT '{}'",
        "prepared_json": "TEXT NOT NULL DEFAULT '{}'",
        "result_status": "TEXT NOT NULL DEFAULT 'applied'",
        "status": "TEXT NOT NULL DEFAULT 'applied'",
        "updated_at": "TEXT NOT NULL DEFAULT ''",
    }
    for field, definition in additions.items():
        if field not in columns:
            uow.execute(
                f'ALTER TABLE task_reward_claim_operations ADD COLUMN "{field}" {definition}'
            )


def apply_task_claim_player(uow: DatabaseUnitOfWork) -> None:
    uow.execute(
        "CREATE TABLE IF NOT EXISTS task_reward_claim_player_operations("
        "operation_id TEXT PRIMARY KEY,user_id TEXT NOT NULL,payload TEXT NOT NULL,"
        "prepared_json TEXT NOT NULL,status TEXT NOT NULL,result_status TEXT NOT NULL,"
        "result_json TEXT NOT NULL,created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,"
        "updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP)"
    )
    uow.execute(
        "CREATE TABLE IF NOT EXISTS task_reward_claim_reservations("
        "user_id TEXT NOT NULL,cycle TEXT NOT NULL,period TEXT NOT NULL,task_key TEXT NOT NULL,"
        "operation_id TEXT NOT NULL,created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,"
        "PRIMARY KEY(user_id,cycle,period,task_key),UNIQUE(operation_id,cycle,task_key))"
    )
    uow.execute(
        "CREATE INDEX IF NOT EXISTS idx_task_reward_claim_reservations_operation "
        "ON task_reward_claim_reservations(operation_id)"
    )


__all__ = [
    "apply_tasks",
    "apply_task_progress",
    "apply_task_claim",
    "apply_task_claim_recovery",
    "apply_task_claim_player",
]
