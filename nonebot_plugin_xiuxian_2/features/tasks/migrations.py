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


__all__ = ["apply_tasks", "apply_task_progress"]
