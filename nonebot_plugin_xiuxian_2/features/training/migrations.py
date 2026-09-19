from ...infrastructure.database import DatabaseUnitOfWork


def apply_training(uow: DatabaseUnitOfWork) -> None:
    uow.execute("CREATE TABLE IF NOT EXISTS training_feature_migrations (version TEXT PRIMARY KEY)")
    uow.execute("INSERT OR IGNORE INTO training_feature_migrations(version) VALUES ('legacy.training.001')")


def apply_training_state(uow: DatabaseUnitOfWork) -> None:
    uow.execute(
        "CREATE TABLE IF NOT EXISTS training ("
        "user_id TEXT PRIMARY KEY,progress INTEGER DEFAULT 0,last_time TEXT DEFAULT NULL,"
        "points INTEGER DEFAULT 0,completed INTEGER DEFAULT 0,max_progress INTEGER DEFAULT 0,"
        "last_event TEXT DEFAULT '',weekly_purchases TEXT DEFAULT NULL)"
    )
    columns = {str(row[1]) for row in uow.execute("PRAGMA table_info(training)").fetchall()}
    for name, definition in {
        "progress": "INTEGER DEFAULT 0", "last_time": "TEXT DEFAULT NULL",
        "points": "INTEGER DEFAULT 0", "completed": "INTEGER DEFAULT 0",
        "max_progress": "INTEGER DEFAULT 0", "last_event": "TEXT DEFAULT ''",
        "weekly_purchases": "TEXT DEFAULT NULL",
    }.items():
        if name not in columns:
            uow.execute(f'ALTER TABLE training ADD COLUMN "{name}" {definition}')
    uow.execute(
        "CREATE TABLE IF NOT EXISTS training_state_operations ("
        "operation_id TEXT PRIMARY KEY,user_id TEXT NOT NULL,kind TEXT NOT NULL,"
        "period_key TEXT NOT NULL,snapshot TEXT NOT NULL,"
        "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
    )


__all__ = ["apply_training", "apply_training_state"]
