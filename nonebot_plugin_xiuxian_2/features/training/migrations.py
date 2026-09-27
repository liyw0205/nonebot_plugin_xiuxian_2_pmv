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


def apply_training_event_operations(uow: DatabaseUnitOfWork) -> None:
    """Create the game-side idempotency projection before traffic arrives."""
    uow.execute(
        "CREATE TABLE IF NOT EXISTS training_event_operations ("
        "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,result_json TEXT NOT NULL DEFAULT '',"
        "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
    )
    columns = {
        str(row["name"])
        for row in uow.query_all("PRAGMA table_info(training_event_operations)")
    }
    if "result_json" not in columns:
        uow.execute(
            "ALTER TABLE training_event_operations "
            "ADD COLUMN result_json TEXT NOT NULL DEFAULT ''"
        )


def apply_training_event_player(uow: DatabaseUnitOfWork) -> None:
    """Prepare player-owned training state and statistics without request DDL."""
    apply_training_state(uow)
    uow.execute("CREATE TABLE IF NOT EXISTS statistics(user_id TEXT PRIMARY KEY)")
    columns = {
        str(row["name"])
        for row in uow.query_all("PRAGMA table_info(statistics)")
    }
    if "历练次数" not in columns:
        uow.execute('ALTER TABLE statistics ADD COLUMN "历练次数" INTEGER DEFAULT 0')


def apply_training_purchase_operations(uow: DatabaseUnitOfWork) -> None:
    """Prepare the game-side idempotency ledger before purchase traffic."""
    uow.execute(
        "CREATE TABLE IF NOT EXISTS training_purchase_operations ("
        "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,"
        "quantity INTEGER NOT NULL,cost INTEGER NOT NULL,points INTEGER NOT NULL,"
        "purchased INTEGER NOT NULL,inventory INTEGER NOT NULL,"
        "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
    )
    columns = {
        str(row["name"])
        for row in uow.query_all("PRAGMA table_info(training_purchase_operations)")
    }
    for name, definition in {
        "payload": "TEXT NOT NULL DEFAULT ''",
        "quantity": "INTEGER NOT NULL DEFAULT 0",
        "cost": "INTEGER NOT NULL DEFAULT 0",
        "points": "INTEGER NOT NULL DEFAULT 0",
        "purchased": "INTEGER NOT NULL DEFAULT 0",
        "inventory": "INTEGER NOT NULL DEFAULT 0",
    }.items():
        if name not in columns:
            uow.execute(
                f'ALTER TABLE training_purchase_operations ADD COLUMN "{name}" {definition}'
            )


def apply_training_reset_operations(uow: DatabaseUnitOfWork) -> None:
    """Prepare the game-owned resumable administrator reset tables."""
    uow.execute(
        "CREATE TABLE IF NOT EXISTS admin_training_reset_operations("
        "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,reset_date TEXT NOT NULL,"
        "total INTEGER NOT NULL,completed INTEGER NOT NULL DEFAULT 0,"
        "changed INTEGER NOT NULL DEFAULT 0,skipped INTEGER NOT NULL DEFAULT 0,"
        "status TEXT NOT NULL DEFAULT 'running',created_at TEXT NOT NULL,updated_at TEXT NOT NULL)"
    )
    operation_columns = {
        str(row["name"])
        for row in uow.query_all("PRAGMA table_info(admin_training_reset_operations)")
    }
    for name, definition in {
        "payload": "TEXT NOT NULL DEFAULT ''",
        "reset_date": "TEXT NOT NULL DEFAULT ''",
        "total": "INTEGER NOT NULL DEFAULT 0",
        "completed": "INTEGER NOT NULL DEFAULT 0",
        "changed": "INTEGER NOT NULL DEFAULT 0",
        "skipped": "INTEGER NOT NULL DEFAULT 0",
        "status": "TEXT NOT NULL DEFAULT 'running'",
        "created_at": "TEXT NOT NULL DEFAULT ''",
        "updated_at": "TEXT NOT NULL DEFAULT ''",
    }.items():
        if name not in operation_columns:
            uow.execute(
                f'ALTER TABLE admin_training_reset_operations ADD COLUMN "{name}" {definition}'
            )
    uow.execute(
        "CREATE TABLE IF NOT EXISTS admin_training_reset_targets("
        "operation_id TEXT NOT NULL,user_id TEXT NOT NULL,"
        "status TEXT NOT NULL DEFAULT 'pending',previous_state TEXT,updated_at TEXT NOT NULL,"
        "PRIMARY KEY(operation_id,user_id))"
    )
    target_columns = {
        str(row["name"])
        for row in uow.query_all("PRAGMA table_info(admin_training_reset_targets)")
    }
    for name, definition in {
        "status": "TEXT NOT NULL DEFAULT 'pending'",
        "previous_state": "TEXT",
        "updated_at": "TEXT NOT NULL DEFAULT ''",
    }.items():
        if name not in target_columns:
            uow.execute(
                f'ALTER TABLE admin_training_reset_targets ADD COLUMN "{name}" {definition}'
            )


__all__ = [
    "apply_training",
    "apply_training_event_operations",
    "apply_training_event_player",
    "apply_training_purchase_operations",
    "apply_training_reset_operations",
    "apply_training_state",
]
