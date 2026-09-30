from ...infrastructure.database import DatabaseUnitOfWork


def apply_base(uow: DatabaseUnitOfWork) -> None:
    uow.execute("CREATE TABLE IF NOT EXISTS base_feature_migrations (version TEXT PRIMARY KEY)")
    uow.execute("INSERT OR IGNORE INTO base_feature_migrations(version) VALUES ('base.001')")


def apply_base_player_rename_operations(uow: DatabaseUnitOfWork) -> None:
    uow.execute(
        "CREATE TABLE IF NOT EXISTS player_rename_operations("
        "operation_id TEXT PRIMARY KEY,user_id TEXT NOT NULL,rename_type TEXT NOT NULL,"
        "new_name TEXT NOT NULL,previous_name TEXT NOT NULL,"
        "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,payload TEXT)"
    )
    columns = {
        str(row["name"]).casefold()
        for row in uow.query_all('PRAGMA table_info("player_rename_operations")')
    }
    required = {"operation_id", "user_id", "rename_type", "new_name", "previous_name"}
    if not required.issubset(columns):
        raise RuntimeError("player_rename_operations has an unsupported legacy schema")
    if "payload" not in columns:
        uow.execute("ALTER TABLE player_rename_operations ADD COLUMN payload TEXT")


def apply_base_stone_contest_operations(uow: DatabaseUnitOfWork) -> None:
    uow.execute(
        "CREATE TABLE IF NOT EXISTS stone_contest_operations("
        "operation_id TEXT PRIMARY KEY,payer_id TEXT NOT NULL,receiver_id TEXT NOT NULL,"
        "requested_amount INTEGER NOT NULL,transferred_amount INTEGER NOT NULL,"
        "payer_balance INTEGER NOT NULL,operation_type TEXT NOT NULL DEFAULT 'transfer',"
        "thief_id TEXT NOT NULL DEFAULT '',victim_id TEXT NOT NULL DEFAULT '',"
        "outcome TEXT NOT NULL DEFAULT '',penalty_amount INTEGER NOT NULL DEFAULT 0,"
        "stamina_cost INTEGER NOT NULL DEFAULT 0,thief_stamina INTEGER NOT NULL DEFAULT 0,"
        "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
    )
    additions = {
        "operation_type": "TEXT NOT NULL DEFAULT 'transfer'",
        "thief_id": "TEXT NOT NULL DEFAULT ''",
        "victim_id": "TEXT NOT NULL DEFAULT ''",
        "outcome": "TEXT NOT NULL DEFAULT ''",
        "penalty_amount": "INTEGER NOT NULL DEFAULT 0",
        "stamina_cost": "INTEGER NOT NULL DEFAULT 0",
        "thief_stamina": "INTEGER NOT NULL DEFAULT 0",
    }
    columns = {
        str(row["name"]).casefold()
        for row in uow.query_all('PRAGMA table_info("stone_contest_operations")')
    }
    required = {
        "operation_id", "payer_id", "receiver_id", "requested_amount",
        "transferred_amount", "payer_balance",
    }
    if not required.issubset(columns):
        raise RuntimeError("stone_contest_operations has an unsupported legacy schema")
    for name, definition in additions.items():
        if name not in columns:
            uow.execute(
                f"ALTER TABLE stone_contest_operations ADD COLUMN {name} {definition}"
            )


__all__ = [
    "apply_base",
    "apply_base_player_rename_operations",
    "apply_base_stone_contest_operations",
]
