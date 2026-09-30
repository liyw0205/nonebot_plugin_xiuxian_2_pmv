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


__all__ = ["apply_base", "apply_base_player_rename_operations"]
