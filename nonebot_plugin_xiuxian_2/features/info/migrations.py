from ...infrastructure.database import DatabaseUnitOfWork


def apply_info(uow: DatabaseUnitOfWork) -> None:
    uow.execute("CREATE TABLE IF NOT EXISTS info_feature_migrations (version TEXT PRIMARY KEY)")
    uow.execute("INSERT OR IGNORE INTO info_feature_migrations(version) VALUES ('legacy.info.001')")


def apply_avatar_identity_player(uow: DatabaseUnitOfWork) -> None:
    existing = uow.query_one(
        "SELECT type FROM sqlite_master WHERE name='avatar' LIMIT 1"
    )
    if existing is not None and existing["type"] != "table":
        raise RuntimeError("avatar identity migration requires a table named avatar")
    uow.execute(
        "CREATE TABLE IF NOT EXISTS avatar ("
        "user_id TEXT PRIMARY KEY, main_id TEXT, avatar_id TEXT, active_id TEXT, create_time TEXT)"
    )
    columns = {
        str(row["name"]).casefold()
        for row in uow.query_all('PRAGMA table_info("avatar")')
    }
    for name in ("main_id", "avatar_id", "active_id", "create_time"):
        if name not in columns:
            uow.execute(f'ALTER TABLE avatar ADD COLUMN "{name}" TEXT')
    uow.execute(
        "CREATE TABLE IF NOT EXISTS avatar_operation_receipts ("
        "operation_id TEXT NOT NULL, action TEXT NOT NULL, request_hash TEXT NOT NULL, "
        "result_json TEXT NOT NULL, created_at TEXT NOT NULL, "
        "PRIMARY KEY(operation_id, action))"
    )


def apply_avatar_initialization_player(uow: DatabaseUnitOfWork) -> None:
    uow.execute(
        "CREATE TABLE IF NOT EXISTS avatar_initialization_plans ("
        "operation_id TEXT PRIMARY KEY, user_id TEXT NOT NULL, avatar_id TEXT NOT NULL, "
        "create_time TEXT NOT NULL, request_hash TEXT NOT NULL, created_at TEXT NOT NULL)"
    )


__all__ = ["apply_avatar_identity_player", "apply_avatar_initialization_player", "apply_info"]
