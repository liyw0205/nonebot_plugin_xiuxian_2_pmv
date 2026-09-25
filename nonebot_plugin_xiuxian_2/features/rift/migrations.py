from ...infrastructure.database import DatabaseUnitOfWork


def apply_rift(uow: DatabaseUnitOfWork) -> None:
    uow.execute("CREATE TABLE IF NOT EXISTS rift_feature_migrations (version TEXT PRIMARY KEY)")
    uow.execute("INSERT OR IGNORE INTO rift_feature_migrations(version) VALUES ('rift.001')")


def apply_rift_demon_token_operations(uow: DatabaseUnitOfWork) -> None:
    uow.execute(
        "CREATE TABLE IF NOT EXISTS rift_demon_token_battle_operations("
        "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,explore_count INTEGER NOT NULL,"
        "message TEXT NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
    )


def apply_rift_demon_token_player_schema(uow: DatabaseUnitOfWork) -> None:
    uow.execute("CREATE TABLE IF NOT EXISTS rift(user_id TEXT PRIMARY KEY)")
    rift_columns = {str(row["name"]) for row in uow.query_all("PRAGMA table_info(rift)")}
    if "explore_count" not in rift_columns:
        uow.execute('ALTER TABLE rift ADD COLUMN "explore_count" INTEGER DEFAULT 0')

    uow.execute("CREATE TABLE IF NOT EXISTS statistics(user_id TEXT PRIMARY KEY)")
    statistics_columns = {
        str(row["name"]) for row in uow.query_all("PRAGMA table_info(statistics)")
    }
    for name in ("秘境打怪", "秘境次数", "rift_combat"):
        if name not in statistics_columns:
            uow.execute(f'ALTER TABLE statistics ADD COLUMN "{name}" INTEGER DEFAULT 0')


__all__ = ["apply_rift", "apply_rift_demon_token_operations", "apply_rift_demon_token_player_schema"]
