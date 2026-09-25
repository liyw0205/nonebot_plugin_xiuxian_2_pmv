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


def apply_rift_speedup_operations(uow: DatabaseUnitOfWork) -> None:
    uow.execute(
        "CREATE TABLE IF NOT EXISTS rift_speedup_operations("
        "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,new_time INTEGER NOT NULL,"
        "rift_data TEXT NOT NULL DEFAULT '{}',create_time TEXT,"
        "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
    )
    columns = {
        str(row["name"])
        for row in uow.query_all("PRAGMA table_info(rift_speedup_operations)")
    }
    if "rift_data" not in columns:
        uow.execute(
            "ALTER TABLE rift_speedup_operations "
            "ADD COLUMN rift_data TEXT NOT NULL DEFAULT '{}'"
        )
    if "create_time" not in columns:
        uow.execute("ALTER TABLE rift_speedup_operations ADD COLUMN create_time TEXT")


def apply_rift_world_generation(uow: DatabaseUnitOfWork) -> None:
    uow.execute(
        "CREATE TABLE IF NOT EXISTS rift_world_state("
        "rift_key TEXT PRIMARY KEY,generation_id TEXT NOT NULL,"
        "rift_data TEXT NOT NULL,participants TEXT NOT NULL,revision INTEGER NOT NULL,"
        "updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
    )
    uow.execute(
        "CREATE TABLE IF NOT EXISTS rift_generation_operations("
        "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,rift_key TEXT NOT NULL,"
        "generation_id TEXT NOT NULL,rift_data TEXT NOT NULL,revision INTEGER NOT NULL,"
        "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
    )


def apply_rift_termination_operations(uow: DatabaseUnitOfWork) -> None:
    uow.execute(
        "CREATE TABLE IF NOT EXISTS rift_termination_operations("
        "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,"
        "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
    )


def apply_rift_key_event_operations(uow: DatabaseUnitOfWork) -> None:
    uow.execute(
        "CREATE TABLE IF NOT EXISTS rift_key_event_operations("
        "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,"
        "explore_count INTEGER NOT NULL,message TEXT NOT NULL,"
        "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
    )


__all__ = [
    "apply_rift",
    "apply_rift_demon_token_operations",
    "apply_rift_demon_token_player_schema",
    "apply_rift_speedup_operations",
    "apply_rift_world_generation",
    "apply_rift_termination_operations",
    "apply_rift_key_event_operations",
]
