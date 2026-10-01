from ...infrastructure.database import DatabaseUnitOfWork


def apply_boss(uow: DatabaseUnitOfWork) -> None:
    uow.execute("CREATE TABLE IF NOT EXISTS boss_feature_migrations (version TEXT PRIMARY KEY)")
    uow.execute("INSERT OR IGNORE INTO boss_feature_migrations(version) VALUES ('boss.001')")


def apply_boss_purchase(uow: DatabaseUnitOfWork) -> None:
    uow.execute("CREATE TABLE IF NOT EXISTS boss_purchase_operations(operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,quantity INTEGER NOT NULL,cost INTEGER NOT NULL,integral INTEGER NOT NULL,purchased INTEGER NOT NULL,inventory INTEGER NOT NULL,created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP)")


def apply_boss_settlement(uow: DatabaseUnitOfWork) -> None:
    uow.execute("CREATE TABLE IF NOT EXISTS world_boss_battle_operations(operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,boss_hp INTEGER NOT NULL,stamina INTEGER NOT NULL,battle_count INTEGER NOT NULL,stone INTEGER NOT NULL,exp INTEGER NOT NULL,integral INTEGER NOT NULL,activity_lines TEXT NOT NULL,created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP)")


def apply_boss_player_schema(uow: DatabaseUnitOfWork) -> None:
    """Create player-owned world-boss lifecycle tables before runtime use."""
    uow.execute(
        "CREATE TABLE IF NOT EXISTS boss(user_id TEXT PRIMARY KEY,"
        "boss_integral INTEGER DEFAULT 0,boss_stone INTEGER DEFAULT 0,"
        "boss_battle_count INTEGER DEFAULT 0)"
    )
    columns = {str(row["name"]).casefold() for row in uow.query_all('PRAGMA table_info("boss")')}
    for name in ("boss_integral", "boss_stone", "boss_battle_count"):
        if name not in columns:
            uow.execute(f'ALTER TABLE boss ADD COLUMN "{name}" INTEGER DEFAULT 0')
    uow.execute(
        "CREATE TABLE IF NOT EXISTS world_boss_state("
        "state_key TEXT PRIMARY KEY,bosses TEXT NOT NULL,updated_at TEXT NOT NULL,"
        "revision INTEGER NOT NULL DEFAULT 0)"
    )
    state_columns = {str(row["name"]).casefold() for row in uow.query_all('PRAGMA table_info("world_boss_state")')}
    if "revision" not in state_columns:
        uow.execute('ALTER TABLE world_boss_state ADD COLUMN "revision" INTEGER NOT NULL DEFAULT 0')
    uow.execute(
        "CREATE TABLE IF NOT EXISTS world_boss_manual_spawn_operations("
        "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,result_json TEXT NOT NULL,"
        "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
    )
    uow.execute(
        "CREATE TABLE IF NOT EXISTS world_boss_daily_limit_reset_operations("
        "business_date TEXT PRIMARY KEY,total INTEGER NOT NULL,completed INTEGER NOT NULL DEFAULT 0,"
        "changed INTEGER NOT NULL DEFAULT 0,skipped INTEGER NOT NULL DEFAULT 0,"
        "status TEXT NOT NULL DEFAULT 'running',created_at TEXT NOT NULL,updated_at TEXT NOT NULL)"
    )
    uow.execute(
        "CREATE TABLE IF NOT EXISTS world_boss_daily_limit_reset_targets("
        "business_date TEXT NOT NULL,user_id TEXT NOT NULL,status TEXT NOT NULL DEFAULT 'pending',"
        "previous_integral INTEGER,previous_stone INTEGER,previous_battle_count INTEGER,"
        "updated_at TEXT NOT NULL,PRIMARY KEY(business_date,user_id))"
    )


__all__ = ["apply_boss", "apply_boss_purchase", "apply_boss_settlement", "apply_boss_player_schema"]
