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
        "CREATE TABLE IF NOT EXISTS boss_limit(user_id TEXT PRIMARY KEY,integral INTEGER NOT NULL DEFAULT 0)"
    )
    limit_columns = {str(row["name"]).casefold() for row in uow.query_all('PRAGMA table_info("boss_limit")')}
    if "integral" not in limit_columns:
        uow.execute('ALTER TABLE boss_limit ADD COLUMN "integral" INTEGER NOT NULL DEFAULT 0')
    uow.execute(
        "CREATE TABLE IF NOT EXISTS boss_weekly_purchases("
        "user_id TEXT PRIMARY KEY,weekly_purchases TEXT NOT NULL DEFAULT '{}')"
    )
    uow.execute(
        "CREATE TABLE IF NOT EXISTS world_boss_state("
        "state_key TEXT PRIMARY KEY,bosses TEXT NOT NULL,updated_at TEXT NOT NULL,"
        "revision INTEGER NOT NULL DEFAULT 0)"
    )
    state_columns = {str(row["name"]).casefold() for row in uow.query_all('PRAGMA table_info("world_boss_state")')}
    if "revision" not in state_columns:
        uow.execute('ALTER TABLE world_boss_state ADD COLUMN "revision" INTEGER NOT NULL DEFAULT 0')
    uow.execute("CREATE TABLE IF NOT EXISTS statistics(user_id TEXT PRIMARY KEY)")
    statistics_columns = {str(row["name"]) for row in uow.query_all('PRAGMA table_info("statistics")')}
    for name in ("讨伐世界BOSS", "击败世界BOSS"):
        if name not in statistics_columns:
            uow.execute(f'ALTER TABLE statistics ADD COLUMN "{name}" INTEGER DEFAULT 0')
    uow.execute("CREATE TABLE IF NOT EXISTS xiuxian_tasks(user_id TEXT PRIMARY KEY)")
    task_columns = {str(row["name"]) for row in uow.query_all('PRAGMA table_info("xiuxian_tasks")')}
    for name in ("daily_period", "daily_progress", "daily_claimed", "weekly_period", "weekly_progress", "weekly_claimed"):
        if name not in task_columns:
            uow.execute(f'ALTER TABLE xiuxian_tasks ADD COLUMN "{name}" TEXT')
    uow.execute(
        "CREATE TABLE IF NOT EXISTS world_boss_manual_spawn_operations("
        "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,result_json TEXT NOT NULL,"
        "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
    )
    apply_boss_full_refresh_player_schema(uow)
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


def apply_boss_full_refresh_player_schema(uow: DatabaseUnitOfWork) -> None:
    """Install the operation receipt table used by full session refreshes."""
    uow.execute(
        "CREATE TABLE IF NOT EXISTS world_boss_full_refresh_operations("
        "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,result_json TEXT NOT NULL,"
        "created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP)"
    )


def apply_boss_battle_player_schema(uow: DatabaseUnitOfWork) -> None:
    """Install player projections required by feature-owned battle settlement."""
    apply_boss_player_schema(uow)


__all__ = [
    "apply_boss",
    "apply_boss_purchase",
    "apply_boss_settlement",
    "apply_boss_player_schema",
    "apply_boss_full_refresh_player_schema",
    "apply_boss_battle_player_schema",
]
