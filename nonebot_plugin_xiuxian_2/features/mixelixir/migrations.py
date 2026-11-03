from ...infrastructure.database import DatabaseUnitOfWork


def apply_mixelixir(uow: DatabaseUnitOfWork) -> None:
    uow.execute("CREATE TABLE IF NOT EXISTS mixelixir_feature_migrations (version TEXT PRIMARY KEY)")
    uow.execute("INSERT OR IGNORE INTO mixelixir_feature_migrations(version) VALUES ('mixelixir.001')")


def apply_mixelixir_refine_claim(uow: DatabaseUnitOfWork) -> None:
    uow.execute(
        "CREATE TABLE IF NOT EXISTS mixelixir_refine_tasks ("
        "task_id TEXT PRIMARY KEY,user_id TEXT NOT NULL,recipe_set_id TEXT NOT NULL,"
        "recipe_key TEXT NOT NULL DEFAULT '',status TEXT NOT NULL,materials_json TEXT NOT NULL,"
        "reward_id INTEGER NOT NULL DEFAULT 0,reward_name TEXT NOT NULL DEFAULT '',"
        "reward_quantity INTEGER NOT NULL,expected_mix_state TEXT NOT NULL DEFAULT '{}',"
        "updated_mix_state TEXT NOT NULL DEFAULT '{}',created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,"
        "claimed_at TIMESTAMP)"
    )
    columns = {
        str(row["name"])
        for row in uow.query_all("PRAGMA table_info(mixelixir_refine_tasks)")
    }
    for name, definition in (
        ("recipe_key", "TEXT NOT NULL DEFAULT ''"),
        ("reward_id", "INTEGER NOT NULL DEFAULT 0"),
        ("reward_name", "TEXT NOT NULL DEFAULT ''"),
        ("expected_mix_state", "TEXT NOT NULL DEFAULT '{}'"),
        ("updated_mix_state", "TEXT NOT NULL DEFAULT '{}'"),
    ):
        if name not in columns:
            uow.execute(f'ALTER TABLE mixelixir_refine_tasks ADD COLUMN "{name}" {definition}')
    uow.execute(
        "CREATE TABLE IF NOT EXISTS mixelixir_refine_cost_operations ("
        "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,task_id TEXT NOT NULL,"
        "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
    )
    uow.execute(
        "CREATE TABLE IF NOT EXISTS mixelixir_refine_reward_operations ("
        "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,task_id TEXT NOT NULL,"
        "reward_id INTEGER NOT NULL,reward_name TEXT NOT NULL,reward_quantity INTEGER NOT NULL,"
        "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
    )


OPERATIONS_TABLE_DDL = (
    "CREATE TABLE IF NOT EXISTS mixelixir_harvest_operations ("
    "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,harvested_at TEXT NOT NULL,"
    "rewards_json TEXT NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)",
    "CREATE TABLE IF NOT EXISTS mixelixir_settlement_operations ("
    "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,reward_quantity INTEGER NOT NULL,"
    "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)",
    "CREATE TABLE IF NOT EXISTS mixelixir_harvest_level_upgrade_operations ("
    "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,cost INTEGER NOT NULL,"
    "wallet_stone INTEGER NOT NULL,level INTEGER NOT NULL,experience INTEGER NOT NULL,"
    "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)",
    "CREATE TABLE IF NOT EXISTS mixelixir_fire_control_upgrade_operations ("
    "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,cost INTEGER NOT NULL,"
    "wallet_stone INTEGER NOT NULL,level INTEGER NOT NULL,experience INTEGER NOT NULL,"
    "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)",
)

OPERATIONS_TABLE_COLUMNS: dict[str, frozenset[str]] = {
    "mixelixir_harvest_operations": frozenset(
        {"operation_id", "payload", "harvested_at", "rewards_json"}
    ),
    "mixelixir_settlement_operations": frozenset(
        {"operation_id", "payload", "reward_quantity"}
    ),
    "mixelixir_harvest_level_upgrade_operations": frozenset(
        {"operation_id", "payload", "cost", "wallet_stone", "level", "experience"}
    ),
    "mixelixir_fire_control_upgrade_operations": frozenset(
        {"operation_id", "payload", "cost", "wallet_stone", "level", "experience"}
    ),
}


def apply_mixelixir_operations(uow: DatabaseUnitOfWork) -> None:
    """Startup migration for the four 炼丹 operation ledgers.

    All four used to be created lazily inside the request transaction (both in
    the feature repositories and in the legacy ``transaction_service``), which
    conflicts with the repo-wide "request paths never execute DDL" contract.
    The statements stay ``IF NOT EXISTS`` so databases that already took the
    lazy path keep every existing receipt row.
    """
    uow.execute("CREATE TABLE IF NOT EXISTS mixelixir_feature_migrations (version TEXT PRIMARY KEY)")
    for statement in OPERATIONS_TABLE_DDL:
        uow.execute(statement)
    uow.execute("INSERT OR IGNORE INTO mixelixir_feature_migrations(version) VALUES ('mixelixir.004')")


def apply_mixelixir_refine_claim_player(uow: DatabaseUnitOfWork) -> None:
    uow.execute("CREATE TABLE IF NOT EXISTS statistics (user_id TEXT PRIMARY KEY)")
    columns = {str(row["name"]) for row in uow.query_all("PRAGMA table_info(statistics)")}
    if "炼丹次数" not in columns:
        uow.execute('ALTER TABLE statistics ADD COLUMN "炼丹次数" INTEGER DEFAULT NULL')


__all__ = [
    "OPERATIONS_TABLE_COLUMNS",
    "OPERATIONS_TABLE_DDL",
    "apply_mixelixir",
    "apply_mixelixir_operations",
    "apply_mixelixir_refine_claim",
    "apply_mixelixir_refine_claim_player",
]
