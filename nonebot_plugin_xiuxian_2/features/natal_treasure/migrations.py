from ...infrastructure.database import DatabaseUnitOfWork

# 本命法宝 receipt ledgers.  Every one of them used to be created lazily inside
# the request transaction (both in the feature repositories and in the legacy
# ``transaction_service``), which conflicts with the repo-wide "request paths
# never execute DDL" contract.
#
# Placement follows the writers: 觉醒 writes its receipt straight into the
# player database, while 重塑/养成/升阶/铭刻/遗忘 run in a game-database
# transaction with the player database attached.
GAME_OPERATIONS_TABLE_DDL = (
    "CREATE TABLE IF NOT EXISTS natal_reawaken_operations ("
    "operation_id TEXT PRIMARY KEY,user_id TEXT NOT NULL,scripture_id INTEGER NOT NULL,"
    "scripture_cost INTEGER NOT NULL,max_slots INTEGER NOT NULL,choice_seed INTEGER NOT NULL,"
    "form INTEGER NOT NULL,name TEXT NOT NULL,effect_type INTEGER NOT NULL,"
    "base_value REAL NOT NULL,scripture_change INTEGER NOT NULL)",
    "CREATE TABLE IF NOT EXISTS natal_training_operations ("
    "operation_id TEXT PRIMARY KEY,user_id TEXT,requested_exp INTEGER,base_cost INTEGER,"
    "growth_rate REAL,max_level INTEGER,max_exp_base INTEGER,max_exp_growth INTEGER,"
    "exp_added INTEGER,stone_cost INTEGER,level INTEGER,exp INTEGER,max_exp INTEGER)",
    "CREATE TABLE IF NOT EXISTS natal_effect_upgrade_operations ("
    "operation_id TEXT PRIMARY KEY,user_id TEXT,scripture_id INTEGER,scripture_cost INTEGER,"
    "max_slots INTEGER,max_effect_level INTEGER,choice_seed INTEGER,slot INTEGER,"
    "effect_type INTEGER,level INTEGER)",
    "CREATE TABLE IF NOT EXISTS natal_engraving_operations ("
    "operation_id TEXT PRIMARY KEY,user_id TEXT,scripture_id INTEGER,scripture_cost INTEGER,"
    "max_slots INTEGER,choice_seed INTEGER,slot INTEGER,effect_type INTEGER,base_value REAL)",
    "CREATE TABLE IF NOT EXISTS natal_forget_operations ("
    "operation_id TEXT PRIMARY KEY,user_id TEXT,effect_type INTEGER,scripture_id INTEGER,"
    "scripture_cost INTEGER,max_slots INTEGER,slot INTEGER,effect_level INTEGER,"
    "scripture_change INTEGER)",
)

PLAYER_OPERATIONS_TABLE_DDL = (
    "CREATE TABLE IF NOT EXISTS natal_awaken_operations ("
    "operation_id TEXT PRIMARY KEY,user_id TEXT NOT NULL,max_slots INTEGER NOT NULL,"
    "choice_seed INTEGER NOT NULL,form INTEGER NOT NULL,name TEXT NOT NULL,"
    "effect_type INTEGER NOT NULL,base_value REAL NOT NULL)",
)

# The columns each receipt row is read back through; a partially built legacy
# table must be reported as schema_missing instead of silently losing fields.
GAME_OPERATIONS_TABLE_COLUMNS: dict[str, frozenset[str]] = {
    "natal_reawaken_operations": frozenset(
        {"operation_id", "user_id", "scripture_id", "scripture_cost", "max_slots", "choice_seed",
         "form", "name", "effect_type", "base_value", "scripture_change"}
    ),
    "natal_training_operations": frozenset(
        {"operation_id", "user_id", "requested_exp", "base_cost", "growth_rate", "max_level",
         "max_exp_base", "max_exp_growth", "exp_added", "stone_cost", "level", "exp", "max_exp"}
    ),
    "natal_effect_upgrade_operations": frozenset(
        {"operation_id", "user_id", "scripture_id", "scripture_cost", "max_slots",
         "max_effect_level", "choice_seed", "slot", "effect_type", "level"}
    ),
    "natal_engraving_operations": frozenset(
        {"operation_id", "user_id", "scripture_id", "scripture_cost", "max_slots",
         "choice_seed", "slot", "effect_type", "base_value"}
    ),
    "natal_forget_operations": frozenset(
        {"operation_id", "user_id", "effect_type", "scripture_id", "scripture_cost",
         "max_slots", "slot", "effect_level", "scripture_change"}
    ),
}

PLAYER_OPERATIONS_TABLE_COLUMNS: dict[str, frozenset[str]] = {
    "natal_awaken_operations": frozenset(
        {"operation_id", "user_id", "max_slots", "choice_seed", "form", "name", "effect_type", "base_value"}
    ),
}

OPERATIONS_TABLE_COLUMNS: dict[str, frozenset[str]] = {
    **GAME_OPERATIONS_TABLE_COLUMNS,
    **PLAYER_OPERATIONS_TABLE_COLUMNS,
}
OPERATIONS_TABLE_DDL = GAME_OPERATIONS_TABLE_DDL + PLAYER_OPERATIONS_TABLE_DDL


def apply_natal_treasure(uow: DatabaseUnitOfWork) -> None:
    uow.execute("CREATE TABLE IF NOT EXISTS natal_treasure_feature_migrations (version TEXT PRIMARY KEY)")
    uow.execute("INSERT OR IGNORE INTO natal_treasure_feature_migrations(version) VALUES ('natal_treasure.001')")


def apply_natal_treasure_operations(uow: DatabaseUnitOfWork) -> None:
    """Startup migration for the five game-side 本命法宝 receipt ledgers.

    The statements stay ``IF NOT EXISTS`` so a database that already took the
    lazy path keeps every existing receipt row; request paths only verify the
    schema and fail closed when it is missing.
    """
    uow.execute("CREATE TABLE IF NOT EXISTS natal_treasure_feature_migrations (version TEXT PRIMARY KEY)")
    for statement in GAME_OPERATIONS_TABLE_DDL:
        uow.execute(statement)
    uow.execute("INSERT OR IGNORE INTO natal_treasure_feature_migrations(version) VALUES ('natal_treasure.002')")


def apply_natal_treasure_player_operations(uow: DatabaseUnitOfWork) -> None:
    """Startup migration for the player-side 觉醒 receipt ledger."""
    uow.execute("CREATE TABLE IF NOT EXISTS natal_treasure_feature_migrations (version TEXT PRIMARY KEY)")
    for statement in PLAYER_OPERATIONS_TABLE_DDL:
        uow.execute(statement)
    uow.execute("INSERT OR IGNORE INTO natal_treasure_feature_migrations(version) VALUES ('natal_treasure.003')")


__all__ = [
    "GAME_OPERATIONS_TABLE_COLUMNS",
    "GAME_OPERATIONS_TABLE_DDL",
    "OPERATIONS_TABLE_COLUMNS",
    "OPERATIONS_TABLE_DDL",
    "PLAYER_OPERATIONS_TABLE_COLUMNS",
    "PLAYER_OPERATIONS_TABLE_DDL",
    "apply_natal_treasure",
    "apply_natal_treasure_operations",
    "apply_natal_treasure_player_operations",
]
