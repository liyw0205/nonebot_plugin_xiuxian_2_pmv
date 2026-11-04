from ...infrastructure.database import DatabaseUnitOfWork
from .statistics_columns import (
    CLOSING_SETTLEMENT_FIELDS,
    IMPART_STATISTICS_FIELDS,
    ensure_statistics_column,
)

# 虚神界 receipt/projection ledgers.  Every one of them used to be created lazily
# inside the request transaction (``battle_repository``, ``closing_enter_repository``,
# ``closing_settlement_repository``, ``explore_repository``, ``training_repository``,
# ``project_join_repository``), which conflicts with the repo-wide "request paths
# never execute DDL" contract.
#
# Placement follows the writers exactly: four receipt ledgers open the game
# database as their main connection, the battle batch ledger opens the impart
# database, and the projection tables the projection command owns live in the
# player database.
GAME_OPERATIONS_TABLE_DDL = (
    "CREATE TABLE IF NOT EXISTS impart_closing_enter_operations("
    "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,result_json TEXT NOT NULL,"
    "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)",
    "CREATE TABLE IF NOT EXISTS impart_closing_operations("
    "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,result_json TEXT NOT NULL,"
    "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)",
    "CREATE TABLE IF NOT EXISTS impart_explore_operations("
    "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,result_json TEXT NOT NULL,"
    "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)",
    "CREATE TABLE IF NOT EXISTS impart_training_operations("
    "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,result_json TEXT NOT NULL,"
    "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)",
)

IMPART_OPERATIONS_TABLE_DDL = (
    "CREATE TABLE IF NOT EXISTS impart_battle_batch_operations("
    "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,result_json TEXT NOT NULL,"
    "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)",
)

PLAYER_OPERATIONS_TABLE_DDL = (
    "CREATE TABLE IF NOT EXISTS impart_pk_state("
    "user_id TEXT PRIMARY KEY,pk_num INTEGER NOT NULL DEFAULT 7,"
    "win_num INTEGER NOT NULL DEFAULT 0)",
    "CREATE TABLE IF NOT EXISTS impart_project_members("
    "user_id TEXT PRIMARY KEY,joined_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)",
    "CREATE TABLE IF NOT EXISTS impart_project_meta("
    "meta_key TEXT PRIMARY KEY,meta_value TEXT NOT NULL)",
    "CREATE TABLE IF NOT EXISTS impart_project_join_operations("
    "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,result_json TEXT NOT NULL,"
    "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)",
)

# The columns each ledger row is read back through; a partially built legacy table
# must be reported as schema_missing instead of silently losing fields.
GAME_OPERATIONS_TABLE_COLUMNS: dict[str, frozenset[str]] = {
    name: frozenset({"operation_id", "payload", "result_json"})
    for name in (
        "impart_closing_enter_operations",
        "impart_closing_operations",
        "impart_explore_operations",
        "impart_training_operations",
    )
}

IMPART_OPERATIONS_TABLE_COLUMNS: dict[str, frozenset[str]] = {
    "impart_battle_batch_operations": frozenset({"operation_id", "payload", "result_json"}),
}

PLAYER_OPERATIONS_TABLE_COLUMNS: dict[str, frozenset[str]] = {
    "impart_pk_state": frozenset({"user_id", "pk_num", "win_num"}),
    "impart_project_members": frozenset({"user_id"}),
    "impart_project_meta": frozenset({"meta_key", "meta_value"}),
    "impart_project_join_operations": frozenset({"operation_id", "payload", "result_json"}),
}

OPERATIONS_TABLE_COLUMNS: dict[str, frozenset[str]] = {
    **GAME_OPERATIONS_TABLE_COLUMNS,
    **IMPART_OPERATIONS_TABLE_COLUMNS,
    **PLAYER_OPERATIONS_TABLE_COLUMNS,
}
OPERATIONS_TABLE_DDL = (
    GAME_OPERATIONS_TABLE_DDL + IMPART_OPERATIONS_TABLE_DDL + PLAYER_OPERATIONS_TABLE_DDL
)


def operations_ready(uow: DatabaseUnitOfWork, table: str, schema: str = "main") -> bool:
    """Request-path schema gate: the ledger exists with every column we read.

    A missing table or a half-built one both mean "startup migration has not run
    here"; the caller returns ``schema_missing`` and touches nothing.
    """
    expected = OPERATIONS_TABLE_COLUMNS[table]
    if uow.query_one(
        f"SELECT 1 AS present FROM {schema}.sqlite_master WHERE type='table' AND name=?",
        (table,),
    ) is None:
        return False
    columns = {
        str(row["name"]).casefold()
        for row in uow.query_all(f'PRAGMA {schema}.table_info("{table}")')
    }
    return expected.issubset(columns)


def _apply(uow: DatabaseUnitOfWork, statements: tuple[str, ...], version: str) -> None:
    uow.execute("CREATE TABLE IF NOT EXISTS impart_pk_feature_migrations (version TEXT PRIMARY KEY)")
    for statement in statements:
        uow.execute(statement)
    uow.execute(
        "INSERT OR IGNORE INTO impart_pk_feature_migrations(version) VALUES (?)", (version,)
    )


def apply_impart_pk(uow: DatabaseUnitOfWork) -> None:
    uow.execute("CREATE TABLE IF NOT EXISTS impart_pk_feature_migrations (version TEXT PRIMARY KEY)")
    uow.execute("INSERT OR IGNORE INTO impart_pk_feature_migrations(version) VALUES ('legacy.impart_pk.001')")


def apply_impart_pk_operations(uow: DatabaseUnitOfWork) -> None:
    """Startup migration for the four game-side 虚神界 receipt ledgers.

    The statements stay ``IF NOT EXISTS`` so a database that already took the lazy
    path keeps every existing receipt row; requests only verify the schema.
    """
    _apply(uow, GAME_OPERATIONS_TABLE_DDL, "impart_pk.002")


def apply_impart_pk_player_operations(uow: DatabaseUnitOfWork) -> None:
    """Startup migration for the four player-side 虚神界 projection tables."""
    _apply(uow, PLAYER_OPERATIONS_TABLE_DDL, "impart_pk.003")


def apply_impart_pk_impart_operations(uow: DatabaseUnitOfWork) -> None:
    """Startup migration for the impart-side 虚神界 battle batch ledger."""
    _apply(uow, IMPART_OPERATIONS_TABLE_DDL, "impart_pk.004")


def apply_impart_pk_closing_statistics(uow: DatabaseUnitOfWork) -> None:
    """Startup migration for the player-owned 虚神界出关 statistics columns.

    `closing_settlement_repository.settle` used to `ALTER TABLE ... ADD COLUMN`
    on every request.  `impart_pk.005` owns the columns now; the request path
    only verifies them and fails closed when they are absent.
    """
    uow.execute(
        "CREATE TABLE IF NOT EXISTS impart_pk_feature_migrations (version TEXT PRIMARY KEY)"
    )
    for field in CLOSING_SETTLEMENT_FIELDS:
        ensure_statistics_column(uow, field, schema="")
    uow.execute(
        "INSERT OR IGNORE INTO impart_pk_feature_migrations(version) VALUES ('impart_pk.005')"
    )


def apply_impart_pk_impart_statistics(uow: DatabaseUnitOfWork) -> None:
    """Startup migration for the remaining 虚神界 statistics columns."""
    uow.execute(
        "CREATE TABLE IF NOT EXISTS impart_pk_feature_migrations (version TEXT PRIMARY KEY)"
    )
    for field in IMPART_STATISTICS_FIELDS:
        ensure_statistics_column(uow, field, schema="")
    uow.execute(
        "INSERT OR IGNORE INTO impart_pk_feature_migrations(version) VALUES ('impart_pk.006')"
    )


__all__ = [
    "GAME_OPERATIONS_TABLE_COLUMNS",
    "GAME_OPERATIONS_TABLE_DDL",
    "IMPART_OPERATIONS_TABLE_COLUMNS",
    "IMPART_OPERATIONS_TABLE_DDL",
    "OPERATIONS_TABLE_COLUMNS",
    "OPERATIONS_TABLE_DDL",
    "PLAYER_OPERATIONS_TABLE_COLUMNS",
    "PLAYER_OPERATIONS_TABLE_DDL",
    "apply_impart_pk",
    "apply_impart_pk_closing_statistics",
    "apply_impart_pk_impart_operations",
    "apply_impart_pk_impart_statistics",
    "apply_impart_pk_operations",
    "apply_impart_pk_player_operations",
    "operations_ready",
]
