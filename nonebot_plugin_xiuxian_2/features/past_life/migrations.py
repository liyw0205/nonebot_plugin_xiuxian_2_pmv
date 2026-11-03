from ...infrastructure.database import DatabaseUnitOfWork
from ..impart_pk.statistics_columns import ensure_statistics_column
from .schema import PAST_LIFE_FIELDS, PAST_LIFE_INTEGER_FIELDS, STATISTICS_COLUMN


def apply_past_life(uow: DatabaseUnitOfWork) -> None:
    uow.execute("CREATE TABLE IF NOT EXISTS past_life_feature_migrations (version TEXT PRIMARY KEY)")
    uow.execute("INSERT OR IGNORE INTO past_life_feature_migrations(version) VALUES ('legacy.past_life.001')")


def _past_life_state_ddl() -> str:
    definitions = [
        f'"{field}" {"INTEGER" if field in PAST_LIFE_INTEGER_FIELDS else "TEXT"} DEFAULT NULL'
        for field in PAST_LIFE_FIELDS
    ]
    return 'CREATE TABLE IF NOT EXISTS past_life(user_id TEXT PRIMARY KEY,' + ",".join(definitions) + ")"


def apply_past_life_player(uow: DatabaseUnitOfWork) -> None:
    """Build the past-life state table in the player database.

    `statistics` is shared with base/training/auction migrations; only the
    `前尘往事次数` column is added here, through the shared column contract.
    """
    uow.execute(_past_life_state_ddl())
    ensure_statistics_column(uow, STATISTICS_COLUMN, schema="")


def apply_past_life_game(uow: DatabaseUnitOfWork) -> None:
    """Build the past-life receipt ledgers in the game database.

    The repositories open their write transaction on the game database and only
    attach the player database for state, so these tables have always lived here.
    """
    uow.execute(
        "CREATE TABLE IF NOT EXISTS past_life_start_operations("
        "operation_id TEXT PRIMARY KEY,user_id TEXT NOT NULL,payload TEXT NOT NULL,"
        "result_json TEXT NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
    )
    uow.execute(
        "CREATE TABLE IF NOT EXISTS past_life_choice_operations("
        "operation_id TEXT PRIMARY KEY,user_id TEXT NOT NULL,kind TEXT NOT NULL,"
        "payload TEXT NOT NULL,response_json TEXT NOT NULL,"
        "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
    )
    uow.execute(
        "CREATE TABLE IF NOT EXISTS past_life_final_operations("
        "operation_id TEXT PRIMARY KEY,user_id TEXT NOT NULL,payload TEXT NOT NULL,"
        "result_json TEXT NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
    )
    uow.execute(
        "CREATE TABLE IF NOT EXISTS past_life_reset_operations("
        "operation_id TEXT PRIMARY KEY,mode TEXT NOT NULL DEFAULT 'single',"
        "user_id TEXT NOT NULL DEFAULT '',clear_history INTEGER NOT NULL DEFAULT 0,"
        "payload TEXT NOT NULL DEFAULT '',status TEXT NOT NULL DEFAULT 'completed',"
        "total INTEGER NOT NULL DEFAULT 0,processed INTEGER NOT NULL DEFAULT 0,"
        "applied INTEGER NOT NULL DEFAULT 0,conflicted INTEGER NOT NULL DEFAULT 0,"
        "missing INTEGER NOT NULL DEFAULT 0,last_error TEXT NOT NULL DEFAULT '',"
        "created_at TEXT NOT NULL DEFAULT '',updated_at TEXT NOT NULL DEFAULT '')"
    )
    uow.execute(
        "CREATE TABLE IF NOT EXISTS past_life_reset_targets("
        "operation_id TEXT NOT NULL,ordinal INTEGER NOT NULL,user_id TEXT NOT NULL,"
        "expected_json TEXT NOT NULL,final_json TEXT NOT NULL,"
        "status TEXT NOT NULL DEFAULT 'pending',error_text TEXT NOT NULL DEFAULT '',"
        "updated_at TEXT NOT NULL,PRIMARY KEY(operation_id,user_id))"
    )


__all__ = ["apply_past_life", "apply_past_life_game", "apply_past_life_player"]
