from ...infrastructure.database import DatabaseUnitOfWork
from .schema import GAME_OPERATION_TABLES, PLAYER_OPERATION_TABLES


def apply_pet(uow: DatabaseUnitOfWork) -> None:
    uow.execute("CREATE TABLE IF NOT EXISTS pet_feature_migrations (version TEXT PRIMARY KEY)")
    uow.execute("INSERT OR IGNORE INTO pet_feature_migrations(version) VALUES ('pet.001')")


def apply_pet_hatch(uow: DatabaseUnitOfWork) -> None:
    uow.execute(
        "CREATE TABLE IF NOT EXISTS pet_hatch_operations("
        "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,result_json TEXT,"
        "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
    )
    columns = {str(row[1]) for row in uow.execute("PRAGMA table_info(pet_hatch_operations)").fetchall()}
    if "result_json" not in columns:
        uow.execute("ALTER TABLE pet_hatch_operations ADD COLUMN result_json TEXT")


def apply_pet_skill_replace(uow: DatabaseUnitOfWork) -> None:
    uow.execute(
        "CREATE TABLE IF NOT EXISTS pet_skill_replace_operations("
        "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,skill_id TEXT NOT NULL,"
        "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
    )


def apply_pet_travel_claim(uow: DatabaseUnitOfWork) -> None:
    uow.execute(
        "CREATE TABLE IF NOT EXISTS pet_travel_claim_operations("
        "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,"
        "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
    )


# The six receipt ledgers the default command paths write to.  They used to be
# created lazily inside `features/pet/repository.py`; the startup migration owns
# them now so request paths never execute DDL.
_PLAYER_OPERATION_DDL = (
    "CREATE TABLE IF NOT EXISTS pet_active_switch_operations("
    "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,active_uid TEXT NOT NULL,"
    "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)",
    "CREATE TABLE IF NOT EXISTS pet_travel_start_operations("
    "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,"
    "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)",
    "CREATE TABLE IF NOT EXISTS pet_fusion_breakthrough_operations("
    "operation_id TEXT PRIMARY KEY,payload TEXT,stars INTEGER,exp INTEGER,"
    "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)",
)

_GAME_OPERATION_DDL = (
    "CREATE TABLE IF NOT EXISTS pet_feed_operations("
    "operation_id TEXT PRIMARY KEY,payload TEXT,stars INTEGER,exp INTEGER,total_exp INTEGER)",
    "CREATE TABLE IF NOT EXISTS pet_release_operations("
    "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,refund INTEGER NOT NULL,"
    "released_uids TEXT NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)",
    "CREATE TABLE IF NOT EXISTS pet_skill_reroll_operations("
    "operation_id TEXT PRIMARY KEY,payload TEXT,skill_id TEXT,"
    "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)",
)


def apply_pet_player_operations(uow: DatabaseUnitOfWork) -> None:
    """Build the receipt ledgers whose repository opens the player database."""
    for statement in _PLAYER_OPERATION_DDL:
        uow.execute(statement)


def apply_pet_game_operations(uow: DatabaseUnitOfWork) -> None:
    """Build the receipt ledgers whose repository opens the game database."""
    for statement in _GAME_OPERATION_DDL:
        uow.execute(statement)


__all__ = [
    "GAME_OPERATION_TABLES",
    "PLAYER_OPERATION_TABLES",
    "apply_pet",
    "apply_pet_game_operations",
    "apply_pet_hatch",
    "apply_pet_player_operations",
    "apply_pet_skill_replace",
    "apply_pet_travel_claim",
]
