from ...infrastructure.database import DatabaseUnitOfWork
from .operation_schema import OPERATION_TABLE_DDL


def apply_dongfu(uow: DatabaseUnitOfWork) -> None:
    uow.execute("CREATE TABLE IF NOT EXISTS dongfu_feature_migrations (version TEXT PRIMARY KEY)")
    uow.execute("INSERT OR IGNORE INTO dongfu_feature_migrations(version) VALUES ('legacy.dongfu.001')")


def apply_dongfu_infiltrate_success(uow: DatabaseUnitOfWork) -> None:
    uow.execute(
        "CREATE TABLE IF NOT EXISTS dongfu_infiltrate_success_operations ("
        "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,infiltrate_left INTEGER NOT NULL,"
        "intrude_left INTEGER NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
    )


def apply_dongfu_infiltrate_failure(uow: DatabaseUnitOfWork) -> None:
    uow.execute(
        "CREATE TABLE IF NOT EXISTS dongfu_infiltrate_failure_operations ("
        "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,infiltrate_left INTEGER NOT NULL,"
        "intrude_left INTEGER NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
    )


def apply_dongfu_operations(uow: DatabaseUnitOfWork) -> None:
    for statement in OPERATION_TABLE_DDL:
        uow.execute(statement)


__all__ = [
    "apply_dongfu",
    "apply_dongfu_infiltrate_success",
    "apply_dongfu_infiltrate_failure",
    "apply_dongfu_operations",
]
