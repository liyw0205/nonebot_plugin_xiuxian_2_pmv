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


def apply_dongfu_event_replay(uow: DatabaseUnitOfWork) -> None:
    visit_rows = uow.query_all('PRAGMA main.table_info("dongfu_visit_reward_operations")')
    columns = {str(row["name"]) for row in visit_rows}
    if not columns:
        raise RuntimeError("dongfu visit operation schema is missing")
    if (
        not {"operation_id", "payload"}.issubset(columns)
        or not any(
            str(row["name"]) == "operation_id" and int(row["pk"] or 0) == 1
            for row in visit_rows
        )
    ):
        raise RuntimeError("dongfu visit operation schema is unsupported")
    if "gain" not in columns:
        uow.execute(
            "ALTER TABLE dongfu_visit_reward_operations "
            "ADD COLUMN gain INTEGER NOT NULL DEFAULT 0"
        )
    uow.execute(
        "CREATE TABLE IF NOT EXISTS dongfu_infiltration_operations("
        "operation_id TEXT PRIMARY KEY,user_id TEXT NOT NULL,payload TEXT NOT NULL,"
        "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
    )
    plan_rows = uow.query_all('PRAGMA main.table_info("dongfu_infiltration_operations")')
    plan_columns = {str(row["name"]) for row in plan_rows}
    if (
        not {"operation_id", "user_id", "payload", "created_at"}.issubset(plan_columns)
        or not any(
            str(row["name"]) == "operation_id" and int(row["pk"] or 0) == 1
            for row in plan_rows
        )
    ):
        raise RuntimeError("dongfu infiltration operation schema is unsupported")


__all__ = [
    "apply_dongfu",
    "apply_dongfu_infiltrate_success",
    "apply_dongfu_infiltrate_failure",
    "apply_dongfu_operations",
    "apply_dongfu_event_replay",
]
