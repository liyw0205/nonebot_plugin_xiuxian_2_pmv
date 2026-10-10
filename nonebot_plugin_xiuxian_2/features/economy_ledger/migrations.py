from __future__ import annotations

from ...infrastructure.database import DatabaseUnitOfWork


# The composition root owns execution; this tuple records the feature's
# startup migration without creating a second migration runner.
MIGRATIONS = ("economy_ledger.001",)


def apply_economy_ledger_read_indexes(uow: DatabaseUnitOfWork) -> None:
    table = uow.query_one(
        "SELECT 1 AS present FROM sqlite_master WHERE type='table' AND name='economy_log'"
    )
    if table is None:
        return

    columns = {
        str(row["name"]).casefold()
        for row in uow.query_all('PRAGMA table_info("economy_log")')
    }
    indexes = (
        (
            {"created_at"},
            "CREATE INDEX IF NOT EXISTS idx_economy_log_created_at "
            "ON economy_log(created_at)",
        ),
        (
            {"source", "action", "created_at"},
            "CREATE INDEX IF NOT EXISTS idx_economy_log_source_action_time "
            "ON economy_log(source, action, created_at)",
        ),
        (
            {"user_id", "source", "created_at"},
            "CREATE INDEX IF NOT EXISTS idx_economy_log_user_source_time "
            "ON economy_log(user_id, source, created_at)",
        ),
        (
            {"trace_id"},
            "CREATE INDEX IF NOT EXISTS idx_economy_log_trace_id "
            "ON economy_log(trace_id)",
        ),
        (
            {"stone_delta"},
            "CREATE INDEX IF NOT EXISTS idx_economy_log_stone_delta "
            "ON economy_log(stone_delta)",
        ),
    )
    for required_columns, statement in indexes:
        if required_columns.issubset(columns):
            uow.execute(statement)


__all__ = ["MIGRATIONS", "apply_economy_ledger_read_indexes"]
