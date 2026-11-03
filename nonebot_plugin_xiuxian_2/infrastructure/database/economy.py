"""共享经济账本 `economy_log` 的权威 schema。

`economy_log` 是全仓共享的审计账本：`tasks`/`dufang`/`auction`/`admin_asset`/`sign_in`
的迁移各自建过一份精简表，而真正被 `log_economy_change()` 写入的列集
（`sect_id`、`sect_contribution_delta`、`sect_scale_delta`、`sect_materials_delta`、
`event_id`）与五个索引原先只在请求期 `ensure_economy_log_table()` 里补齐。

启动迁移 `economy.001` 现在建这份权威形状（列 + 索引）；请求期只校验，
schema 齐全时直接返回，不再执行任何 DDL。
"""

from __future__ import annotations

from . import DatabaseUnitOfWork


ECONOMY_LOG_COLUMNS = frozenset(
    {
        "id", "user_id", "sect_id", "source", "action", "stone_delta", "exp_delta",
        "sect_contribution_delta", "sect_scale_delta", "sect_materials_delta",
        "item_delta", "detail", "trace_id", "event_id", "created_at",
    }
)

ECONOMY_LOG_INDEXES = (
    "idx_economy_log_user_time",
    "idx_economy_log_sect_time",
    "idx_economy_log_source_action",
    "idx_economy_log_trace_id",
    "idx_economy_log_event_id",
)

ECONOMY_LOG_DDL = (
    "CREATE TABLE IF NOT EXISTS economy_log ("
    "id INTEGER PRIMARY KEY AUTOINCREMENT,"
    "user_id TEXT,"
    "sect_id INTEGER,"
    "source TEXT NOT NULL,"
    "action TEXT NOT NULL,"
    "stone_delta INTEGER NOT NULL DEFAULT 0,"
    "exp_delta INTEGER NOT NULL DEFAULT 0,"
    "sect_contribution_delta INTEGER NOT NULL DEFAULT 0,"
    "sect_scale_delta INTEGER NOT NULL DEFAULT 0,"
    "sect_materials_delta INTEGER NOT NULL DEFAULT 0,"
    "item_delta TEXT NOT NULL DEFAULT '[]',"
    "detail TEXT NOT NULL DEFAULT '{}',"
    "trace_id TEXT,"
    "event_id TEXT,"
    "created_at TEXT NOT NULL)",
    "CREATE INDEX IF NOT EXISTS idx_economy_log_user_time ON economy_log(user_id, created_at)",
    "CREATE INDEX IF NOT EXISTS idx_economy_log_sect_time ON economy_log(sect_id, created_at)",
    "CREATE INDEX IF NOT EXISTS idx_economy_log_source_action ON economy_log(source, action)",
    "CREATE INDEX IF NOT EXISTS idx_economy_log_trace_id ON economy_log(trace_id)",
    "CREATE UNIQUE INDEX IF NOT EXISTS idx_economy_log_event_id "
    "ON economy_log(event_id) WHERE event_id IS NOT NULL",
)


def apply_economy_log(uow: DatabaseUnitOfWork) -> None:
    """Create the canonical shared ledger shape (idempotent)."""
    for statement in ECONOMY_LOG_DDL:
        uow.execute(statement)


def columns(uow: DatabaseUnitOfWork) -> frozenset[str]:
    rows = uow.query_all('PRAGMA table_info("economy_log")')
    return frozenset(str(row["name"]).casefold() for row in rows)


def indexes(uow: DatabaseUnitOfWork) -> frozenset[str]:
    rows = uow.query_all(
        "SELECT name FROM sqlite_master WHERE type='index' AND tbl_name='economy_log'"
    )
    return frozenset(str(row["name"]) for row in rows)


def schema_ready(uow: DatabaseUnitOfWork) -> bool:
    actual = columns(uow)
    return bool(actual) and ECONOMY_LOG_COLUMNS.issubset(actual) and set(ECONOMY_LOG_INDEXES).issubset(
        indexes(uow)
    )


__all__ = [
    "ECONOMY_LOG_COLUMNS",
    "ECONOMY_LOG_DDL",
    "ECONOMY_LOG_INDEXES",
    "apply_economy_log",
    "columns",
    "indexes",
    "schema_ready",
]
