"""灵石赠送两张表的权威 schema。

`stone_gift_operations` / `stone_gift_limits` 已分别由 `stone_gift.001` / `stone_gift.002`
创建；`StoneGiftRepository` 此前在 `limits()` / `transfer()` 里重复执行
`CREATE TABLE IF NOT EXISTS stone_gift_limits`。现在 DDL 只在启动迁移里执行，
请求路径只经 `table_ready()` 校验并降级/失败关闭。
"""

from __future__ import annotations

from ...infrastructure.database import DatabaseUnitOfWork


SCHEMA_MISSING = "schema_missing"

OPERATIONS_COLUMNS = frozenset(
    {
        "operation_id",
        "sender_id",
        "recipient_id",
        "gross_amount",
        "net_amount",
        "fee_amount",
        "created_at",
    }
)

LIMITS_COLUMNS = frozenset(
    {"limit_date", "user_id", "sent_amount", "received_amount", "updated_at"}
)

OPERATIONS_DDL = (
    "CREATE TABLE IF NOT EXISTS stone_gift_operations ("
    "operation_id TEXT PRIMARY KEY,"
    "sender_id TEXT NOT NULL,"
    "recipient_id TEXT NOT NULL,"
    "gross_amount INTEGER NOT NULL,"
    "net_amount INTEGER NOT NULL,"
    "fee_amount INTEGER NOT NULL,"
    "created_at TEXT DEFAULT CURRENT_TIMESTAMP)",
)

LIMITS_DDL = (
    "CREATE TABLE IF NOT EXISTS stone_gift_limits ("
    "limit_date TEXT NOT NULL,"
    "user_id TEXT NOT NULL,"
    "sent_amount INTEGER NOT NULL DEFAULT 0,"
    "received_amount INTEGER NOT NULL DEFAULT 0,"
    "updated_at TEXT DEFAULT CURRENT_TIMESTAMP,"
    "PRIMARY KEY(limit_date, user_id))",
)


def table_ready(
    uow: DatabaseUnitOfWork, table: str, required: frozenset[str]
) -> bool:
    """Return whether `table` exists with the columns this call reads or writes."""
    rows = uow.query_all(f'PRAGMA table_info("{table}")')
    columns = {str(row["name"]) for row in rows}
    return bool(columns) and required.issubset(columns)


__all__ = [
    "LIMITS_COLUMNS",
    "LIMITS_DDL",
    "OPERATIONS_COLUMNS",
    "OPERATIONS_DDL",
    "SCHEMA_MISSING",
    "table_ready",
]
