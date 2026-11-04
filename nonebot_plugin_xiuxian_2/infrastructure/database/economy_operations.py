"""转账幂等回执表 `economy_operations` 的权威 schema。

`EconomyRepository.transfer_stones` 原先在请求事务内创建该表。`economy_ledger.003`
现在是唯一创建者；请求路径只校验形状，缺表或缺列时返回 `False` 且零写入。

本模块刻意只依赖标准库：`xiuxian_utils/repositories.py` 会被测试以文件路径独立加载，
不能依赖包级相对导入，只能通过 importlib fallback 读取这里的 DDL 与校验函数。
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any


SCHEMA_MISSING = "schema_missing"

ECONOMY_OPERATIONS_COLUMNS = frozenset(
    {"operation_id", "operation_type", "created_at"}
)

ECONOMY_OPERATIONS_DDL = (
    "CREATE TABLE IF NOT EXISTS economy_operations ("
    "operation_id TEXT PRIMARY KEY,"
    "operation_type TEXT NOT NULL,"
    "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)",
)


def _cell(row: Any, key: str, index: int):
    if isinstance(row, Mapping):
        return row.get(key)
    try:
        return row[index]
    except (TypeError, IndexError):
        return None


def operations_ready(connection: Any) -> bool:
    """Return whether the receipt table exists with its primary key intact."""
    cursor = connection.cursor()
    cursor.execute('PRAGMA table_info("economy_operations")')
    rows = cursor.fetchall()
    columns = {str(_cell(row, "name", 1) or "").casefold() for row in rows}
    has_primary_key = any(
        str(_cell(row, "name", 1) or "").casefold() == "operation_id"
        and int(_cell(row, "pk", 5) or 0) == 1
        for row in rows
    )
    return ECONOMY_OPERATIONS_COLUMNS.issubset(columns) and has_primary_key


__all__ = [
    "ECONOMY_OPERATIONS_COLUMNS",
    "ECONOMY_OPERATIONS_DDL",
    "SCHEMA_MISSING",
    "operations_ready",
]
