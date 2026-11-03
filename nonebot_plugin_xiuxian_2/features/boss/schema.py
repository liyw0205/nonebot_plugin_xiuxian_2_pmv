"""世界首领（world boss）惩罚与状态表的 schema 归属。

两张表原先由 `punishment_repository` 在**请求路径**上 `CREATE TABLE IF NOT EXISTS`
（`snapshot`、`get_result`、`punish` 共四处）。现在由启动迁移 `boss.007` 统一建立，
请求路径只校验「表存在 + 本次要读的列齐全」：

- 写路径 `punish` 缺表/缺列返回 `schema_missing`，不改变首领状态、不写回执；
- 读路径 `snapshot` 降级为空快照 `([], 0)`，`get_result` 返回 `None`。

两张表都在 game 库（仓储只持有一个 `database` 连接）。
"""

from __future__ import annotations

from typing import Mapping

from ...infrastructure.database import DatabaseUnitOfWork


SCHEMA_MISSING = "schema_missing"

STATE_COLUMNS = frozenset({"state_key", "bosses", "updated_at", "revision"})
PUNISHMENT_OPERATION_COLUMNS = frozenset({"operation_id", "payload", "result_json", "created_at"})

# 启动迁移结束后的形状（game 库），供迁移形状测试与文档使用。
TABLE_COLUMNS: Mapping[str, frozenset[str]] = {
    "world_boss_state": STATE_COLUMNS,
    "world_boss_punishment_operations": PUNISHMENT_OPERATION_COLUMNS,
}


def columns(uow: DatabaseUnitOfWork, table: str) -> frozenset[str]:
    """Return the live column set of ``table`` (empty when the table is absent)."""
    rows = uow.query_all(f'PRAGMA table_info("{table}")')
    return frozenset(str(row["name"]).casefold() for row in rows)


def table_ready(
    uow: DatabaseUnitOfWork,
    table: str,
    required: frozenset[str] = frozenset(),
) -> bool:
    actual = columns(uow, table)
    return bool(actual) and all(name.casefold() in actual for name in required)


__all__ = [
    "PUNISHMENT_OPERATION_COLUMNS",
    "SCHEMA_MISSING",
    "STATE_COLUMNS",
    "TABLE_COLUMNS",
    "columns",
    "table_ready",
]
