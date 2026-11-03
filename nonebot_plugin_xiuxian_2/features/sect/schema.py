"""宗门（sect）操作回执表的 schema 归属与 fail-closed 校验。

这 12 张回执表原先由 11 份仓储在各自的业务方法写事务里
`CREATE TABLE IF NOT EXISTS` 惰性创建；现在由启动迁移 `sect.019` 统一建立，
请求路径只校验「表存在 + 本次要读的列齐全」，缺表或缺列返回 `schema_missing`
且不改动宗门资产、成员、贡献与任何回执行。

所有表都在 game 库：这些仓储只持有一个 `database` 连接，不使用附库。
"""

from __future__ import annotations

from typing import Mapping

from ...infrastructure.database import DatabaseUnitOfWork


SCHEMA_MISSING = "schema_missing"

BUFF_SEARCH_COLUMNS = frozenset(
    {
        "operation_id", "actor_id", "sect_id", "buff_type", "previous_value",
        "new_value", "stone_cost", "materials_cost",
    }
)
CLOSE_MOUNTAIN_COLUMNS = frozenset({"operation_id", "actor_id", "sect_id"})
CREATION_COLUMNS = frozenset({"operation_id", "user_id", "sect_id", "sect_name", "stone_cost"})
DAILY_RESET_COLUMNS = frozenset({"business_date", "payload", "user_count", "outcomes"})
INACTIVE_DISBAND_COLUMNS = frozenset(
    {"operation_id", "payload", "sect_id", "sect_name", "reason", "member_count"}
)
ELIXIR_ROOM_COLUMNS = frozenset(
    {"operation_id", "actor_id", "sect_id", "from_level", "to_level", "stone_cost", "scale_cost"}
)
JOIN_STATE_COLUMNS = frozenset({"operation_id", "actor_id", "sect_id"})
NAME_REFRESH_COLUMNS = frozenset({"operation_id", "user_id", "stone_cost"})
OWNER_INHERIT_COLUMNS = frozenset({"operation_id", "actor_id", "sect_id"})
OWNER_TRANSFER_COLUMNS = frozenset(
    {"operation_id", "operation_type", "actor_id", "target_id", "sect_id"}
)
PRACTICE_COLUMNS = frozenset(
    {
        "operation_id", "user_id", "sect_id", "practice_type", "from_level", "to_level",
        "stone_cost", "materials_cost",
    }
)

# 启动迁移结束后的形状（game 库），供迁移形状测试与文档使用。
OPERATION_TABLES: Mapping[str, frozenset[str]] = {
    "sect_buff_search_operations": BUFF_SEARCH_COLUMNS,
    "sect_close_mountain_operations": CLOSE_MOUNTAIN_COLUMNS,
    "sect_creation_operations": CREATION_COLUMNS,
    "sect_daily_reset_operations": DAILY_RESET_COLUMNS,
    "sect_inactive_disband_operations": INACTIVE_DISBAND_COLUMNS,
    "sect_elixir_room_operations": ELIXIR_ROOM_COLUMNS,
    "sect_open_join_operations": JOIN_STATE_COLUMNS,
    "sect_close_join_operations": JOIN_STATE_COLUMNS,
    "sect_name_refresh_operations": NAME_REFRESH_COLUMNS,
    "sect_owner_inherit_operations": OWNER_INHERIT_COLUMNS,
    "sect_operations": OWNER_TRANSFER_COLUMNS,
    "sect_practice_operations": PRACTICE_COLUMNS,
}

JOIN_STATE_TABLES = ("sect_open_join_operations", "sect_close_join_operations")


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
    "BUFF_SEARCH_COLUMNS",
    "CLOSE_MOUNTAIN_COLUMNS",
    "CREATION_COLUMNS",
    "DAILY_RESET_COLUMNS",
    "ELIXIR_ROOM_COLUMNS",
    "INACTIVE_DISBAND_COLUMNS",
    "JOIN_STATE_COLUMNS",
    "JOIN_STATE_TABLES",
    "NAME_REFRESH_COLUMNS",
    "OPERATION_TABLES",
    "OWNER_INHERIT_COLUMNS",
    "OWNER_TRANSFER_COLUMNS",
    "PRACTICE_COLUMNS",
    "SCHEMA_MISSING",
    "columns",
    "table_ready",
]
