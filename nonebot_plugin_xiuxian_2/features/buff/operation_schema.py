"""洞天福地操作回执表 `blessed_spot_operations` 的权威 schema。

此前 `features/buff/rename_repository.py` 与 `features/buff/upgrade_repository.py`
在请求事务内各自 `CREATE TABLE IF NOT EXISTS blessed_spot_operations`，而启动迁移
从未创建过该表。`buff.014`（game DB）现在是唯一创建者，两个仓储只校验形状。

legacy `xiuxian/xiuxian_buff/transaction_service.py::BlessedSpotService._ensure_operation_table`
是显式兼容/回滚入口，仍可在自身事务内建表；定向测试钉住它是该 domain 内唯一
允许出现 DDL 的非迁移文件。
"""

from __future__ import annotations

from ...infrastructure.database import DatabaseUnitOfWork


SCHEMA_MISSING = "schema_missing"

BLESSED_SPOT_OPERATION_COLUMNS = frozenset(
    {"operation_id", "action", "payload", "result_json", "created_at"}
)

BLESSED_SPOT_OPERATION_DDL = (
    "CREATE TABLE IF NOT EXISTS blessed_spot_operations ("
    "operation_id TEXT PRIMARY KEY,"
    "action TEXT NOT NULL,"
    "payload TEXT NOT NULL,"
    "result_json TEXT NOT NULL,"
    "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)",
)

STONE_TRAINING_OPERATION_COLUMNS = frozenset(
    {"operation_id", "payload", "result_json", "created_at"}
)

STONE_TRAINING_OPERATION_DDL = (
    "CREATE TABLE IF NOT EXISTS stone_training_operations ("
    "operation_id TEXT PRIMARY KEY,"
    "payload TEXT NOT NULL,"
    "result_json TEXT NOT NULL,"
    "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)",
)


def _table_ready(
    uow: DatabaseUnitOfWork, table: str, required: frozenset[str]
) -> bool:
    rows = uow.query_all(f'PRAGMA table_info("{table}")')
    columns = {str(row["name"]) for row in rows}
    has_primary_key = any(
        str(row["name"]) == "operation_id" and int(row["pk"] or 0) == 1
        for row in rows
    )
    return required.issubset(columns) and has_primary_key


def operations_ready(uow: DatabaseUnitOfWork) -> bool:
    """Return whether the 洞天福地 receipt table can serve idempotent writes."""
    return _table_ready(uow, "blessed_spot_operations", BLESSED_SPOT_OPERATION_COLUMNS)


def stone_training_ready(uow: DatabaseUnitOfWork) -> bool:
    """Return whether the 灵石修炼 receipt table can serve idempotent writes."""
    return _table_ready(
        uow, "stone_training_operations", STONE_TRAINING_OPERATION_COLUMNS
    )


__all__ = [
    "BLESSED_SPOT_OPERATION_COLUMNS",
    "BLESSED_SPOT_OPERATION_DDL",
    "SCHEMA_MISSING",
    "STONE_TRAINING_OPERATION_COLUMNS",
    "STONE_TRAINING_OPERATION_DDL",
    "operations_ready",
    "stone_training_ready",
]
