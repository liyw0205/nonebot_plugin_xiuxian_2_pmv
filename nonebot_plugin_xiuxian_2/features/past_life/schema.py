"""前世（past_life）schema 归属与 fail-closed 校验。

`past_life` 与五张回执表原本由四份仓储在请求事务内 `CREATE TABLE` / `ALTER TABLE`
惰性创建（`docs/features/past_life.md` 也写着「legacy tables remain the compatibility
data source」）。启动迁移 `past_life.002` 接管全部建表后，请求路径只校验、不建表。

`economy_log` 是共享账本，由 `tasks`/`dufang`/`auction`/`admin_asset`/`sign_in` 的启动迁移
各自 `IF NOT EXISTS` 建立，past_life 不重复声明所有权，只校验它存在。
"""

from __future__ import annotations

from typing import Mapping

from ...core.errors import DomainError
from ...infrastructure.database import DatabaseUnitOfWork


SCHEMA_MISSING = "schema_missing"

# 19 个业务字段在 start/choice/final/reset 四份仓储里逐字一致。
PAST_LIFE_INTEGER_FIELDS = frozenset(
    {"state", "stage", "revision", "total_score", "total_runs", "best_score", "achievement_points"}
)
PAST_LIFE_FIELDS = (
    "state", "stage", "revision", "alloc", "accumulated", "talent", "birth_scenario",
    "total_score", "score_breakdown", "event_indices", "event_snapshots", "early_death_rolls",
    "history", "last_run_time", "total_runs", "best_ending", "best_score", "endings_log",
    "achievement_points",
)
PAST_LIFE_COLUMNS = frozenset({"user_id", *PAST_LIFE_FIELDS})

STATISTICS_COLUMN = "前尘往事次数"
STATISTICS_COLUMNS = frozenset({"user_id", STATISTICS_COLUMN})

START_OPERATIONS_COLUMNS = frozenset({"operation_id", "user_id", "payload", "result_json"})
CHOICE_OPERATIONS_COLUMNS = frozenset(
    {"operation_id", "user_id", "kind", "payload", "response_json"}
)
FINAL_OPERATIONS_COLUMNS = frozenset({"operation_id", "user_id", "payload", "result_json"})
RESET_OPERATIONS_COLUMNS = frozenset(
    {
        "operation_id", "mode", "user_id", "clear_history", "payload", "status", "total",
        "processed", "applied", "conflicted", "missing", "last_error", "created_at", "updated_at",
    }
)
RESET_TARGETS_COLUMNS = frozenset(
    {
        "operation_id", "ordinal", "user_id", "expected_json", "final_json", "status",
        "error_text", "updated_at",
    }
)

# 启动迁移结束后的各库形状，供迁移形状测试与文档使用。
# 状态表在 player 库（仓储通过 `player_data` 附库读写），回执表在 game 库
# （仓储的写事务主库就是 game，回执语句不带 schema 前缀）。
PLAYER_TABLES: Mapping[str, frozenset[str]] = {
    "past_life": PAST_LIFE_COLUMNS,
    "statistics": STATISTICS_COLUMNS,
}
GAME_TABLES: Mapping[str, frozenset[str]] = {
    "past_life_start_operations": START_OPERATIONS_COLUMNS,
    "past_life_choice_operations": CHOICE_OPERATIONS_COLUMNS,
    "past_life_final_operations": FINAL_OPERATIONS_COLUMNS,
    "past_life_reset_operations": RESET_OPERATIONS_COLUMNS,
    "past_life_reset_targets": RESET_TARGETS_COLUMNS,
}

ECONOMY_LOG_COLUMNS = frozenset(
    {"id", "user_id", "source", "action", "stone_delta", "exp_delta", "item_delta", "detail", "created_at"}
)


class PastLifeSchemaMissing(DomainError):
    """The startup migrations have not built the past-life tables this operation needs."""

    def __init__(self, message: str = "前世数据尚未就绪，请检查启动迁移。", **details: object) -> None:
        super().__init__(SCHEMA_MISSING, message, dict(details))


def _prefix(schema: str | None) -> str:
    return f"{schema}." if schema else ""


def columns(uow: DatabaseUnitOfWork, table: str, *, schema: str | None = "player_data") -> frozenset[str]:
    """Return the live column set of ``table`` (empty when the table is absent)."""
    rows = uow.query_all(f'PRAGMA {_prefix(schema)}table_info("{table}")')
    return frozenset(str(row["name"]).casefold() for row in rows)


def table_ready(
    uow: DatabaseUnitOfWork,
    table: str,
    required: frozenset[str] = frozenset(),
    *,
    schema: str | None = "player_data",
) -> bool:
    actual = columns(uow, table, schema=schema)
    return bool(actual) and all(name.casefold() in actual for name in required)


def tables_ready(
    uow: DatabaseUnitOfWork,
    spec: Mapping[str, frozenset[str]],
    *,
    schema: str | None = "player_data",
) -> bool:
    return all(table_ready(uow, table, required, schema=schema) for table, required in spec.items())


__all__ = [
    "CHOICE_OPERATIONS_COLUMNS",
    "ECONOMY_LOG_COLUMNS",
    "FINAL_OPERATIONS_COLUMNS",
    "GAME_TABLES",
    "PAST_LIFE_COLUMNS",
    "PAST_LIFE_FIELDS",
    "PAST_LIFE_INTEGER_FIELDS",
    "PLAYER_TABLES",
    "RESET_OPERATIONS_COLUMNS",
    "RESET_TARGETS_COLUMNS",
    "SCHEMA_MISSING",
    "START_OPERATIONS_COLUMNS",
    "STATISTICS_COLUMN",
    "STATISTICS_COLUMNS",
    "PastLifeSchemaMissing",
    "columns",
    "table_ready",
    "tables_ready",
]
