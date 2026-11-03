"""竞技场（arena）schema 归属与 fail-closed 校验。

启动迁移是竞技场所有表的唯一写入者：

- `arena.002` `arena_challenge_purchase_operations`
- `arena.003` + `arena.010` `arena_purchase_operations`（`arena.010` 补 `status`/`result_json`）
- `arena.004` `arena_challenge_ticket_operations`
- `arena.005` `arena_challenge_settlement_operations`
- `arena.006` 两张周降段回执表
- `arena.007` `arena_season_reward_operations`
- `arena.008` / `arena.009` `arena` 与 `arena_state_operations`（`arena.009` 补齐列）

请求路径（命令、调度、Web、客户端回执读取）只校验，不建表、不补列；缺表或缺本次要读的
任一列时 fail-closed 并返回/抛出 `schema_missing`，不产生任何资产写入。
"""

from __future__ import annotations

from typing import Mapping

from ...core.errors import DomainError
from ...infrastructure.database import DatabaseUnitOfWork


SCHEMA_MISSING = "schema_missing"

# 启动迁移结束后的完整形状，供迁移形状测试与文档使用；请求路径按自身读取列校验。
GAME_TABLES: Mapping[str, frozenset[str]] = {
    "arena_purchase_operations": frozenset(
        {
            "operation_id", "payload", "quantity", "cost", "honor_points", "purchased",
            "inventory", "created_at", "status", "result_json",
        }
    ),
    "arena_challenge_purchase_operations": frozenset(
        {"operation_id", "payload", "amount", "cost", "stone", "bought", "extra", "created_at"}
    ),
    "arena_challenge_ticket_operations": frozenset(
        {
            "operation_id", "payload", "used_tickets", "item_remaining", "challenges_used",
            "challenges_remaining", "challenge_cap", "created_at",
        }
    ),
    "arena_challenge_settlement_operations": frozenset(
        {"operation_id", "challenger_id", "payload", "result_json", "created_at"}
    ),
    "arena_season_reward_operations": frozenset(
        {
            "operation_id", "payload", "season_key", "user_id", "honor", "honor_points",
            "total_honor_earned", "created_at",
        }
    ),
}

PLAYER_TABLES: Mapping[str, frozenset[str]] = {
    "arena": frozenset(
        {
            "user_id", "score", "total_wins", "total_losses", "daily_challenges_used",
            "daily_extra_challenges", "daily_challenge_buys", "last_reset_date", "last_buy_date",
            "last_challenge_time", "win_streak", "max_win_streak", "rank", "honor_points",
            "total_honor_earned", "weekly_purchases",
        }
    ),
    "arena_state_operations": frozenset(
        {"operation_id", "user_id", "kind", "period_key", "snapshot", "created_at"}
    ),
}


class ArenaSchemaMissing(DomainError):
    """The startup migrations have not built the arena tables this operation needs."""

    def __init__(self, message: str = "竞技场数据尚未就绪，请检查启动迁移。", **details: object) -> None:
        super().__init__(SCHEMA_MISSING, message, dict(details))


def columns(uow: DatabaseUnitOfWork, table: str, *, prefix: str = "") -> frozenset[str]:
    """Return the live column set of ``table`` (empty when the table is absent)."""
    rows = uow.query_all(f'PRAGMA {prefix}table_info("{table}")')
    return frozenset(str(row["name"]).casefold() for row in rows)


def table_ready(
    uow: DatabaseUnitOfWork,
    table: str,
    required: frozenset[str] = frozenset(),
    *,
    prefix: str = "",
) -> bool:
    actual = columns(uow, table, prefix=prefix)
    return bool(actual) and required.issubset(actual)


def tables_ready(
    uow: DatabaseUnitOfWork,
    spec: Mapping[str, frozenset[str]],
    *,
    prefix: str = "",
) -> bool:
    return all(table_ready(uow, table, required, prefix=prefix) for table, required in spec.items())


__all__ = [
    "ArenaSchemaMissing",
    "GAME_TABLES",
    "PLAYER_TABLES",
    "SCHEMA_MISSING",
    "columns",
    "table_ready",
    "tables_ready",
]
