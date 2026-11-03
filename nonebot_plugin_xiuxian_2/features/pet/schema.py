"""宠物（pet）回执表的 schema 归属与 fail-closed 校验。

这六张回执表原先由 `features/pet/repository.py` 在各自的写事务里
`CREATE TABLE IF NOT EXISTS` 惰性创建；现在由启动迁移 `pet.005` 统一建立，
请求路径只校验「表存在 + 本次要读的列齐全」，缺表或缺列返回 `schema_missing`
且不改动宠物、背包与任何回执行。
"""

from __future__ import annotations

from typing import Mapping

from ...core.errors import DomainError
from ...infrastructure.database import DatabaseUnitOfWork


SCHEMA_MISSING = "schema_missing"

ACTIVE_SWITCH_COLUMNS = frozenset({"operation_id", "payload", "active_uid"})
FEED_COLUMNS = frozenset({"operation_id", "payload", "stars", "exp", "total_exp"})
TRAVEL_START_COLUMNS = frozenset({"operation_id", "payload"})
RELEASE_COLUMNS = frozenset({"operation_id", "payload", "refund", "released_uids"})
FUSION_COLUMNS = frozenset({"operation_id", "payload", "stars", "exp"})
REROLL_COLUMNS = frozenset({"operation_id", "payload", "skill_id"})

# 启动迁移结束后的形状，供迁移形状测试与文档使用。
# `active_switch`/`travel_start`/`fusion` 的仓储只接 player 库；
# `feed`/`release`/`skill_reroll` 的仓储以 game 为主库、player 作附库。
PLAYER_OPERATION_TABLES: Mapping[str, frozenset[str]] = {
    "pet_active_switch_operations": ACTIVE_SWITCH_COLUMNS,
    "pet_travel_start_operations": TRAVEL_START_COLUMNS,
    "pet_fusion_breakthrough_operations": FUSION_COLUMNS,
}
GAME_OPERATION_TABLES: Mapping[str, frozenset[str]] = {
    "pet_feed_operations": FEED_COLUMNS,
    "pet_release_operations": RELEASE_COLUMNS,
    "pet_skill_reroll_operations": REROLL_COLUMNS,
}


class PetSchemaMissing(DomainError):
    """The startup migrations have not built the pet receipt tables yet."""

    def __init__(self, message: str = "宠物数据尚未就绪，请检查启动迁移。", **details: object) -> None:
        super().__init__(SCHEMA_MISSING, message, dict(details))


def columns(uow: DatabaseUnitOfWork, table: str) -> frozenset[str]:
    """Return the live column set of ``table`` (empty when the table is absent)."""
    rows = uow.query_all(f'PRAGMA table_info("{table}")')
    return frozenset(str(row["name"]).casefold() for row in rows)


def operations_ready(
    uow: DatabaseUnitOfWork,
    table: str,
    required: frozenset[str] = frozenset(),
) -> bool:
    actual = columns(uow, table)
    return bool(actual) and all(name.casefold() in actual for name in required)


__all__ = [
    "ACTIVE_SWITCH_COLUMNS",
    "FEED_COLUMNS",
    "FUSION_COLUMNS",
    "GAME_OPERATION_TABLES",
    "OPERATION_TABLES",
    "PLAYER_OPERATION_TABLES",
    "PetSchemaMissing",
    "RELEASE_COLUMNS",
    "REROLL_COLUMNS",
    "SCHEMA_MISSING",
    "TRAVEL_START_COLUMNS",
    "columns",
    "operations_ready",
]
