"""地图（map）玩家状态表的 schema 归属。

`map_daily_limit`、`map_status`、`map_mission` 三张表此前在仓库里**没有任何启动迁移**
创建过（只有测试自建），运行期靠仓储的 `ALTER TABLE ... ADD COLUMN` 惰性补列；
`map_cooldown`、`map_explore_status` 由 `map.003`/`map.005` 建出但只含部分列。

`map.018` 现在建出这五张表的完整列集；请求路径只校验「表存在 + 本次要读的列齐全」，
缺表或缺列时返回 `schema_missing`，不再在业务方法里补列（对历史库保留一次显式修复入口，
见 `docs/features/map.md`）。
"""

from __future__ import annotations

from typing import Mapping

from ...infrastructure.database import DatabaseUnitOfWork


SCHEMA_MISSING = "schema_missing"

DAILY_LIMIT_COLUMNS = frozenset(
    {"user_id", "date", "gather_count", "combat_count", "explore_count", "resource_total_count"}
)
COOLDOWN_COLUMNS = frozenset(
    {"user_id", "gather_cd_until", "explore_start_cd_until", "combat_cd_until"}
)
STATUS_COLUMNS = frozenset({"user_id", "realm", "heaven", "node_id", "visited_nodes"})
EXPLORE_STATUS_COLUMNS = frozenset(
    {
        "user_id", "running", "node_type", "node_name", "start_time", "duration_min",
        "settlement", "max_duration_min", "interval_min",
    }
)
MISSION_COLUMNS = frozenset(
    {"user_id", "date", "mission_type", "target", "claimed", "settlement"}
)

# 启动迁移结束后的形状（player 库）。
PLAYER_TABLES: Mapping[str, frozenset[str]] = {
    "map_daily_limit": DAILY_LIMIT_COLUMNS,
    "map_cooldown": COOLDOWN_COLUMNS,
    "map_status": STATUS_COLUMNS,
    "map_explore_status": EXPLORE_STATUS_COLUMNS,
    "map_mission": MISSION_COLUMNS,
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
    "COOLDOWN_COLUMNS",
    "DAILY_LIMIT_COLUMNS",
    "EXPLORE_STATUS_COLUMNS",
    "MISSION_COLUMNS",
    "PLAYER_TABLES",
    "SCHEMA_MISSING",
    "STATUS_COLUMNS",
    "columns",
    "table_ready",
]
