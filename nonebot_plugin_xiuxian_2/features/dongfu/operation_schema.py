from __future__ import annotations

from pathlib import Path

from ...infrastructure.database import DatabaseUnitOfWork


OPERATION_TABLE_DDL = (
    "CREATE TABLE IF NOT EXISTS dongfu_accelerate_operations ("
    "operation_id TEXT PRIMARY KEY, payload TEXT NOT NULL, "
    "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)",
    "CREATE TABLE IF NOT EXISTS dongfu_array_upgrade_operations ("
    "operation_id TEXT PRIMARY KEY, payload TEXT NOT NULL, level INTEGER NOT NULL, "
    "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)",
    "CREATE TABLE IF NOT EXISTS dongfu_expansion_operations ("
    "operation_id TEXT PRIMARY KEY, user_id TEXT NOT NULL, previous_count INTEGER NOT NULL, "
    "current_count INTEGER NOT NULL, deed_cost INTEGER NOT NULL, stone_cost INTEGER NOT NULL, "
    "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)",
    "CREATE TABLE IF NOT EXISTS dongfu_fertilize_operations ("
    "operation_id TEXT PRIMARY KEY, payload TEXT NOT NULL, "
    "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)",
    "CREATE TABLE IF NOT EXISTS dongfu_harvest_operations ("
    "operation_id TEXT PRIMARY KEY, payload TEXT NOT NULL, rewards TEXT NOT NULL, "
    "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)",
    "CREATE TABLE IF NOT EXISTS dongfu_patrol_operations ("
    "operation_id TEXT PRIMARY KEY, payload TEXT NOT NULL, patrol_count INTEGER NOT NULL, "
    "patrol_guard INTEGER NOT NULL, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)",
    "CREATE TABLE IF NOT EXISTS dongfu_plant_operations ("
    "operation_id TEXT PRIMARY KEY, payload TEXT NOT NULL, "
    "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)",
    "CREATE TABLE IF NOT EXISTS dongfu_visit_reward_operations ("
    "operation_id TEXT PRIMARY KEY, payload TEXT NOT NULL, gain INTEGER NOT NULL DEFAULT 0, "
    "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)",
)

OPERATION_TABLE_COLUMNS = {
    "dongfu_accelerate_operations": {"operation_id", "payload", "created_at"},
    "dongfu_array_upgrade_operations": {"operation_id", "payload", "level", "created_at"},
    "dongfu_expansion_operations": {
        "operation_id", "user_id", "previous_count", "current_count", "deed_cost", "stone_cost", "created_at"
    },
    "dongfu_fertilize_operations": {"operation_id", "payload", "created_at"},
    "dongfu_harvest_operations": {"operation_id", "payload", "rewards", "created_at"},
    "dongfu_patrol_operations": {
        "operation_id", "payload", "patrol_count", "patrol_guard", "created_at"
    },
    "dongfu_plant_operations": {"operation_id", "payload", "created_at"},
    "dongfu_visit_reward_operations": {"operation_id", "payload", "gain", "created_at"},
    "dongfu_infiltration_operations": {
        "operation_id", "user_id", "payload", "created_at"
    },
}


def operation_databases_ready(game_database: str | Path, player_database: str | Path) -> bool:
    return Path(game_database).is_file() and Path(player_database).is_file()


def operation_schema_ready(uow: DatabaseUnitOfWork, table: str) -> bool:
    expected = OPERATION_TABLE_COLUMNS[table]
    rows = uow.query_all(f'PRAGMA main.table_info("{table}")')
    columns = {str(row["name"]) for row in rows}
    has_primary_key = any(
        str(row["name"]) == "operation_id" and int(row["pk"] or 0) == 1
        for row in rows
    )
    return expected.issubset(columns) and has_primary_key


__all__ = [
    "OPERATION_TABLE_COLUMNS",
    "OPERATION_TABLE_DDL",
    "operation_databases_ready",
    "operation_schema_ready",
]
