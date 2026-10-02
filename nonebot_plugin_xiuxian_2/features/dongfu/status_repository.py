from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from ...infrastructure.database import DatabaseUnitOfWork


_STATUS_FIELDS = (
    "built",
    "realm",
    "heaven",
    "node_id",
    "node_name",
    "node_type",
    "array_level",
    "planting",
    "plant_seed_id",
    "plant_start",
    "harvest_settlement",
    "plant_finish",
    "plant_slots",
    "plot_count",
    "intrude_date",
    "intrude_count",
    "infiltrate_date",
    "infiltrate_active_count",
    "infiltrate_random_count",
    "patrol_date",
    "patrol_count",
    "patrol_guard",
)


class DongfuStatusSqlQueryRepository:
    """Read the player-owned dongfu projection without schema repair."""

    def __init__(self, player_database: str | Path) -> None:
        self.player_database = Path(player_database)

    def get(self, user_id: str) -> dict[str, Any] | None:
        user_id = str(user_id or "").strip()
        if not user_id or not self.player_database.is_file():
            return None
        try:
            with DatabaseUnitOfWork(self.player_database, read_only=True) as uow:
                table = uow.query_one(
                    "SELECT 1 AS present FROM sqlite_master "
                    "WHERE type='table' AND name='dongfu_status'"
                )
                if table is None:
                    return None
                columns = {
                    str(row["name"])
                    for row in uow.query_all("PRAGMA table_info(dongfu_status)")
                }
                if "user_id" not in columns or "built" not in columns:
                    return None
                selected = [field for field in _STATUS_FIELDS if field in columns]
                row = uow.query_one(
                    "SELECT " + ",".join(selected) + " FROM dongfu_status WHERE user_id=?",
                    (user_id,),
                )
        except OSError:
            return None
        except Exception:
            return None
        if row is None:
            return None
        result = dict(row)
        if isinstance(result.get("plant_slots"), str):
            try:
                result["plant_slots"] = json.loads(result["plant_slots"])
            except json.JSONDecodeError:
                pass
        return result


__all__ = ["DongfuStatusSqlQueryRepository"]
