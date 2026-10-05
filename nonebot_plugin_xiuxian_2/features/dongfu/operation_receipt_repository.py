from __future__ import annotations

import json
from pathlib import Path

from ...infrastructure.database import DatabaseUnitOfWork


_ACTION_RECEIPTS = {
    "accelerate": ("dongfu_accelerate_operations", ("payload",)),
    "array": ("dongfu_array_upgrade_operations", ("payload", "level")),
    "expand": (
        "dongfu_expansion_operations",
        ("previous_count", "current_count", "deed_cost", "stone_cost"),
    ),
    "fertilize": ("dongfu_fertilize_operations", ("payload",)),
    "harvest": ("dongfu_harvest_operations", ("payload", "rewards")),
    "patrol": ("dongfu_patrol_operations", ("payload", "patrol_count", "patrol_guard")),
    "plant": ("dongfu_plant_operations", ("payload",)),
    "visit": ("dongfu_visit_reward_operations", ("payload", "gain")),
    "infiltrate_success": (
        "dongfu_infiltrate_success_operations",
        ("payload", "infiltrate_left", "intrude_left"),
    ),
    "infiltrate_failure": (
        "dongfu_infiltrate_failure_operations",
        ("payload", "infiltrate_left", "intrude_left"),
    ),
}


class DongfuOperationReceiptSqlQueryRepository:
    """Read existing action receipts before handler prechecks can hide a replay."""

    def __init__(self, database: str | Path) -> None:
        self.database = Path(database)

    def get(self, action: str, operation_id: str) -> dict | None:
        table, result_columns = _ACTION_RECEIPTS[str(action)]
        operation_id = str(operation_id).strip()
        if not operation_id or not self.database.is_file():
            return None
        with DatabaseUnitOfWork(self.database, read_only=True) as uow:
            table_row = uow.query_one(
                "SELECT 1 AS present FROM sqlite_master WHERE type='table' AND name=?",
                (table,),
            )
            if table_row is None:
                return None
            columns = {
                str(row["name"])
                for row in uow.query_all(f'PRAGMA main.table_info("{table}")')
            }
            expected = {"operation_id", *result_columns}
            if not expected.issubset(columns):
                return None
            selected = ",".join(result_columns)
            row = uow.query_one(
                f'SELECT {selected} FROM "{table}" WHERE operation_id=?',
                (operation_id,),
            )
        if row is None:
            return None
        result = dict(row)
        if action == "visit":
            payload = str(result.get("payload") or "")
            parts = payload.split("|")
            if len(parts) == 3:
                try:
                    result["gain"] = int(parts[2])
                    result["payload"] = json.dumps(
                        parts[:2], ensure_ascii=False, separators=(",", ":")
                    )
                except ValueError:
                    return None
        return result


__all__ = ["DongfuOperationReceiptSqlQueryRepository"]
