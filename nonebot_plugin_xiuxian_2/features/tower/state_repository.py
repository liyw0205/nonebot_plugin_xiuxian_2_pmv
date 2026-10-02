from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

from ...infrastructure.database import DatabaseUnitOfWork


TOWER_FIELDS = ("current_floor", "max_floor", "score", "weekly_purchases")


@dataclass(frozen=True)
class TowerFloorResetResult:
    status: str
    total: int = 0
    changed: int = 0

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


class TowerStateRepository:
    def __init__(self, player_database: str | Path) -> None:
        self.player_database = str(player_database)

    @staticmethod
    def _period_key(today: date) -> str:
        iso = today.isocalendar()
        return f"{iso.year}-W{iso.week:02d}"

    @staticmethod
    def _default(today: date) -> dict[str, Any]:
        return {
            "current_floor": 0,
            "max_floor": 0,
            "score": 0,
            "weekly_purchases": {"_last_reset": today.isoformat()},
        }

    @staticmethod
    def _weekly(value: Any, today: date) -> tuple[dict[str, int | str], bool]:
        changed = False
        if isinstance(value, str):
            try:
                value = json.loads(value) if value else {}
            except (TypeError, ValueError):
                value, changed = {}, True
        if not isinstance(value, dict):
            value, changed = {}, True
        try:
            reset = date.fromisoformat(str(value.get("_last_reset", "")))
        except (TypeError, ValueError):
            reset = None
        if reset is None or reset.isocalendar()[:2] != today.isocalendar()[:2]:
            return {"_last_reset": today.isoformat()}, True
        weekly: dict[str, int | str] = {"_last_reset": reset.isoformat()}
        for raw_key, raw_amount in value.items():
            key = str(raw_key)
            if key == "_last_reset":
                continue
            try:
                amount = int(raw_amount)
            except (TypeError, ValueError):
                changed = True
                continue
            if amount < 0:
                changed = True
                continue
            weekly[key] = amount
            if key != raw_key or not isinstance(raw_amount, int) or isinstance(raw_amount, bool):
                changed = True
        return weekly, changed

    def initialize(self, user_id: str, today: date) -> dict[str, Any]:
        user_id = str(user_id).strip()
        if not user_id:
            raise ValueError("user_id is required")
        with DatabaseUnitOfWork(self.player_database, immediate=True) as uow:
            row = uow.query_one(
                "SELECT current_floor,max_floor,score,weekly_purchases FROM tower WHERE user_id=?",
                (user_id,),
            )
            if row is not None:
                result = dict(row)
                result["current_floor"] = int(result["current_floor"] or 0)
                result["max_floor"] = int(result["max_floor"] or 0)
                result["score"] = int(result["score"] or 0)
                weekly, weekly_changed = self._weekly(result["weekly_purchases"], today)
                result["weekly_purchases"] = weekly
                if weekly_changed:
                    period_key = self._period_key(today)
                    uow.execute(
                        "UPDATE tower SET weekly_purchases=? WHERE user_id=?",
                        (json.dumps(weekly, ensure_ascii=True, sort_keys=True), user_id),
                    )
                    uow.execute(
                        "INSERT INTO tower_state_operations(operation_id,user_id,kind,period_key,snapshot,created_at) "
                        "VALUES(?,?,?,?,?,CURRENT_TIMESTAMP)",
                        (
                            f"tower-state-week:{user_id}:{period_key}",
                            user_id,
                            "week",
                            period_key,
                            json.dumps(result, ensure_ascii=True, sort_keys=True, separators=(",", ":")),
                        ),
                    )
                return result
            state = self._default(today)
            uow.execute(
                "INSERT INTO tower(user_id,current_floor,max_floor,score,weekly_purchases) VALUES(?,?,?,?,?)",
                (
                    user_id,
                    state["current_floor"],
                    state["max_floor"],
                    state["score"],
                    json.dumps(state["weekly_purchases"], ensure_ascii=True, sort_keys=True),
                ),
            )
            uow.execute(
                "INSERT INTO tower_state_operations(operation_id,user_id,kind,period_key,snapshot,created_at) "
                "VALUES(?,?,?,?,?,CURRENT_TIMESTAMP)",
                (
                    f"tower-state-init:{user_id}",
                    user_id,
                    "initialize",
                    self._period_key(today),
                    json.dumps(state, ensure_ascii=True, sort_keys=True, separators=(",", ":")),
                ),
            )
            return state

    def reset_all_floors(
        self, *, operation_id: str, source: str, period_key: str
    ) -> TowerFloorResetResult:
        operation_id, source, period_key = (
            str(operation_id).strip(), str(source).strip(), str(period_key).strip()
        )
        if not operation_id or source not in {"admin", "scheduler"} or not period_key:
            raise ValueError("valid tower reset identity is required")
        if not Path(self.player_database).is_file():
            return TowerFloorResetResult("schema_missing")

        required = {
            "tower": {"user_id", "current_floor"},
            "tower_state_operations": {
                "operation_id", "user_id", "kind", "period_key", "snapshot"
            },
        }
        request = {"source": source, "period_key": period_key}
        with DatabaseUnitOfWork(self.player_database, immediate=True) as uow:
            for table, columns in required.items():
                actual = {
                    str(row["name"])
                    for row in uow.query_all(f"PRAGMA table_info({table})")
                }
                if not columns <= actual:
                    return TowerFloorResetResult("schema_missing")

            previous = uow.query_one(
                "SELECT user_id,kind,period_key,snapshot FROM tower_state_operations "
                "WHERE operation_id=?",
                (operation_id,),
            )
            if previous is not None:
                try:
                    saved = json.loads(str(previous["snapshot"] or "{}"))
                except (TypeError, ValueError):
                    return TowerFloorResetResult("operation_conflict")
                if (
                    not isinstance(saved, dict)
                    or str(previous["user_id"]) != "0"
                    or str(previous["kind"]) != "reset_all_floors"
                    or str(previous["period_key"]) != period_key
                    or saved.get("request") != request
                ):
                    return TowerFloorResetResult("operation_conflict")
                result = saved.get("result")
                if not isinstance(result, dict):
                    return TowerFloorResetResult("operation_conflict")
                total_value = result.get("total")
                changed_value = result.get("changed")
                if (
                    not isinstance(total_value, int)
                    or isinstance(total_value, bool)
                    or not isinstance(changed_value, int)
                    or isinstance(changed_value, bool)
                    or total_value < 0
                    or changed_value < 0
                    or changed_value > total_value
                ):
                    return TowerFloorResetResult("operation_conflict")
                return TowerFloorResetResult(
                    "duplicate", total_value, changed_value
                )

            total_row = uow.query_one("SELECT COUNT(*) AS total FROM tower")
            total = int(total_row["total"] if total_row is not None else 0)
            changed = uow.execute(
                "UPDATE tower SET current_floor=0 "
                "WHERE current_floor IS NULL OR current_floor<>0"
            ).rowcount
            result = {"total": total, "changed": int(changed)}
            snapshot = json.dumps(
                {"request": request, "result": result},
                ensure_ascii=True,
                sort_keys=True,
                separators=(",", ":"),
            )
            uow.execute(
                "INSERT INTO tower_state_operations(operation_id,user_id,kind,period_key,snapshot) "
                "VALUES(?,?,?,?,?)",
                (operation_id, "0", "reset_all_floors", period_key, snapshot),
            )
            return TowerFloorResetResult("applied", total, int(changed))

    def ranking(self, field: str, limit: int = 50) -> list[tuple[str, int]]:
        if field not in {"current_floor", "score"}:
            raise ValueError("unsupported tower ranking field")
        limit = min(50, max(0, int(limit)))
        if limit == 0 or not Path(self.player_database).is_file():
            return []
        with DatabaseUnitOfWork(self.player_database) as uow:
            columns = {
                str(row["name"]) for row in uow.query_all("PRAGMA table_info(tower)")
            }
            if not {"user_id", field} <= columns:
                return []
            rows = uow.query_all(
                f'SELECT user_id,CAST(COALESCE("{field}",0) AS INTEGER) AS value '
                f'FROM tower ORDER BY value DESC,user_id ASC LIMIT ?',
                (limit,),
            )
        return [(str(row["user_id"]), int(row["value"] or 0)) for row in rows]


__all__ = ["TOWER_FIELDS", "TowerFloorResetResult", "TowerStateRepository"]
