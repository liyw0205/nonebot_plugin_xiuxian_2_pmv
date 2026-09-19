from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Any

from ...infrastructure.database import DatabaseUnitOfWork


TOWER_FIELDS = ("current_floor", "max_floor", "score", "weekly_purchases")


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


__all__ = ["TOWER_FIELDS", "TowerStateRepository"]
