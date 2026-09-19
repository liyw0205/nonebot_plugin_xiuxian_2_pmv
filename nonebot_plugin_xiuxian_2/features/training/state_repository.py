from __future__ import annotations

import json
from datetime import date, datetime
from pathlib import Path
from typing import Any

from ...infrastructure.database import DatabaseUnitOfWork


TRAINING_FIELDS = (
    "progress", "last_time", "points", "completed", "max_progress", "last_event", "weekly_purchases"
)


class TrainingStateRepository:
    def __init__(self, player_database: str | Path) -> None:
        self.player_database = str(player_database)

    @staticmethod
    def _period_key(today: date) -> str:
        iso = today.isocalendar()
        return f"{iso.year}-W{iso.week:02d}"

    @staticmethod
    def _default(today: date) -> dict[str, Any]:
        return {
            "progress": 0,
            "last_time": None,
            "points": 0,
            "completed": 0,
            "max_progress": 0,
            "last_event": "",
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
                "SELECT progress,last_time,points,completed,max_progress,last_event,weekly_purchases "
                "FROM training WHERE user_id=?",
                (user_id,),
            )
            if row is not None:
                result = dict(row)
                for key in ("progress", "points", "completed", "max_progress"):
                    result[key] = int(result[key] or 0)
                if result["last_time"]:
                    result["last_time"] = datetime.fromisoformat(str(result["last_time"]))
                else:
                    result["last_time"] = None
                weekly, weekly_changed = self._weekly(result["weekly_purchases"], today)
                result["weekly_purchases"] = weekly
                result["last_event"] = str(result["last_event"] or "")
                if weekly_changed:
                    period_key = self._period_key(today)
                    uow.execute(
                        "UPDATE training SET weekly_purchases=? WHERE user_id=?",
                        (json.dumps(weekly, ensure_ascii=True, sort_keys=True), user_id),
                    )
                    uow.execute(
                        "INSERT INTO training_state_operations(operation_id,user_id,kind,period_key,snapshot,created_at) "
                        "VALUES(?,?,?,?,?,CURRENT_TIMESTAMP)",
                        (
                            f"training-state-week:{user_id}:{period_key}", user_id, "week", period_key,
                            json.dumps(result, ensure_ascii=True, sort_keys=True, default=str, separators=(",", ":")),
                        ),
                    )
                return result
            state = self._default(today)
            uow.execute(
                "INSERT INTO training(user_id,progress,last_time,points,completed,max_progress,last_event,weekly_purchases) "
                "VALUES(?,?,?,?,?,?,?,?)",
                (
                    user_id, 0, None, 0, 0, 0, "",
                    json.dumps(state["weekly_purchases"], ensure_ascii=True, sort_keys=True),
                ),
            )
            uow.execute(
                "INSERT INTO training_state_operations(operation_id,user_id,kind,period_key,snapshot,created_at) "
                "VALUES(?,?,?,?,?,CURRENT_TIMESTAMP)",
                (
                    f"training-state-init:{user_id}", user_id, "initialize", self._period_key(today),
                    json.dumps(state, ensure_ascii=True, sort_keys=True, default=str, separators=(",", ":")),
                ),
            )
            return state


__all__ = ["TRAINING_FIELDS", "TrainingStateRepository"]
