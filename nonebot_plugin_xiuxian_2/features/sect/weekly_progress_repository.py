from __future__ import annotations

import json
from collections.abc import Iterable, Mapping
from pathlib import Path
from typing import Any

from ...infrastructure.database import DatabaseUnitOfWork


class SectWeeklyProgressSqlRepository:
    """Owns the migrated sect weekly goal progress tables."""

    _REQUIRED_COLUMNS = {
        "sect_id",
        "week_key",
        "goal_key",
        "progress",
        "target",
        "participants",
        "claimed_users",
        "updated_at",
    }

    def __init__(self, database: str | Path) -> None:
        self.database = str(database)

    @classmethod
    def _assert_schema_ready(cls, uow: DatabaseUnitOfWork) -> None:
        table = uow.query_one(
            "SELECT 1 AS present FROM sqlite_master WHERE type='table' AND name=?",
            ("sect_weekly_goal",),
        )
        columns = {
            str(row["name"]) for row in uow.query_all("PRAGMA table_info(sect_weekly_goal)")
        }
        if table is None or not cls._REQUIRED_COLUMNS.issubset(columns):
            raise RuntimeError("sect_weekly_goal schema is not ready; run migrations first")

    @staticmethod
    def _goal_values(goals: Iterable[Mapping[str, Any]]) -> tuple[tuple[str, int], ...]:
        values = tuple((str(goal["key"]), int(goal["target"])) for goal in goals)
        if any(not key or target <= 0 for key, target in values):
            raise ValueError("weekly goal keys and targets must be valid")
        if len({key for key, _ in values}) != len(values):
            raise ValueError("weekly goal keys must be unique")
        return values

    @classmethod
    def _ensure_goals(
        cls,
        uow: DatabaseUnitOfWork,
        sect_id: int,
        week_key: str,
        goals: tuple[tuple[str, int], ...],
        updated_at: str,
    ) -> None:
        cls._assert_schema_ready(uow)
        uow.executemany(
            "INSERT OR IGNORE INTO sect_weekly_goal "
            "(sect_id,week_key,goal_key,progress,target,participants,claimed_users,updated_at) "
            "VALUES(?,?,?,0,?,'{}','[]',?)",
            ((sect_id, week_key, key, target, updated_at) for key, target in goals),
        )

    def assert_schema_ready(self) -> None:
        with DatabaseUnitOfWork(self.database, read_only=True) as uow:
            self._assert_schema_ready(uow)

    def ensure_goals(
        self,
        sect_id: int | str,
        week_key: str,
        goals: Iterable[Mapping[str, Any]],
        updated_at: str,
    ) -> None:
        sect_id, week_key = int(sect_id), str(week_key)
        if sect_id <= 0 or not week_key:
            raise ValueError("sect_id and week_key are required")
        goal_values = self._goal_values(goals)
        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            self._ensure_goals(uow, sect_id, week_key, goal_values, updated_at)

    def list_goals(
        self,
        sect_id: int | str,
        week_key: str,
        goals: Iterable[Mapping[str, Any]],
        updated_at: str,
    ) -> list[dict[str, Any]]:
        sect_id, week_key = int(sect_id), str(week_key)
        if sect_id <= 0 or not week_key:
            raise ValueError("sect_id and week_key are required")
        goal_values = self._goal_values(goals)
        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            self._ensure_goals(uow, sect_id, week_key, goal_values, updated_at)
            return uow.query_all(
                "SELECT * FROM sect_weekly_goal WHERE sect_id=? AND week_key=? ORDER BY goal_key",
                (sect_id, week_key),
            )

    def record_progress(
        self,
        sect_id: int | str,
        week_key: str,
        user_id: int | str,
        amount: int,
        goals: Iterable[Mapping[str, Any]],
        updated_at: str,
    ) -> list[dict[str, Any]]:
        sect_id, week_key, user_id, amount = int(sect_id), str(week_key), str(user_id), int(amount)
        if sect_id <= 0 or not week_key or not user_id or amount <= 0:
            raise ValueError("sect_id, week_key, user_id and positive amount are required")
        goal_values = self._goal_values(goals)
        updated: list[dict[str, Any]] = []
        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            self._ensure_goals(uow, sect_id, week_key, goal_values, updated_at)
            for goal_key, target in goal_values:
                row = uow.query_one(
                    "SELECT progress,participants FROM sect_weekly_goal "
                    "WHERE sect_id=? AND week_key=? AND goal_key=?",
                    (sect_id, week_key, goal_key),
                )
                if row is None:
                    raise RuntimeError("weekly goal disappeared during progress update")
                old_progress = int(row["progress"] or 0)
                try:
                    participants = json.loads(str(row["participants"] or "{}"))
                except (TypeError, ValueError, json.JSONDecodeError):
                    participants = {}
                if not isinstance(participants, dict):
                    participants = {}
                participants[user_id] = int(participants.get(user_id, 0) or 0) + amount
                new_progress = min(old_progress + amount, target)
                uow.execute(
                    "UPDATE sect_weekly_goal SET progress=?,participants=?,updated_at=? "
                    "WHERE sect_id=? AND week_key=? AND goal_key=?",
                    (
                        new_progress,
                        json.dumps(participants, ensure_ascii=False),
                        updated_at,
                        sect_id,
                        week_key,
                        goal_key,
                    ),
                )
                updated.append(
                    {
                        "goal_key": goal_key,
                        "old_progress": old_progress,
                        "progress": new_progress,
                        "target": target,
                        "completed": old_progress < target <= new_progress,
                    }
                )
        return updated

    def weekly_rank(self, limit: int, week_key: str) -> list[dict[str, Any]]:
        limit, week_key = max(1, min(int(limit), 50)), str(week_key)
        with DatabaseUnitOfWork(self.database, read_only=True) as uow:
            self._assert_schema_ready(uow)
            sects = uow.query_one(
                "SELECT 1 AS present FROM sqlite_master WHERE type='table' AND name=?",
                ("sects",),
            )
            if sects is None:
                raise RuntimeError("sects schema is not ready; run migrations first")
            return uow.query_all(
                "SELECT g.sect_id,COALESCE(s.sect_name,g.sect_id) AS sect_name,"
                "SUM(g.progress) AS total_progress FROM sect_weekly_goal AS g "
                "LEFT JOIN sects AS s ON s.sect_id=g.sect_id WHERE g.week_key=? "
                "GROUP BY g.sect_id,s.sect_name ORDER BY total_progress DESC LIMIT ?",
                (week_key, limit),
            )


__all__ = ["SectWeeklyProgressSqlRepository"]
