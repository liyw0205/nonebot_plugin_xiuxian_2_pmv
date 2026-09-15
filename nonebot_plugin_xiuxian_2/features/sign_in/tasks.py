from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any

from ...infrastructure.database import DatabaseUnitOfWork


class SignInTaskRepository:
    """Application-owned task projection for sign-in task definitions."""

    def __init__(self, database: str | Path) -> None:
        self.database = str(database)

    @staticmethod
    def ensure_schema(uow: DatabaseUnitOfWork) -> None:
        uow.execute("CREATE TABLE IF NOT EXISTS sign_in_task_events(operation_id TEXT PRIMARY KEY,user_id TEXT NOT NULL,occurred_at TEXT NOT NULL)")
        uow.execute("CREATE TABLE IF NOT EXISTS sign_in_task_projection(user_id TEXT PRIMARY KEY,daily_period TEXT NOT NULL,daily_progress INTEGER NOT NULL,weekly_period TEXT NOT NULL,weekly_progress INTEGER NOT NULL)")

    def record(self, *, user_id: str, operation_id: str, occurred_at: datetime) -> list[str]:
        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            inserted = uow.execute("INSERT INTO sign_in_task_events(operation_id,user_id,occurred_at) VALUES(?,?,?) ON CONFLICT(operation_id) DO NOTHING", (str(operation_id), str(user_id), occurred_at.isoformat()))
            if inserted.rowcount != 1:
                return []
            day = occurred_at.date().isoformat()
            week = f"{occurred_at.isocalendar().year}-W{occurred_at.isocalendar().week:02d}"
            previous_daily = 0
            previous_weekly = 0
            row = uow.query_one("SELECT daily_period,daily_progress,weekly_period,weekly_progress FROM sign_in_task_projection WHERE user_id=?", (str(user_id),))
            if row is None:
                daily_progress, weekly_progress = 1, 1
                uow.execute("INSERT INTO sign_in_task_projection VALUES(?,?,?,?,?)", (str(user_id), day, daily_progress, week, weekly_progress))
            else:
                previous_daily = int(row["daily_progress"]) if str(row["daily_period"]) == day else 0
                previous_weekly = int(row["weekly_progress"]) if str(row["weekly_period"]) == week else 0
                daily_progress = min(1, previous_daily + 1)
                weekly_progress = min(6, previous_weekly + 1)
                uow.execute("UPDATE sign_in_task_projection SET daily_period=?,daily_progress=?,weekly_period=?,weekly_progress=? WHERE user_id=?", (day, daily_progress, week, weekly_progress, str(user_id)))
            completed = []
            if row is None or previous_daily == 0:
                completed.append("今日问道")
            if row is not None and previous_weekly == 5 and weekly_progress == 6:
                completed.append("七日勤修")
            return completed


class SignInTaskEffects:
    """Boundary adapter for task progress after a committed sign-in."""

    def __init__(self, record_progress: Any) -> None:
        self.record_progress = record_progress

    def record(self, *, user_id: str, operation_id: str, amount: int = 1) -> list[str]:
        return list(self.record_progress(str(user_id), "sign_in", int(amount), operation_id=f"task-progress:{operation_id}") or [])


__all__ = ["SignInTaskEffects", "SignInTaskRepository"]
