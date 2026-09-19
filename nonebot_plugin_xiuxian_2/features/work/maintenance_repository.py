from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any

from ...infrastructure.database import DatabaseUnitOfWork


@dataclass(frozen=True)
class WorkDailyRefreshResetResult:
    status: str
    business_date: str
    task_status: str = ""
    reset_count: int = 0
    total: int = 0
    completed: int = 0
    changed: int = 0

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


class WorkDailyRefreshResetRepository:
    def __init__(self, database: str | Path) -> None:
        self.database = str(database)

    @staticmethod
    def _date(value: Any) -> str:
        if isinstance(value, datetime):
            value = value.date()
        if isinstance(value, date):
            return value.isoformat()
        return date.fromisoformat(str(value).strip()).isoformat()

    @staticmethod
    def _result(uow: DatabaseUnitOfWork, business_date: str, status: str) -> WorkDailyRefreshResetResult:
        row = uow.query_one(
            "SELECT reset_count,total,completed,changed,status "
            "FROM work_daily_refresh_reset_operations WHERE business_date=?",
            (business_date,),
        )
        if row is None:
            return WorkDailyRefreshResetResult(status, business_date)
        return WorkDailyRefreshResetResult(
            status, business_date, str(row["status"]), int(row["reset_count"]),
            int(row["total"]), int(row["completed"]), int(row["changed"]),
        )

    def reset(
        self,
        business_date: Any,
        reset_count: int,
        *,
        chunk_size: int = 500,
        updated_at: str | None = None,
    ) -> WorkDailyRefreshResetResult:
        business_date = self._date(business_date)
        reset_count, chunk_size = int(reset_count), max(1, int(chunk_size))
        updated_at = str(updated_at or datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        if reset_count < 0:
            raise ValueError("reset count must not be negative")
        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            operation = uow.query_one(
                "SELECT reset_count,status FROM work_daily_refresh_reset_operations WHERE business_date=?",
                (business_date,),
            )
            if operation is None:
                user_ids = tuple(
                    str(row["user_id"])
                    for row in uow.query_all("SELECT DISTINCT user_id FROM user_xiuxian ORDER BY user_id")
                )
                uow.execute(
                    "INSERT INTO work_daily_refresh_reset_operations "
                    "(business_date,reset_count,total,status,created_at,updated_at) VALUES(?,?,?,?,?,?)",
                    (business_date, reset_count, len(user_ids), "completed" if not user_ids else "running", updated_at, updated_at),
                )
                uow.executemany(
                    "INSERT INTO work_daily_refresh_reset_targets(business_date,user_id,updated_at) VALUES(?,?,?)",
                    ((business_date, user_id, updated_at) for user_id in user_ids),
                )
                if not user_ids:
                    return self._result(uow, business_date, "applied")
            else:
                if int(operation["reset_count"]) != reset_count:
                    return self._result(uow, business_date, "operation_conflict")
                if str(operation["status"]) == "completed":
                    return self._result(uow, business_date, "duplicate")

            pending = uow.query_all(
                "SELECT user_id FROM work_daily_refresh_reset_targets "
                "WHERE business_date=? AND status='pending' ORDER BY user_id LIMIT ?",
                (business_date, chunk_size),
            )
            changed = 0
            for pending_row in pending:
                user_id = str(pending_row["user_id"])
                user = uow.query_one(
                    "SELECT COUNT(*) AS row_count,MIN(COALESCE(work_num,0)) AS previous_count,"
                    "MAX(COALESCE(work_num,0)) AS previous_max FROM user_xiuxian WHERE user_id=?",
                    (user_id,),
                )
                row_count = int(user["row_count"] or 0) if user else 0
                if row_count == 0:
                    uow.execute(
                        "UPDATE work_daily_refresh_reset_targets SET status='skipped',updated_at=? "
                        "WHERE business_date=? AND user_id=? AND status='pending'",
                        (updated_at, business_date, user_id),
                    )
                    continue
                previous_count, previous_max = int(user["previous_count"] or 0), int(user["previous_max"] or 0)
                updated = uow.execute("UPDATE user_xiuxian SET work_num=? WHERE user_id=?", (reset_count, user_id))
                if updated.rowcount != row_count:
                    raise RuntimeError("work refresh reset target changed")
                changed += int(previous_count != reset_count or previous_max != reset_count)
                uow.execute(
                    "UPDATE work_daily_refresh_reset_targets SET status='applied',previous_count=?,final_count=?,updated_at=? "
                    "WHERE business_date=? AND user_id=? AND status='pending'",
                    (previous_count, reset_count, updated_at, business_date, user_id),
                )
            progress = uow.query_one(
                "SELECT COUNT(*) AS total,COALESCE(SUM(CASE WHEN status='pending' THEN 1 ELSE 0 END),0) AS pending "
                "FROM work_daily_refresh_reset_targets WHERE business_date=?",
                (business_date,),
            )
            completed = int(progress["total"]) - int(progress["pending"])
            task_status = "completed" if int(progress["pending"]) == 0 else "running"
            uow.execute(
                "UPDATE work_daily_refresh_reset_operations SET completed=?,changed=changed+?,status=?,updated_at=? WHERE business_date=?",
                (completed, changed, task_status, updated_at, business_date),
            )
            return self._result(uow, business_date, "applied")


__all__ = ["WorkDailyRefreshResetRepository", "WorkDailyRefreshResetResult"]
