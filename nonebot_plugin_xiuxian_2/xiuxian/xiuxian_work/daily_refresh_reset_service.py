from __future__ import annotations

from contextlib import closing
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from threading import RLock

from ..xiuxian_utils import db_backend


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


class WorkDailyRefreshResetService:
    """Reset a date-frozen player set in durable chunks."""

    def __init__(self, database: str | Path, lock: RLock | None = None) -> None:
        self._database = Path(database)
        self._lock = lock or RLock()

    @staticmethod
    def _normalize_date(value) -> str:
        if isinstance(value, datetime):
            value = value.date()
        if isinstance(value, date):
            return value.isoformat()
        return date.fromisoformat(str(value).strip()).isoformat()

    @staticmethod
    def _ensure_schema(conn) -> None:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS work_daily_refresh_reset_operations("
            "business_date TEXT PRIMARY KEY,reset_count INTEGER NOT NULL,total INTEGER NOT NULL,"
            "completed INTEGER NOT NULL DEFAULT 0,changed INTEGER NOT NULL DEFAULT 0,"
            "status TEXT NOT NULL DEFAULT 'running',created_at TEXT NOT NULL,updated_at TEXT NOT NULL)"
        )
        conn.execute(
            "CREATE TABLE IF NOT EXISTS work_daily_refresh_reset_targets("
            "business_date TEXT NOT NULL,user_id TEXT NOT NULL,status TEXT NOT NULL DEFAULT 'pending',"
            "previous_count INTEGER,final_count INTEGER,updated_at TEXT NOT NULL,"
            "PRIMARY KEY(business_date,user_id))"
        )

    @staticmethod
    def _result(conn, business_date, status):
        row = conn.execute(
            "SELECT reset_count,total,completed,changed,status "
            "FROM work_daily_refresh_reset_operations WHERE business_date=%s",
            (business_date,),
        ).fetchone()
        if row is None:
            return WorkDailyRefreshResetResult(status, business_date)
        return WorkDailyRefreshResetResult(
            status,
            business_date,
            str(row[4]),
            int(row[0]),
            int(row[1]),
            int(row[2]),
            int(row[3]),
        )

    def reset(
        self,
        business_date,
        reset_count,
        *,
        chunk_size=500,
        updated_at=None,
    ) -> WorkDailyRefreshResetResult:
        business_date = self._normalize_date(business_date)
        reset_count = int(reset_count)
        chunk_size = max(1, int(chunk_size))
        updated_at = str(updated_at or datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        if reset_count < 0:
            raise ValueError("reset count must not be negative")

        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_schema(conn)
                operation = conn.execute(
                    "SELECT reset_count,status FROM work_daily_refresh_reset_operations "
                    "WHERE business_date=%s",
                    (business_date,),
                ).fetchone()
                if operation is None:
                    user_ids = tuple(
                        str(row[0])
                        for row in conn.execute(
                            "SELECT DISTINCT user_id FROM user_xiuxian ORDER BY user_id"
                        ).fetchall()
                    )
                    task_status = "completed" if not user_ids else "running"
                    conn.execute(
                        "INSERT INTO work_daily_refresh_reset_operations("
                        "business_date,reset_count,total,status,created_at,updated_at) "
                        "VALUES(%s,%s,%s,%s,%s,%s)",
                        (
                            business_date,
                            reset_count,
                            len(user_ids),
                            task_status,
                            updated_at,
                            updated_at,
                        ),
                    )
                    conn.executemany(
                        "INSERT INTO work_daily_refresh_reset_targets("
                        "business_date,user_id,updated_at) VALUES(%s,%s,%s)",
                        (
                            (business_date, user_id, updated_at)
                            for user_id in user_ids
                        ),
                    )
                    conn.commit()
                    if not user_ids:
                        return self._result(conn, business_date, "applied")
                else:
                    if int(operation[0]) != reset_count:
                        result = self._result(conn, business_date, "operation_conflict")
                        conn.rollback()
                        return result
                    if str(operation[1]) == "completed":
                        result = self._result(conn, business_date, "duplicate")
                        conn.rollback()
                        return result
                    conn.commit()

                conn.execute("BEGIN IMMEDIATE")
                pending = conn.execute(
                    "SELECT user_id FROM work_daily_refresh_reset_targets "
                    "WHERE business_date=%s AND status='pending' ORDER BY user_id LIMIT %s",
                    (business_date, chunk_size),
                ).fetchall()
                changed = 0
                for pending_row in pending:
                    user_id = str(pending_row[0])
                    user = conn.execute(
                        "SELECT COUNT(*),MIN(COALESCE(work_num,0)),"
                        "MAX(COALESCE(work_num,0)) FROM user_xiuxian WHERE user_id=%s",
                        (user_id,),
                    ).fetchone()
                    row_count = int(user[0] or 0) if user is not None else 0
                    if row_count == 0:
                        conn.execute(
                            "UPDATE work_daily_refresh_reset_targets SET status='skipped',"
                            "updated_at=%s WHERE business_date=%s AND user_id=%s AND status='pending'",
                            (updated_at, business_date, user_id),
                        )
                        continue
                    previous_count = int(user[1] or 0)
                    previous_max = int(user[2] or 0)
                    updated = conn.execute(
                        "UPDATE user_xiuxian SET work_num=%s WHERE user_id=%s",
                        (reset_count, user_id),
                    )
                    if updated.rowcount != row_count:
                        raise db_backend.IntegrityError("work refresh reset target changed")
                    changed += int(
                        previous_count != reset_count or previous_max != reset_count
                    )
                    conn.execute(
                        "UPDATE work_daily_refresh_reset_targets SET status='applied',"
                        "previous_count=%s,final_count=%s,updated_at=%s "
                        "WHERE business_date=%s AND user_id=%s AND status='pending'",
                        (
                            previous_count,
                            reset_count,
                            updated_at,
                            business_date,
                            user_id,
                        ),
                    )

                progress = conn.execute(
                    "SELECT COUNT(*),COALESCE(SUM(CASE WHEN status='pending' THEN 1 ELSE 0 END),0) "
                    "FROM work_daily_refresh_reset_targets WHERE business_date=%s",
                    (business_date,),
                ).fetchone()
                completed = int(progress[0]) - int(progress[1])
                task_status = "completed" if int(progress[1]) == 0 else "running"
                conn.execute(
                    "UPDATE work_daily_refresh_reset_operations SET completed=%s,changed=changed+%s,"
                    "status=%s,updated_at=%s WHERE business_date=%s",
                    (completed, changed, task_status, updated_at, business_date),
                )
                result = self._result(conn, business_date, "applied")
                conn.commit()
                return result
            except Exception:
                conn.rollback()
                raise


__all__ = ["WorkDailyRefreshResetResult", "WorkDailyRefreshResetService"]
