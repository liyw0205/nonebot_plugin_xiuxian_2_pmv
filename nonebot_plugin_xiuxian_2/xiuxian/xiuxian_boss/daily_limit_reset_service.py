from __future__ import annotations

from contextlib import closing
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from threading import RLock

from ..xiuxian_utils import db_backend


@dataclass(frozen=True)
class WorldBossDailyLimitResetResult:
    status: str
    business_date: str
    task_status: str = ""
    total: int = 0
    completed: int = 0
    changed: int = 0
    skipped: int = 0

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


class WorldBossDailyLimitResetService:
    """Reset a date-frozen set of world-boss limit rows in durable chunks."""

    _FIELDS = ("boss_integral", "boss_stone", "boss_battle_count")

    def __init__(self, database: str | Path, lock: RLock | None = None) -> None:
        self._database = Path(database)
        self._lock = lock or RLock()

    @staticmethod
    def _normalize_date(value) -> str:
        if value is None:
            value = date.today()
        if isinstance(value, datetime):
            value = value.date()
        if isinstance(value, date):
            return value.isoformat()
        return date.fromisoformat(str(value).strip()).isoformat()

    @classmethod
    def _ensure_schema(cls, conn) -> None:
        conn.execute("CREATE TABLE IF NOT EXISTS boss(user_id TEXT PRIMARY KEY)")
        columns = set(conn.column_names("boss"))
        for field in cls._FIELDS:
            if field not in columns:
                conn.execute(
                    f"ALTER TABLE boss ADD COLUMN {db_backend.quote_ident(field)} "
                    "INTEGER DEFAULT 0"
                )
        conn.execute(
            "CREATE TABLE IF NOT EXISTS world_boss_daily_limit_reset_operations("
            "business_date TEXT PRIMARY KEY,total INTEGER NOT NULL,"
            "completed INTEGER NOT NULL DEFAULT 0,changed INTEGER NOT NULL DEFAULT 0,"
            "skipped INTEGER NOT NULL DEFAULT 0,status TEXT NOT NULL DEFAULT 'running',"
            "created_at TEXT NOT NULL,updated_at TEXT NOT NULL)"
        )
        conn.execute(
            "CREATE TABLE IF NOT EXISTS world_boss_daily_limit_reset_targets("
            "business_date TEXT NOT NULL,user_id TEXT NOT NULL,"
            "status TEXT NOT NULL DEFAULT 'pending',previous_integral INTEGER,"
            "previous_stone INTEGER,previous_battle_count INTEGER,updated_at TEXT NOT NULL,"
            "PRIMARY KEY(business_date,user_id))"
        )

    @staticmethod
    def _result(conn, business_date: str, status: str):
        row = conn.execute(
            "SELECT status,total,completed,changed,skipped "
            "FROM world_boss_daily_limit_reset_operations WHERE business_date=%s",
            (business_date,),
        ).fetchone()
        if row is None:
            return WorldBossDailyLimitResetResult(status, business_date)
        return WorldBossDailyLimitResetResult(
            status=status,
            business_date=business_date,
            task_status=str(row[0]),
            total=int(row[1]),
            completed=int(row[2]),
            changed=int(row[3]),
            skipped=int(row[4]),
        )

    def reset(
        self,
        business_date=None,
        *,
        chunk_size=500,
        updated_at=None,
    ) -> WorldBossDailyLimitResetResult:
        business_date = self._normalize_date(business_date)
        chunk_size = max(1, int(chunk_size))
        updated_at = str(
            updated_at or datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        )

        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_schema(conn)
                operation = conn.execute(
                    "SELECT status FROM world_boss_daily_limit_reset_operations "
                    "WHERE business_date=%s",
                    (business_date,),
                ).fetchone()
                if operation is None:
                    user_ids = tuple(
                        str(row[0])
                        for row in conn.execute(
                            "SELECT user_id FROM boss ORDER BY user_id"
                        ).fetchall()
                    )
                    task_status = "completed" if not user_ids else "running"
                    conn.execute(
                        "INSERT INTO world_boss_daily_limit_reset_operations("
                        "business_date,total,status,created_at,updated_at) "
                        "VALUES(%s,%s,%s,%s,%s)",
                        (
                            business_date,
                            len(user_ids),
                            task_status,
                            updated_at,
                            updated_at,
                        ),
                    )
                    conn.executemany(
                        "INSERT INTO world_boss_daily_limit_reset_targets("
                        "business_date,user_id,updated_at) VALUES(%s,%s,%s)",
                        (
                            (business_date, user_id, updated_at)
                            for user_id in user_ids
                        ),
                    )
                    conn.commit()
                    if not user_ids:
                        return self._result(conn, business_date, "applied")
                elif str(operation[0]) == "completed":
                    result = self._result(conn, business_date, "duplicate")
                    conn.rollback()
                    return result
                else:
                    conn.commit()

                conn.execute("BEGIN IMMEDIATE")
                pending = conn.execute(
                    "SELECT user_id FROM world_boss_daily_limit_reset_targets "
                    "WHERE business_date=%s AND status='pending' "
                    "ORDER BY user_id LIMIT %s",
                    (business_date, chunk_size),
                ).fetchall()
                changed = 0
                skipped = 0
                for pending_row in pending:
                    user_id = str(pending_row[0])
                    row = conn.execute(
                        "SELECT COALESCE(boss_integral,0),COALESCE(boss_stone,0),"
                        "COALESCE(boss_battle_count,0) FROM boss WHERE user_id=%s",
                        (user_id,),
                    ).fetchone()
                    if row is None:
                        skipped += 1
                        conn.execute(
                            "UPDATE world_boss_daily_limit_reset_targets SET "
                            "status='skipped',updated_at=%s WHERE business_date=%s "
                            "AND user_id=%s AND status='pending'",
                            (updated_at, business_date, user_id),
                        )
                        continue

                    previous = tuple(int(value or 0) for value in row)
                    updated = conn.execute(
                        "UPDATE boss SET boss_integral=0,boss_stone=0,"
                        "boss_battle_count=0 WHERE user_id=%s",
                        (user_id,),
                    )
                    if updated.rowcount != 1:
                        raise db_backend.IntegrityError(
                            "world boss daily reset target changed"
                        )
                    changed += int(any(previous))
                    conn.execute(
                        "UPDATE world_boss_daily_limit_reset_targets SET "
                        "status='applied',previous_integral=%s,previous_stone=%s,"
                        "previous_battle_count=%s,updated_at=%s WHERE business_date=%s "
                        "AND user_id=%s AND status='pending'",
                        (*previous, updated_at, business_date, user_id),
                    )

                progress = conn.execute(
                    "SELECT COUNT(*),COALESCE(SUM(CASE WHEN status='pending' "
                    "THEN 1 ELSE 0 END),0) FROM world_boss_daily_limit_reset_targets "
                    "WHERE business_date=%s",
                    (business_date,),
                ).fetchone()
                completed = int(progress[0]) - int(progress[1])
                task_status = "completed" if int(progress[1]) == 0 else "running"
                conn.execute(
                    "UPDATE world_boss_daily_limit_reset_operations SET completed=%s,"
                    "changed=changed+%s,skipped=skipped+%s,status=%s,updated_at=%s "
                    "WHERE business_date=%s",
                    (
                        completed,
                        changed,
                        skipped,
                        task_status,
                        updated_at,
                        business_date,
                    ),
                )
                result = self._result(conn, business_date, "applied")
                conn.commit()
                return result
            except Exception:
                conn.rollback()
                raise


__all__ = [
    "WorldBossDailyLimitResetResult",
    "WorldBossDailyLimitResetService",
]
