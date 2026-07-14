from __future__ import annotations

import json
from contextlib import closing
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from threading import RLock
from typing import Callable

from ..xiuxian_utils import db_backend


@dataclass(frozen=True)
class InteractiveDailyFortuneResult:
    status: str
    business_date: str = ""
    fortune_type: str = ""
    description: str = ""
    stars: str = ""

    @property
    def succeeded(self) -> bool:
        return self.status in {"generated", "existing", "duplicate"}

    @property
    def fortune(self) -> dict[str, str]:
        return {
            "type": self.fortune_type,
            "description": self.description,
            "stars": self.stars,
        }


class InteractiveDailyFortuneService:
    """Persist and replay one fixed fortune per player and business date."""

    def __init__(self, database: str | Path, lock: RLock | None = None) -> None:
        self._database = Path(database)
        self._lock = lock or RLock()

    @staticmethod
    def _business_date(value: date | datetime | str) -> str:
        if isinstance(value, datetime):
            return value.date().isoformat()
        if isinstance(value, date):
            return value.isoformat()
        return date.fromisoformat(str(value)).isoformat()

    @staticmethod
    def _ensure_schema(conn) -> None:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS interactive_daily_fortunes("
            "user_id TEXT NOT NULL,business_date TEXT NOT NULL,"
            "fortune_type TEXT NOT NULL,description TEXT NOT NULL,stars TEXT NOT NULL,"
            "operation_id TEXT NOT NULL,PRIMARY KEY(user_id,business_date))"
        )
        conn.execute(
            "CREATE TABLE IF NOT EXISTS interactive_daily_fortune_operations("
            "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,"
            "business_date TEXT NOT NULL,fortune_type TEXT NOT NULL,"
            "description TEXT NOT NULL,stars TEXT NOT NULL,"
            "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
        )

    @staticmethod
    def _result(status: str, business_date: str, row) -> InteractiveDailyFortuneResult:
        return InteractiveDailyFortuneResult(
            status,
            business_date,
            str(row[0]),
            str(row[1]),
            str(row[2]),
        )

    @staticmethod
    def _normalize_fortune(value) -> tuple[str, str, str]:
        if not isinstance(value, dict):
            raise ValueError("fortune factory must return a mapping")
        fortune = (
            str(value.get("type", "")).strip(),
            str(value.get("description", "")).strip(),
            str(value.get("stars", "")).strip(),
        )
        if not all(fortune):
            raise ValueError("fortune fields are required")
        return fortune

    def resolve(
        self,
        operation_id,
        user_id,
        business_date,
        create_fortune: Callable[[], dict[str, str]],
    ) -> InteractiveDailyFortuneResult:
        operation_id = str(operation_id).strip()
        user_id = str(user_id).strip()
        business_date = self._business_date(business_date)
        if not operation_id or not user_id or not callable(create_fortune):
            raise ValueError("valid daily fortune request is required")
        payload = json.dumps(
            [user_id, business_date],
            ensure_ascii=True,
            separators=(",", ":"),
        )

        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_schema(conn)
                previous = conn.execute(
                    "SELECT payload,fortune_type,description,stars "
                    "FROM interactive_daily_fortune_operations WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    if str(previous[0]) != payload:
                        return InteractiveDailyFortuneResult("operation_conflict")
                    return self._result("duplicate", business_date, previous[1:])

                if conn.execute(
                    "SELECT 1 FROM user_xiuxian WHERE user_id=%s",
                    (user_id,),
                ).fetchone() is None:
                    conn.rollback()
                    return InteractiveDailyFortuneResult(
                        "user_missing", business_date
                    )

                existing = conn.execute(
                    "SELECT fortune_type,description,stars "
                    "FROM interactive_daily_fortunes "
                    "WHERE user_id=%s AND business_date=%s",
                    (user_id, business_date),
                ).fetchone()
                if existing is not None:
                    conn.execute(
                        "INSERT INTO interactive_daily_fortune_operations("
                        "operation_id,payload,business_date,fortune_type,description,stars) "
                        "VALUES(%s,%s,%s,%s,%s,%s)",
                        (operation_id, payload, business_date, *existing),
                    )
                    conn.commit()
                    return self._result("existing", business_date, existing)

                fortune = self._normalize_fortune(create_fortune())
                conn.execute(
                    "INSERT INTO interactive_daily_fortunes("
                    "user_id,business_date,fortune_type,description,stars,operation_id) "
                    "VALUES(%s,%s,%s,%s,%s,%s)",
                    (user_id, business_date, *fortune, operation_id),
                )
                conn.execute(
                    "INSERT INTO interactive_daily_fortune_operations("
                    "operation_id,payload,business_date,fortune_type,description,stars) "
                    "VALUES(%s,%s,%s,%s,%s,%s)",
                    (operation_id, payload, business_date, *fortune),
                )
                conn.commit()
                return self._result("generated", business_date, fortune)
            except Exception:
                conn.rollback()
                raise

    def cleanup_before(self, cutoff: date | datetime | str) -> int:
        cutoff_date = self._business_date(cutoff)
        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_schema(conn)
                operations = conn.execute(
                    "DELETE FROM interactive_daily_fortune_operations "
                    "WHERE business_date<%s",
                    (cutoff_date,),
                ).rowcount
                fortunes = conn.execute(
                    "DELETE FROM interactive_daily_fortunes WHERE business_date<%s",
                    (cutoff_date,),
                ).rowcount
                conn.commit()
                return int(operations) + int(fortunes)
            except Exception:
                conn.rollback()
                raise


__all__ = ["InteractiveDailyFortuneResult", "InteractiveDailyFortuneService"]
