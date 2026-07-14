from __future__ import annotations

import json
from contextlib import closing
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from threading import RLock

from ..xiuxian_utils import db_backend


@dataclass(frozen=True)
class InteractiveGreetingClaimResult:
    status: str
    kind: str = ""
    business_date: str = ""
    claimed: bool = False
    position: int = 0

    @property
    def succeeded(self) -> bool:
        return self.claimed and self.status in {"claimed", "duplicate"}


class InteractiveGreetingClaimService:
    """Atomically assign daily morning and night greeting positions."""

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
            "CREATE TABLE IF NOT EXISTS interactive_greeting_claims("
            "kind TEXT NOT NULL,business_date TEXT NOT NULL,user_id TEXT NOT NULL,"
            "position INTEGER NOT NULL,operation_id TEXT NOT NULL,"
            "PRIMARY KEY(kind,business_date,user_id),"
            "UNIQUE(kind,business_date,position))"
        )
        conn.execute(
            "CREATE TABLE IF NOT EXISTS interactive_greeting_operations("
            "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,"
            "business_date TEXT NOT NULL,claimed INTEGER NOT NULL,"
            "position INTEGER NOT NULL,"
            "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
        )

    def claim(
        self,
        operation_id,
        user_id,
        kind,
        business_date,
    ) -> InteractiveGreetingClaimResult:
        operation_id = str(operation_id).strip()
        user_id = str(user_id).strip()
        kind = str(kind).strip().lower()
        business_date = self._business_date(business_date)
        if not operation_id or not user_id or kind not in {"morning", "night"}:
            raise ValueError("valid greeting claim is required")
        payload = json.dumps(
            [user_id, kind, business_date],
            ensure_ascii=True,
            separators=(",", ":"),
        )

        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_schema(conn)
                previous = conn.execute(
                    "SELECT payload,claimed,position "
                    "FROM interactive_greeting_operations WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    if str(previous[0]) != payload:
                        return InteractiveGreetingClaimResult("operation_conflict")
                    return InteractiveGreetingClaimResult(
                        "duplicate",
                        kind,
                        business_date,
                        bool(previous[1]),
                        int(previous[2]),
                    )

                if conn.execute(
                    "SELECT 1 FROM user_xiuxian WHERE user_id=%s",
                    (user_id,),
                ).fetchone() is None:
                    conn.rollback()
                    return InteractiveGreetingClaimResult(
                        "user_missing", kind, business_date
                    )

                existing = conn.execute(
                    "SELECT position FROM interactive_greeting_claims "
                    "WHERE kind=%s AND business_date=%s AND user_id=%s",
                    (kind, business_date, user_id),
                ).fetchone()
                if existing is not None:
                    position = int(existing[0])
                    conn.execute(
                        "INSERT INTO interactive_greeting_operations("
                        "operation_id,payload,business_date,claimed,position) "
                        "VALUES(%s,%s,%s,0,%s)",
                        (operation_id, payload, business_date, position),
                    )
                    conn.commit()
                    return InteractiveGreetingClaimResult(
                        "already_claimed", kind, business_date, False, position
                    )

                row = conn.execute(
                    "SELECT COALESCE(MAX(position),0) "
                    "FROM interactive_greeting_claims "
                    "WHERE kind=%s AND business_date=%s",
                    (kind, business_date),
                ).fetchone()
                position = int(row[0]) + 1
                conn.execute(
                    "INSERT INTO interactive_greeting_claims("
                    "kind,business_date,user_id,position,operation_id) "
                    "VALUES(%s,%s,%s,%s,%s)",
                    (kind, business_date, user_id, position, operation_id),
                )
                conn.execute(
                    "INSERT INTO interactive_greeting_operations("
                    "operation_id,payload,business_date,claimed,position) "
                    "VALUES(%s,%s,%s,1,%s)",
                    (operation_id, payload, business_date, position),
                )
                conn.commit()
                return InteractiveGreetingClaimResult(
                    "claimed", kind, business_date, True, position
                )
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
                    "DELETE FROM interactive_greeting_operations "
                    "WHERE business_date<%s",
                    (cutoff_date,),
                ).rowcount
                claims = conn.execute(
                    "DELETE FROM interactive_greeting_claims WHERE business_date<%s",
                    (cutoff_date,),
                ).rowcount
                conn.commit()
                return int(operations) + int(claims)
            except Exception:
                conn.rollback()
                raise


__all__ = ["InteractiveGreetingClaimResult", "InteractiveGreetingClaimService"]
