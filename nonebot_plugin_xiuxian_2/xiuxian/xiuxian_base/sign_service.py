from __future__ import annotations

import random
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from threading import RLock
from typing import Callable

from ..xiuxian_utils import db_backend


@dataclass(frozen=True)
class SignInResult:
    status: str
    user_id: str
    stone: int = 0

    @property
    def applied(self) -> bool:
        return self.status == "signed"

    @property
    def succeeded(self) -> bool:
        return self.status in {"signed", "duplicate"}


class SignInService:
    def __init__(
        self,
        database: str | Path,
        lock: RLock | None = None,
        randint: Callable[[int, int], int] | None = None,
    ) -> None:
        self._database = Path(database)
        self._lock = lock or RLock()
        self._randint = randint or random.randint

    @staticmethod
    def _ensure_operations(conn) -> None:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS sign_in_operations (
                operation_id TEXT PRIMARY KEY,
                user_id TEXT NOT NULL,
                stone INTEGER NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

    def get_result(self, operation_id: str) -> SignInResult | None:
        operation_id = str(operation_id).strip()
        if not operation_id:
            return None
        with self._lock, closing(db_backend.connect(self._database)) as conn:
            self._ensure_operations(conn)
            previous = conn.execute(
                "SELECT user_id, stone FROM sign_in_operations WHERE operation_id=%s",
                (operation_id,),
            ).fetchone()
            if previous is None:
                return None
            return SignInResult("duplicate", str(previous[0]), int(previous[1]))

    def sign(self, operation_id, user_id, stone_lower: int, stone_upper: int) -> SignInResult:
        operation_id = str(operation_id).strip()
        if not operation_id:
            raise ValueError("operation_id must not be empty")
        user_id = str(user_id)
        stone_lower = int(stone_lower)
        stone_upper = int(stone_upper)
        if stone_lower > stone_upper:
            raise ValueError("stone_lower must not exceed stone_upper")

        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_operations(conn)
                previous = conn.execute(
                    "SELECT stone FROM sign_in_operations WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous:
                    conn.rollback()
                    return SignInResult("duplicate", user_id, int(previous[0]))

                user = conn.execute(
                    "SELECT is_sign FROM user_xiuxian WHERE user_id=%s",
                    (user_id,),
                ).fetchone()
                if user is None:
                    conn.rollback()
                    return SignInResult("user_missing", user_id)
                if int(user[0] or 0) == 1:
                    conn.rollback()
                    return SignInResult("already_signed", user_id)

                stone = int(self._randint(stone_lower, stone_upper))
                updated = conn.execute(
                    "UPDATE user_xiuxian SET is_sign=1, stone=CAST(COALESCE(stone,0) AS INTEGER)+%s "
                    "WHERE user_id=%s AND CAST(COALESCE(is_sign,0) AS INTEGER)=0",
                    (stone, user_id),
                )
                if updated.rowcount != 1:
                    conn.rollback()
                    return SignInResult("already_signed", user_id)
                conn.execute(
                    "INSERT INTO sign_in_operations (operation_id, user_id, stone) "
                    "VALUES (%s, %s, %s)",
                    (operation_id, user_id, stone),
                )
                conn.commit()
                return SignInResult("signed", user_id, stone)
            except Exception:
                conn.rollback()
                raise


__all__ = ["SignInResult", "SignInService"]
