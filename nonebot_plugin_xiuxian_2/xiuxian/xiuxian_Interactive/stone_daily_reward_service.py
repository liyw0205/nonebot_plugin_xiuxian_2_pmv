from __future__ import annotations

import hashlib
import json
from contextlib import closing
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from threading import RLock
from typing import Callable

from ..xiuxian_utils import db_backend


@dataclass(frozen=True)
class InteractiveStoneDailyRewardResult:
    status: str
    granted: bool = False
    stone_reward: int = 0
    stone: int = 0

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


class InteractiveStoneDailyRewardService:
    """Atomically settle the daily interactive stone reward."""

    def __init__(
        self,
        database: str | Path,
        lock: RLock | None = None,
        failure_hook: Callable[[str], None] | None = None,
    ) -> None:
        self._database = Path(database)
        self._lock = lock or RLock()
        self._failure_hook = failure_hook

    @staticmethod
    def _business_date(value: date | datetime | str) -> str:
        if isinstance(value, datetime):
            return value.date().isoformat()
        if isinstance(value, date):
            return value.isoformat()
        return date.fromisoformat(str(value)).isoformat()

    @staticmethod
    def _fixed_roll(operation_id: str) -> tuple[bool, int]:
        digest = hashlib.sha256(f"interactive-stone:{operation_id}".encode()).digest()
        granted = int.from_bytes(digest[:8], "big") < (1 << 63)
        reward = 1_000_000 + int.from_bytes(digest[8:16], "big") % 4_000_001
        return granted, reward if granted else 0

    def _checkpoint(self, name: str) -> None:
        if self._failure_hook is not None:
            self._failure_hook(name)

    def settle(
        self,
        operation_id,
        user_id,
        expected_stone,
        business_date,
    ) -> InteractiveStoneDailyRewardResult:
        operation_id = str(operation_id).strip()
        user_id = str(user_id)
        expected_stone = int(expected_stone)
        business_date = self._business_date(business_date)
        if not operation_id or not user_id or expected_stone < 0:
            raise ValueError("valid interactive stone reward settlement is required")

        granted, reward = self._fixed_roll(operation_id)
        payload = json.dumps(
            [user_id, business_date],
            ensure_ascii=True,
            separators=(",", ":"),
        )
        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS interactive_stone_daily_claims("
                    "user_id TEXT NOT NULL,business_date TEXT NOT NULL,operation_id TEXT NOT NULL,"
                    "stone_reward INTEGER NOT NULL,PRIMARY KEY(user_id,business_date))"
                )
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS interactive_stone_daily_reward_operations("
                    "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,granted INTEGER NOT NULL,"
                    "stone_reward INTEGER NOT NULL,stone INTEGER NOT NULL,"
                    "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
                )
                previous = conn.execute(
                    "SELECT payload,granted,stone_reward,stone "
                    "FROM interactive_stone_daily_reward_operations WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    if str(previous[0]) != payload:
                        return InteractiveStoneDailyRewardResult("operation_conflict")
                    return InteractiveStoneDailyRewardResult(
                        "duplicate", bool(previous[1]), int(previous[2]), int(previous[3])
                    )

                user = conn.execute(
                    "SELECT COALESCE(stone,0) FROM user_xiuxian WHERE user_id=%s", (user_id,)
                ).fetchone()
                if user is None:
                    conn.rollback()
                    return InteractiveStoneDailyRewardResult("user_missing")
                if int(user[0]) != expected_stone:
                    conn.rollback()
                    return InteractiveStoneDailyRewardResult("state_changed")
                if conn.execute(
                    "SELECT 1 FROM interactive_stone_daily_claims WHERE user_id=%s AND business_date=%s",
                    (user_id, business_date),
                ).fetchone():
                    conn.rollback()
                    return InteractiveStoneDailyRewardResult("already_claimed")

                final_stone = expected_stone
                if granted:
                    final_stone += reward
                    changed = conn.execute(
                        "UPDATE user_xiuxian SET stone=%s WHERE user_id=%s AND COALESCE(stone,0)=%s",
                        (final_stone, user_id, expected_stone),
                    )
                    if changed.rowcount != 1:
                        conn.rollback()
                        return InteractiveStoneDailyRewardResult("state_changed")
                    conn.execute(
                        "INSERT INTO interactive_stone_daily_claims "
                        "(user_id,business_date,operation_id,stone_reward) VALUES (%s,%s,%s,%s)",
                        (user_id, business_date, operation_id, reward),
                    )
                    self._checkpoint("after_claim")

                conn.execute(
                    "INSERT INTO interactive_stone_daily_reward_operations "
                    "(operation_id,payload,granted,stone_reward,stone) VALUES (%s,%s,%s,%s,%s)",
                    (operation_id, payload, int(granted), reward, final_stone),
                )
                self._checkpoint("after_operation")
                conn.commit()
                return InteractiveStoneDailyRewardResult("applied", granted, reward, final_stone)
            except Exception:
                conn.rollback()
                raise


__all__ = ["InteractiveStoneDailyRewardResult", "InteractiveStoneDailyRewardService"]
