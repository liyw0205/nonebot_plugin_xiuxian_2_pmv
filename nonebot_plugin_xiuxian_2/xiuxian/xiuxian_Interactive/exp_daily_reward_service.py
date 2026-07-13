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
class InteractiveExpDailyRewardResult:
    status: str
    granted: bool = False
    exp_reward: int = 0
    exp: int = 0

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


class InteractiveExpDailyRewardService:
    """Atomically settle the daily interactive experience reward."""

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
    def _fixed_roll(operation_id: str) -> tuple[bool, float]:
        digest = hashlib.sha256(f"interactive-exp:{operation_id}".encode()).digest()
        granted = int.from_bytes(digest[:8], "big") < (1 << 63)
        ratio = 0.001 + int.from_bytes(digest[8:16], "big") / (1 << 64) * 0.008
        return granted, ratio

    def _checkpoint(self, name: str) -> None:
        if self._failure_hook is not None:
            self._failure_hook(name)

    def settle(
        self,
        operation_id,
        user_id,
        expected_exp,
        expected_level,
        rank_value,
        business_date,
    ) -> InteractiveExpDailyRewardResult:
        operation_id = str(operation_id).strip()
        user_id = str(user_id)
        expected_exp = int(expected_exp)
        expected_level = str(expected_level)
        rank_value = int(rank_value)
        business_date = self._business_date(business_date)
        if not operation_id or not user_id or expected_exp < 0 or rank_value < 0:
            raise ValueError("valid interactive experience reward settlement is required")

        granted, ratio = self._fixed_roll(operation_id)
        reward = int(expected_exp * ratio * min(0.1 * max(rank_value // 3, 1), 1)) if granted else 0
        payload = json.dumps(
            [user_id, expected_level, rank_value, business_date],
            ensure_ascii=True,
            separators=(",", ":"),
        )
        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS interactive_exp_daily_claims("
                    "user_id TEXT NOT NULL,business_date TEXT NOT NULL,operation_id TEXT NOT NULL,"
                    "exp_reward INTEGER NOT NULL,PRIMARY KEY(user_id,business_date))"
                )
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS interactive_exp_daily_reward_operations("
                    "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,granted INTEGER NOT NULL,"
                    "exp_reward INTEGER NOT NULL,exp INTEGER NOT NULL,"
                    "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
                )
                previous = conn.execute(
                    "SELECT payload,granted,exp_reward,exp FROM interactive_exp_daily_reward_operations "
                    "WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    if str(previous[0]) != payload:
                        return InteractiveExpDailyRewardResult("operation_conflict")
                    return InteractiveExpDailyRewardResult(
                        "duplicate", bool(previous[1]), int(previous[2]), int(previous[3])
                    )

                user = conn.execute(
                    "SELECT COALESCE(exp,0),level FROM user_xiuxian WHERE user_id=%s",
                    (user_id,),
                ).fetchone()
                if user is None:
                    conn.rollback()
                    return InteractiveExpDailyRewardResult("user_missing")
                if (int(user[0]), str(user[1])) != (expected_exp, expected_level):
                    conn.rollback()
                    return InteractiveExpDailyRewardResult("state_changed")
                if conn.execute(
                    "SELECT 1 FROM interactive_exp_daily_claims WHERE user_id=%s AND business_date=%s",
                    (user_id, business_date),
                ).fetchone():
                    conn.rollback()
                    return InteractiveExpDailyRewardResult("already_claimed")

                final_exp = expected_exp
                if granted:
                    final_exp += reward
                    changed = conn.execute(
                        "UPDATE user_xiuxian SET exp=%s WHERE user_id=%s AND COALESCE(exp,0)=%s AND level=%s",
                        (final_exp, user_id, expected_exp, expected_level),
                    )
                    if changed.rowcount != 1:
                        conn.rollback()
                        return InteractiveExpDailyRewardResult("state_changed")
                    conn.execute(
                        "INSERT INTO interactive_exp_daily_claims "
                        "(user_id,business_date,operation_id,exp_reward) VALUES (%s,%s,%s,%s)",
                        (user_id, business_date, operation_id, reward),
                    )
                    self._checkpoint("after_claim")

                conn.execute(
                    "INSERT INTO interactive_exp_daily_reward_operations "
                    "(operation_id,payload,granted,exp_reward,exp) VALUES (%s,%s,%s,%s,%s)",
                    (operation_id, payload, int(granted), reward, final_exp),
                )
                self._checkpoint("after_operation")
                conn.commit()
                return InteractiveExpDailyRewardResult("applied", granted, reward, final_exp)
            except Exception:
                conn.rollback()
                raise


__all__ = ["InteractiveExpDailyRewardResult", "InteractiveExpDailyRewardService"]
