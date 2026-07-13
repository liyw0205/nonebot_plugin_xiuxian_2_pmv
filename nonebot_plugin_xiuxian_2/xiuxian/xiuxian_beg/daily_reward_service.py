from __future__ import annotations

import json
from contextlib import closing
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from threading import RLock
from typing import Callable

from ..xiuxian_utils import db_backend


@dataclass(frozen=True)
class BegDailyRewardResult:
    status: str
    stone_reward: int = 0
    stone: int = 0

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


class BegDailyRewardService:
    """Settle one daily beg reward after rechecking its complete state."""

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
    def _parse_datetime(value) -> datetime:
        if isinstance(value, datetime):
            return value
        text = str(value or "").strip()
        if not text:
            raise ValueError("create_time is required")
        try:
            return datetime.fromisoformat(text)
        except ValueError:
            for pattern in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S"):
                try:
                    return datetime.strptime(text, pattern)
                except ValueError:
                    continue
        raise ValueError("invalid create_time")

    @classmethod
    def _canonical_create_time(cls, value) -> str:
        return cls._parse_datetime(value).isoformat(sep=" ")

    @staticmethod
    def _normalize_optional(value):
        return None if value is None else str(value)

    def _checkpoint(self, name: str) -> None:
        if self._failure_hook is not None:
            self._failure_hook(name)

    def settle(
        self,
        operation_id,
        user_id,
        expected_create_time,
        expected_stone,
        expected_sect_id,
        expected_root_type,
        expected_level,
        settled_at,
        max_age_days,
        eligible_levels,
        stone_reward,
    ) -> BegDailyRewardResult:
        operation_id = str(operation_id).strip()
        user_id = str(user_id)
        expected_create_time = self._canonical_create_time(expected_create_time)
        expected_stone = int(expected_stone)
        expected_sect_id = self._normalize_optional(expected_sect_id)
        expected_root_type = str(expected_root_type)
        expected_level = str(expected_level)
        settled_at = self._parse_datetime(settled_at)
        max_age_days = int(max_age_days)
        eligible_levels = tuple(map(str, eligible_levels))
        stone_reward = int(stone_reward)
        if (
            not operation_id
            or expected_stone < 0
            or max_age_days < 0
            or stone_reward < 0
            or not eligible_levels
        ):
            raise ValueError("valid daily beg reward settlement is required")

        payload = json.dumps(
            [
                user_id,
                expected_create_time,
                expected_stone,
                expected_sect_id,
                expected_root_type,
                expected_level,
                settled_at.isoformat(),
                max_age_days,
                eligible_levels,
                stone_reward,
            ],
            ensure_ascii=True,
            separators=(",", ":"),
        )
        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS beg_daily_reward_operations ("
                    "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,"
                    "stone_reward INTEGER NOT NULL,stone INTEGER NOT NULL,"
                    "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
                )
                previous = conn.execute(
                    "SELECT payload,stone_reward,stone FROM beg_daily_reward_operations "
                    "WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous:
                    conn.rollback()
                    if str(previous[0]) != payload:
                        return BegDailyRewardResult("operation_conflict")
                    return BegDailyRewardResult(
                        "duplicate", int(previous[1]), int(previous[2])
                    )

                user = conn.execute(
                    "SELECT COALESCE(stone,0),create_time,COALESCE(is_beg,0),"
                    "sect_id,root_type,level FROM user_xiuxian WHERE user_id=%s",
                    (user_id,),
                ).fetchone()
                if user is None:
                    conn.rollback()
                    return BegDailyRewardResult("user_missing")

                actual_state = (
                    int(user[0]),
                    self._canonical_create_time(user[1]),
                    self._normalize_optional(user[3]),
                    str(user[4]),
                    str(user[5]),
                )
                expected_state = (
                    expected_stone,
                    expected_create_time,
                    expected_sect_id,
                    expected_root_type,
                    expected_level,
                )
                if actual_state != expected_state:
                    conn.rollback()
                    return BegDailyRewardResult("state_changed")
                if int(user[2]) != 0:
                    conn.rollback()
                    return BegDailyRewardResult("already_claimed")
                if expected_sect_id is not None and expected_root_type == "伪灵根":
                    conn.rollback()
                    return BegDailyRewardResult("ineligible_sect")
                if expected_root_type in {"轮回道果", "真·轮回道果"}:
                    conn.rollback()
                    return BegDailyRewardResult("ineligible_root")
                if expected_level not in eligible_levels:
                    conn.rollback()
                    return BegDailyRewardResult("ineligible_level")
                if (settled_at - self._parse_datetime(user[1])).days > max_age_days:
                    conn.rollback()
                    return BegDailyRewardResult("expired")

                final_stone = expected_stone + stone_reward
                changed = conn.execute(
                    "UPDATE user_xiuxian SET stone=%s,is_beg=1 "
                    "WHERE user_id=%s AND COALESCE(stone,0)=%s "
                    "AND COALESCE(is_beg,0)=0",
                    (final_stone, user_id, expected_stone),
                )
                if changed.rowcount != 1:
                    conn.rollback()
                    return BegDailyRewardResult("state_changed")
                self._checkpoint("after_user_update")
                conn.execute(
                    "INSERT INTO beg_daily_reward_operations "
                    "(operation_id,payload,stone_reward,stone) VALUES (%s,%s,%s,%s)",
                    (operation_id, payload, stone_reward, final_stone),
                )
                self._checkpoint("after_operation")
                conn.commit()
                return BegDailyRewardResult("applied", stone_reward, final_stone)
            except Exception:
                conn.rollback()
                raise


__all__ = ["BegDailyRewardResult", "BegDailyRewardService"]
