from __future__ import annotations

import json
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from threading import RLock
import hashlib
from datetime import date, datetime
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

__all__ = [
    "InteractiveExpDailyRewardResult",
    "InteractiveExpDailyRewardService",
    "InteractiveStoneDailyRewardResult",
    "InteractiveStoneDailyRewardService",
    "InteractiveGreetingClaimResult",
    "InteractiveGreetingClaimService",
    "InteractiveDailyFortuneResult",
    "InteractiveDailyFortuneService",
]
