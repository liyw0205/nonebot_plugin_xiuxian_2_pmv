from __future__ import annotations

import hashlib
import json
from datetime import date, datetime
from typing import Any, Callable, Mapping

from ...infrastructure.database import DatabaseUnitOfWork
from .domain import DailyFortuneResult, ExpRewardResult, GreetingClaimResult, StoneRewardResult, business_date


class InteractiveRepository:
    """SQLite projections for daily interactive rewards and greetings."""

    def __init__(self, failure_hook: Callable[[str], None] | None = None) -> None:
        self.failure_hook = failure_hook

    def _checkpoint(self, name: str) -> None:
        if self.failure_hook:
            self.failure_hook(name)

    def ensure_schema(self, db: Any) -> None:
        statements = (
            "CREATE TABLE IF NOT EXISTS interactive_exp_daily_claims(user_id TEXT NOT NULL,business_date TEXT NOT NULL,operation_id TEXT NOT NULL,exp_reward INTEGER NOT NULL,PRIMARY KEY(user_id,business_date))",
            "CREATE TABLE IF NOT EXISTS interactive_exp_daily_reward_operations(operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,granted INTEGER NOT NULL,exp_reward INTEGER NOT NULL,exp INTEGER NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)",
            "CREATE TABLE IF NOT EXISTS interactive_stone_daily_claims(user_id TEXT NOT NULL,business_date TEXT NOT NULL,operation_id TEXT NOT NULL,stone_reward INTEGER NOT NULL,PRIMARY KEY(user_id,business_date))",
            "CREATE TABLE IF NOT EXISTS interactive_stone_daily_reward_operations(operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,granted INTEGER NOT NULL,stone_reward INTEGER NOT NULL,stone INTEGER NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)",
            "CREATE TABLE IF NOT EXISTS interactive_greeting_claims(kind TEXT NOT NULL,business_date TEXT NOT NULL,user_id TEXT NOT NULL,position INTEGER NOT NULL,operation_id TEXT NOT NULL,PRIMARY KEY(kind,business_date,user_id),UNIQUE(kind,business_date,position))",
            "CREATE TABLE IF NOT EXISTS interactive_greeting_operations(operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,business_date TEXT NOT NULL,claimed INTEGER NOT NULL,position INTEGER NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)",
            "CREATE TABLE IF NOT EXISTS interactive_daily_fortunes(user_id TEXT NOT NULL,business_date TEXT NOT NULL,fortune_type TEXT NOT NULL,description TEXT NOT NULL,stars TEXT NOT NULL,operation_id TEXT NOT NULL,PRIMARY KEY(user_id,business_date))",
            "CREATE TABLE IF NOT EXISTS interactive_daily_fortune_operations(operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,business_date TEXT NOT NULL,fortune_type TEXT NOT NULL,description TEXT NOT NULL,stars TEXT NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)",
        )
        for statement in statements:
            db.execute(statement)

    @staticmethod
    def exp_fixed_roll(operation_id: str) -> tuple[bool, float]:
        digest = hashlib.sha256(f"interactive-exp:{operation_id}".encode()).digest()
        return int.from_bytes(digest[:8], "big") < (1 << 63), 0.001 + int.from_bytes(digest[8:16], "big") / (1 << 64) * 0.008

    @staticmethod
    def stone_fixed_roll(operation_id: str) -> tuple[bool, int]:
        digest = hashlib.sha256(f"interactive-stone:{operation_id}".encode()).digest()
        granted = int.from_bytes(digest[:8], "big") < (1 << 63)
        reward = 1_000_000 + int.from_bytes(digest[8:16], "big") % 4_000_001
        return granted, reward if granted else 0

    def settle_exp(self, uow: DatabaseUnitOfWork, *, operation_id: str, user_id: str, expected_exp: int, expected_level: str, rank_value: int, business_day: date | datetime | str) -> ExpRewardResult:
        self.ensure_schema(uow)
        operation_id, user_id = str(operation_id).strip(), str(user_id)
        expected_exp, expected_level, rank_value = int(expected_exp), str(expected_level), int(rank_value)
        day = business_date(business_day)
        if not operation_id or not user_id or expected_exp < 0 or rank_value < 0:
            raise ValueError("valid interactive experience reward settlement is required")
        granted, ratio = self.exp_fixed_roll(operation_id)
        reward = int(expected_exp * ratio * min(0.1 * max(rank_value // 3, 1), 1)) if granted else 0
        payload = json.dumps([user_id, expected_level, rank_value, day], ensure_ascii=True, separators=(",", ":"))
        previous = uow.query_one("SELECT payload,granted,exp_reward,exp FROM interactive_exp_daily_reward_operations WHERE operation_id = ?", (operation_id,))
        if previous:
            if str(previous["payload"]) != payload:
                return ExpRewardResult("operation_conflict")
            return ExpRewardResult("duplicate", bool(previous["granted"]), int(previous["exp_reward"]), int(previous["exp"]))
        user = uow.query_one("SELECT COALESCE(exp,0) AS exp,level FROM user_xiuxian WHERE user_id = ?", (user_id,))
        if user is None:
            return ExpRewardResult("user_missing")
        if (int(user["exp"]), str(user["level"])) != (expected_exp, expected_level):
            return ExpRewardResult("state_changed")
        if uow.query_one("SELECT 1 AS present FROM interactive_exp_daily_claims WHERE user_id = ? AND business_date = ?", (user_id, day)):
            return ExpRewardResult("already_claimed")
        final_exp = expected_exp
        if granted:
            final_exp += reward
            changed = uow.execute("UPDATE user_xiuxian SET exp = ? WHERE user_id = ? AND COALESCE(exp,0) = ? AND level = ?", (final_exp, user_id, expected_exp, expected_level))
            if changed.rowcount != 1:
                return ExpRewardResult("state_changed")
            uow.execute("INSERT INTO interactive_exp_daily_claims(user_id,business_date,operation_id,exp_reward) VALUES (?,?,?,?)", (user_id, day, operation_id, reward))
            self._checkpoint("after_claim")
        uow.execute("INSERT INTO interactive_exp_daily_reward_operations(operation_id,payload,granted,exp_reward,exp) VALUES (?,?,?,?,?)", (operation_id, payload, int(granted), reward, final_exp))
        self._checkpoint("after_operation")
        return ExpRewardResult("applied", granted, reward, final_exp)

    def settle_stone(self, uow: DatabaseUnitOfWork, *, operation_id: str, user_id: str, expected_stone: int, business_day: date | datetime | str) -> StoneRewardResult:
        self.ensure_schema(uow)
        operation_id, user_id = str(operation_id).strip(), str(user_id)
        expected_stone, day = int(expected_stone), business_date(business_day)
        if not operation_id or not user_id or expected_stone < 0:
            raise ValueError("valid interactive stone reward settlement is required")
        granted, reward = self.stone_fixed_roll(operation_id)
        payload = json.dumps([user_id, day], ensure_ascii=True, separators=(",", ":"))
        previous = uow.query_one("SELECT payload,granted,stone_reward,stone FROM interactive_stone_daily_reward_operations WHERE operation_id = ?", (operation_id,))
        if previous:
            if str(previous["payload"]) != payload:
                return StoneRewardResult("operation_conflict")
            return StoneRewardResult("duplicate", bool(previous["granted"]), int(previous["stone_reward"]), int(previous["stone"]))
        user = uow.query_one("SELECT COALESCE(stone,0) AS stone FROM user_xiuxian WHERE user_id = ?", (user_id,))
        if user is None:
            return StoneRewardResult("user_missing")
        if int(user["stone"]) != expected_stone:
            return StoneRewardResult("state_changed")
        if uow.query_one("SELECT 1 AS present FROM interactive_stone_daily_claims WHERE user_id = ? AND business_date = ?", (user_id, day)):
            return StoneRewardResult("already_claimed")
        final_stone = expected_stone
        if granted:
            final_stone += reward
            changed = uow.execute("UPDATE user_xiuxian SET stone = ? WHERE user_id = ? AND COALESCE(stone,0) = ?", (final_stone, user_id, expected_stone))
            if changed.rowcount != 1:
                return StoneRewardResult("state_changed")
            uow.execute("INSERT INTO interactive_stone_daily_claims(user_id,business_date,operation_id,stone_reward) VALUES (?,?,?,?)", (user_id, day, operation_id, reward))
            self._checkpoint("after_claim")
        uow.execute("INSERT INTO interactive_stone_daily_reward_operations(operation_id,payload,granted,stone_reward,stone) VALUES (?,?,?,?,?)", (operation_id, payload, int(granted), reward, final_stone))
        self._checkpoint("after_operation")
        return StoneRewardResult("applied", granted, reward, final_stone)

    def claim_greeting(self, uow: DatabaseUnitOfWork, *, operation_id: str, user_id: str, kind: str, business_day: date | datetime | str) -> GreetingClaimResult:
        self.ensure_schema(uow)
        operation_id, user_id, kind = str(operation_id).strip(), str(user_id).strip(), str(kind).strip().lower()
        day = business_date(business_day)
        if not operation_id or not user_id or kind not in {"morning", "night"}:
            raise ValueError("valid greeting claim is required")
        payload = json.dumps([user_id, kind, day], ensure_ascii=True, separators=(",", ":"))
        previous = uow.query_one("SELECT payload,claimed,position FROM interactive_greeting_operations WHERE operation_id = ?", (operation_id,))
        if previous:
            if str(previous["payload"]) != payload:
                return GreetingClaimResult("operation_conflict")
            return GreetingClaimResult("duplicate", kind, day, bool(previous["claimed"]), int(previous["position"]))
        if uow.query_one("SELECT 1 AS present FROM user_xiuxian WHERE user_id = ?", (user_id,)) is None:
            return GreetingClaimResult("user_missing", kind, day)
        existing = uow.query_one("SELECT position FROM interactive_greeting_claims WHERE kind = ? AND business_date = ? AND user_id = ?", (kind, day, user_id))
        if existing:
            position = int(existing["position"])
            uow.execute("INSERT INTO interactive_greeting_operations(operation_id,payload,business_date,claimed,position) VALUES (?,?,?,?,?)", (operation_id, payload, day, 0, position))
            return GreetingClaimResult("already_claimed", kind, day, False, position)
        row = uow.query_one("SELECT COALESCE(MAX(position),0) AS position FROM interactive_greeting_claims WHERE kind = ? AND business_date = ?", (kind, day))
        position = int(row["position"]) + 1
        uow.execute("INSERT INTO interactive_greeting_claims(kind,business_date,user_id,position,operation_id) VALUES (?,?,?,?,?)", (kind, day, user_id, position, operation_id))
        uow.execute("INSERT INTO interactive_greeting_operations(operation_id,payload,business_date,claimed,position) VALUES (?,?,?,?,?)", (operation_id, payload, day, 1, position))
        return GreetingClaimResult("claimed", kind, day, True, position)

    def cleanup_greeting(self, uow: DatabaseUnitOfWork, cutoff: date | datetime | str) -> int:
        self.ensure_schema(uow)
        day = business_date(cutoff)
        return int(uow.execute("DELETE FROM interactive_greeting_operations WHERE business_date < ?", (day,)).rowcount) + int(uow.execute("DELETE FROM interactive_greeting_claims WHERE business_date < ?", (day,)).rowcount)

    @staticmethod
    def _fortune(value: Any) -> tuple[str, str, str]:
        if not isinstance(value, Mapping):
            raise ValueError("fortune factory must return a mapping")
        result = tuple(str(value.get(key, "")).strip() for key in ("type", "description", "stars"))
        if not all(result):
            raise ValueError("fortune fields are required")
        return result  # type: ignore[return-value]

    @staticmethod
    def _fortune_result(status: str, day: str, values: Any) -> DailyFortuneResult:
        return DailyFortuneResult(status, day, str(values[0]), str(values[1]), str(values[2]))

    def resolve_fortune(self, uow: DatabaseUnitOfWork, *, operation_id: str, user_id: str, business_day: date | datetime | str, create_fortune: Callable[[], Mapping[str, str]]) -> DailyFortuneResult:
        self.ensure_schema(uow)
        operation_id, user_id, day = str(operation_id).strip(), str(user_id).strip(), business_date(business_day)
        if not operation_id or not user_id or not callable(create_fortune):
            raise ValueError("valid daily fortune request is required")
        payload = json.dumps([user_id, day], ensure_ascii=True, separators=(",", ":"))
        previous = uow.query_one("SELECT payload,fortune_type,description,stars FROM interactive_daily_fortune_operations WHERE operation_id = ?", (operation_id,))
        if previous:
            if str(previous["payload"]) != payload:
                return DailyFortuneResult("operation_conflict")
            return self._fortune_result("duplicate", day, (previous["fortune_type"], previous["description"], previous["stars"]))
        if uow.query_one("SELECT 1 AS present FROM user_xiuxian WHERE user_id = ?", (user_id,)) is None:
            return DailyFortuneResult("user_missing", day)
        existing = uow.query_one("SELECT fortune_type,description,stars FROM interactive_daily_fortunes WHERE user_id = ? AND business_date = ?", (user_id, day))
        if existing:
            values = (existing["fortune_type"], existing["description"], existing["stars"])
            uow.execute("INSERT INTO interactive_daily_fortune_operations(operation_id,payload,business_date,fortune_type,description,stars) VALUES (?,?,?,?,?,?)", (operation_id, payload, day, *values))
            return self._fortune_result("existing", day, values)
        fortune = self._fortune(create_fortune())
        uow.execute("INSERT INTO interactive_daily_fortunes(user_id,business_date,fortune_type,description,stars,operation_id) VALUES (?,?,?,?,?,?)", (user_id, day, *fortune, operation_id))
        uow.execute("INSERT INTO interactive_daily_fortune_operations(operation_id,payload,business_date,fortune_type,description,stars) VALUES (?,?,?,?,?,?)", (operation_id, payload, day, *fortune))
        return self._fortune_result("generated", day, fortune)

    def cleanup_fortune(self, uow: DatabaseUnitOfWork, cutoff: date | datetime | str) -> int:
        self.ensure_schema(uow)
        day = business_date(cutoff)
        return int(uow.execute("DELETE FROM interactive_daily_fortune_operations WHERE business_date < ?", (day,)).rowcount) + int(uow.execute("DELETE FROM interactive_daily_fortunes WHERE business_date < ?", (day,)).rowcount)


LegacyInteractiveRepository = InteractiveRepository
__all__ = ["InteractiveRepository", "LegacyInteractiveRepository"]
