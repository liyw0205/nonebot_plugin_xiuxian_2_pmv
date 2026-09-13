from __future__ import annotations

import json
from datetime import datetime, timedelta
from typing import Any, Callable, Iterable

from ...infrastructure.database import DatabaseUnitOfWork
from .domain import (
    BegDailyRewardResult,
    NoviceGiftClaimResult,
    canonical_datetime,
    normalize_optional,
    normalized_rewards,
    parse_datetime,
)


class BegRepository:
    """Transactional projections for the daily reward and novice gift."""

    def __init__(
        self,
        *,
        failure_hook: Callable[[str], None] | None = None,
        now: Callable[[], datetime] | None = None,
    ) -> None:
        self.failure_hook = failure_hook
        self.now = now or datetime.now

    def _checkpoint(self, name: str) -> None:
        if self.failure_hook is not None:
            self.failure_hook(name)

    @staticmethod
    def ensure_schema(uow: DatabaseUnitOfWork) -> None:
        uow.execute(
            "CREATE TABLE IF NOT EXISTS beg_daily_reward_operations ("
            "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,"
            "stone_reward INTEGER NOT NULL,stone INTEGER NOT NULL,"
            "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
        )
        uow.execute(
            "CREATE TABLE IF NOT EXISTS novice_gift_claim_operations ("
            "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,stone INTEGER NOT NULL,"
            "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
        )

    def daily_result(self, uow: DatabaseUnitOfWork, operation_id: str) -> BegDailyRewardResult | None:
        self.ensure_schema(uow)
        row = uow.query_one(
            "SELECT stone_reward,stone FROM beg_daily_reward_operations WHERE operation_id = ?",
            (str(operation_id).strip(),),
        )
        if row is None:
            return None
        return BegDailyRewardResult("duplicate", int(row["stone_reward"]), int(row["stone"]))

    def novice_result(self, uow: DatabaseUnitOfWork, operation_id: str) -> NoviceGiftClaimResult | None:
        self.ensure_schema(uow)
        row = uow.query_one(
            "SELECT stone FROM novice_gift_claim_operations WHERE operation_id = ?",
            (str(operation_id).strip(),),
        )
        if row is None:
            return None
        return NoviceGiftClaimResult("duplicate", int(row["stone"]))

    def settle_daily(
        self,
        uow: DatabaseUnitOfWork,
        *,
        operation_id: str,
        user_id: str,
        expected_create_time: Any,
        expected_stone: int,
        expected_sect_id: Any,
        expected_root_type: str,
        expected_level: str,
        settled_at: Any,
        max_age_days: int,
        eligible_levels: Iterable[str],
        stone_reward: int,
    ) -> BegDailyRewardResult:
        self.ensure_schema(uow)
        operation_id, user_id = str(operation_id).strip(), str(user_id)
        expected_create_time = canonical_datetime(expected_create_time)
        expected_stone = int(expected_stone)
        expected_sect_id = normalize_optional(expected_sect_id)
        expected_root_type = str(expected_root_type)
        expected_level = str(expected_level)
        settled_at = parse_datetime(settled_at)
        max_age_days = int(max_age_days)
        eligible_levels = tuple(map(str, eligible_levels))
        stone_reward = int(stone_reward)
        if not operation_id or expected_stone < 0 or max_age_days < 0 or stone_reward < 0 or not eligible_levels:
            raise ValueError("valid daily beg reward settlement is required")

        payload = json.dumps([user_id], ensure_ascii=True, separators=(",", ":"))
        previous = uow.query_one(
            "SELECT payload,stone_reward,stone FROM beg_daily_reward_operations WHERE operation_id = ?",
            (operation_id,),
        )
        if previous is not None:
            if str(previous["payload"]) != payload:
                return BegDailyRewardResult("operation_conflict")
            return BegDailyRewardResult("duplicate", int(previous["stone_reward"]), int(previous["stone"]))

        user = uow.query_one(
            "SELECT COALESCE(stone,0) AS stone,create_time,COALESCE(is_beg,0) AS is_beg,"
            "sect_id,root_type,level FROM user_xiuxian WHERE user_id = ?",
            (user_id,),
        )
        if user is None:
            return BegDailyRewardResult("user_missing")
        actual_state = (
            int(user["stone"]),
            canonical_datetime(user["create_time"]),
            normalize_optional(user["sect_id"]),
            str(user["root_type"]),
            str(user["level"]),
        )
        expected_state = (expected_stone, expected_create_time, expected_sect_id, expected_root_type, expected_level)
        if actual_state != expected_state:
            return BegDailyRewardResult("state_changed")
        if int(user["is_beg"]) != 0:
            return BegDailyRewardResult("already_claimed")
        if expected_sect_id is not None and expected_root_type == "伪灵根":
            return BegDailyRewardResult("ineligible_sect")
        if expected_root_type in {"轮回道果", "真·轮回道果"}:
            return BegDailyRewardResult("ineligible_root")
        if expected_level not in eligible_levels:
            return BegDailyRewardResult("ineligible_level")
        if (settled_at - parse_datetime(user["create_time"])).days > max_age_days:
            return BegDailyRewardResult("expired")

        final_stone = expected_stone + stone_reward
        changed = uow.execute(
            "UPDATE user_xiuxian SET stone = ?,is_beg = 1 "
            "WHERE user_id = ? AND COALESCE(stone,0) = ? AND COALESCE(is_beg,0) = 0",
            (final_stone, user_id, expected_stone),
        )
        if changed.rowcount < 1:
            return BegDailyRewardResult("state_changed")
        self._checkpoint("after_user_update")
        uow.execute(
            "INSERT INTO beg_daily_reward_operations(operation_id,payload,stone_reward,stone) VALUES (?,?,?,?)",
            (operation_id, payload, stone_reward, final_stone),
        )
        self._checkpoint("after_operation")
        return BegDailyRewardResult("applied", stone_reward, final_stone)

    def claim_novice(
        self,
        uow: DatabaseUnitOfWork,
        *,
        operation_id: str,
        user_id: str,
        expected_create_time: Any,
        claimed_at: Any,
        max_age_days: int,
        stone: int,
        rewards: Iterable[dict[str, Any]],
        max_goods_num: int,
    ) -> NoviceGiftClaimResult:
        self.ensure_schema(uow)
        operation_id, user_id = str(operation_id).strip(), str(user_id)
        expected_create_time = canonical_datetime(expected_create_time)
        claimed_at = parse_datetime(claimed_at)
        max_age_days, stone, max_goods_num = map(int, (max_age_days, stone, max_goods_num))
        reward_rows = normalized_rewards(rewards)
        if not operation_id or max_age_days < 0 or stone < 0 or max_goods_num < 0:
            raise ValueError("valid novice gift claim is required")

        payload = json.dumps([user_id], ensure_ascii=True, separators=(",", ":"))
        previous = uow.query_one(
            "SELECT payload,stone FROM novice_gift_claim_operations WHERE operation_id = ?",
            (operation_id,),
        )
        if previous is not None:
            status = "duplicate" if str(previous["payload"]) == payload else "operation_conflict"
            return NoviceGiftClaimResult(status, int(previous["stone"]))

        user = uow.query_one(
            "SELECT create_time,COALESCE(is_novice,0) AS is_novice FROM user_xiuxian WHERE user_id = ?",
            (user_id,),
        )
        if user is None:
            return NoviceGiftClaimResult("user_missing")
        actual_create_time = canonical_datetime(user["create_time"])
        if actual_create_time != expected_create_time:
            return NoviceGiftClaimResult("state_changed")
        if int(user["is_novice"]) != 0:
            return NoviceGiftClaimResult("already_claimed")
        if claimed_at > parse_datetime(user["create_time"]) + timedelta(days=max_age_days):
            return NoviceGiftClaimResult("expired")

        for item_id, _, _, amount in reward_rows:
            current = uow.query_one(
                "SELECT COALESCE(goods_num,0) AS goods_num FROM back WHERE user_id = ? AND goods_id = ?",
                (user_id, item_id),
            )
            if (int(current["goods_num"]) if current else 0) + amount > max_goods_num:
                return NoviceGiftClaimResult("inventory_full")

        changed = uow.execute(
            "UPDATE user_xiuxian SET stone = CAST(COALESCE(stone,0) AS REAL) + CAST(? AS REAL),is_novice = 1 "
            "WHERE user_id = ? AND COALESCE(is_novice,0) = 0 AND create_time = ?",
            (stone, user_id, user["create_time"]),
        )
        if changed.rowcount < 1:
            return NoviceGiftClaimResult("state_changed")
        self._checkpoint("after_user_update")

        now = self.now()
        for item_id, name, item_type, amount in reward_rows:
            uow.execute(
                "INSERT INTO back(user_id,goods_id,goods_name,goods_type,goods_num,create_time,update_time,bind_num) "
                "VALUES (?,?,?,?,?,?,?,?) ON CONFLICT(user_id,goods_id) DO UPDATE SET "
                "goods_name = excluded.goods_name,goods_type = excluded.goods_type,"
                "goods_num = back.goods_num + excluded.goods_num,"
                "bind_num = COALESCE(back.bind_num,0) + excluded.bind_num,update_time = excluded.update_time",
                (user_id, item_id, name, item_type, amount, now, now, amount),
            )
        self._checkpoint("after_rewards")
        uow.execute(
            "INSERT INTO novice_gift_claim_operations(operation_id,payload,stone) VALUES (?,?,?)",
            (operation_id, payload, stone),
        )
        return NoviceGiftClaimResult("applied", stone)


LegacyBegRepository = BegRepository

__all__ = ["BegRepository", "LegacyBegRepository"]
