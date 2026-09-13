from __future__ import annotations

from datetime import date, datetime
import random
from typing import Any

from ...infrastructure.clock import SystemClock
from ...infrastructure.database import DatabaseUnitOfWork
from .lottery import LotterySettlement, lottery_prize, lottery_tier
from .lottery_repository import LotteryRepository


class LotteryApplication:
    """New lottery use case for already migrated lottery tables."""

    def __init__(self, database: str, *, repository: LotteryRepository | None = None, clock: Any | None = None, random_source: Any | None = None) -> None:
        self.database = str(database)
        self.repository = repository or LotteryRepository()
        self.clock = clock or SystemClock()
        self.random_source = random_source or random

    def _now(self) -> datetime:
        value = self.clock.now() if hasattr(self.clock, "now") else self.clock()
        if not isinstance(value, datetime):
            raise TypeError("clock must return datetime")
        return value

    def settle(self, *, operation_id: str, user_id: str, user_name: str, business_date: str | date, deposit: int = 1_000_000, occurred_at: datetime | None = None) -> LotterySettlement:
        operation_id = str(operation_id).strip()
        user_id = str(user_id).strip()
        if not operation_id or not user_id or int(deposit) <= 0:
            raise ValueError("operation_id, user_id and positive deposit are required")
        occurred_at = self._now() if occurred_at is None else occurred_at
        if not isinstance(occurred_at, datetime):
            raise TypeError("occurred_at must be datetime")
        day = business_date.date().isoformat() if isinstance(business_date, datetime) else business_date.isoformat() if isinstance(business_date, date) else str(business_date)
        deposit = int(deposit)
        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            if not self.repository.schema_exists(uow):
                raise RuntimeError("lottery schema migration and legacy pool reconciliation are required")
            previous = self.repository.operation(uow, operation_id)
            if previous is not None:
                if previous.user_id != user_id or previous.deposit != deposit:
                    return LotterySettlement("operation_conflict", operation_id)
                return previous
            user = uow.query_one("SELECT COALESCE(user_name,?) AS user_name, COALESCE(stone,0) AS stone FROM user_xiuxian WHERE user_id=?", (user_name, user_id))
            if user is None:
                return LotterySettlement("user_missing", operation_id, user_id=user_id, user_name=user_name, business_date=day)
            if self.repository.participant(uow, day, user_id) is not None:
                return LotterySettlement("already_participated", operation_id, user_id=user_id, user_name=str(user["user_name"]), business_date=day, pool_before=self.repository.pool(uow), pool_after=self.repository.pool(uow), participants=self.repository.participant_count(uow, day), wallet_stone=int(user["stone"]))
            number = int(self.random_source.randint(1, 100000))
            if not 1 <= number <= 100000:
                raise ValueError("lottery number must be between 1 and 100000")
            before = self.repository.pool(uow)
            tier = lottery_tier(number)
            prize = lottery_prize(before + deposit, tier)
            after = before + deposit - prize
            wallet = int(user["stone"]) + prize
            if prize:
                changed = uow.execute("UPDATE user_xiuxian SET stone=COALESCE(stone,0)+? WHERE user_id=?", (prize, user_id))
                if changed.rowcount != 1:
                    raise RuntimeError("lottery wallet changed")
            participants = self.repository.participant_count(uow, day) + 1
            result = LotterySettlement("settled", operation_id, user_id, str(user["user_name"]), day, number, tier, prize, deposit, before, after, participants, wallet)
            self.repository.insert(uow, result, occurred_at.strftime("%Y-%m-%d %H:%M:%S"))
            return result


__all__ = ["LotteryApplication"]
