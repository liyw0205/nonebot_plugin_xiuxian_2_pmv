from __future__ import annotations

from pathlib import Path
from threading import RLock
from typing import Any, Callable, Iterable

from ...features.beg.domain import BegDailyRewardResult, NoviceGiftClaimResult
from ...features.beg.repository import BegRepository
from ...infrastructure.database import DatabaseUnitOfWork


class BegDailyRewardService:
    """Compatibility facade for the historical daily reward import path."""

    def __init__(
        self,
        database: str | Path,
        lock: RLock | None = None,
        failure_hook: Callable[[str], None] | None = None,
    ) -> None:
        self._database = Path(database)
        self._repository = BegRepository(failure_hook=failure_hook)
        self._lock = lock or RLock()

    def get_result(self, operation_id: str) -> BegDailyRewardResult | None:
        with self._lock, DatabaseUnitOfWork(self._database) as uow:
            return self._repository.daily_result(uow, operation_id)

    def settle(
        self,
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
        with self._lock, DatabaseUnitOfWork(self._database, immediate=True) as uow:
            return self._repository.settle_daily(
                uow,
                operation_id=operation_id,
                user_id=user_id,
                expected_create_time=expected_create_time,
                expected_stone=expected_stone,
                expected_sect_id=expected_sect_id,
                expected_root_type=expected_root_type,
                expected_level=expected_level,
                settled_at=settled_at,
                max_age_days=max_age_days,
                eligible_levels=eligible_levels,
                stone_reward=stone_reward,
            )


class NoviceGiftClaimService:
    """Compatibility facade for the historical novice gift import path."""

    def __init__(
        self,
        database: str | Path,
        lock: RLock | None = None,
        failure_hook: Callable[[str], None] | None = None,
    ) -> None:
        self._database = Path(database)
        self._repository = BegRepository(failure_hook=failure_hook)
        self._lock = lock or RLock()

    def get_result(self, operation_id: str) -> NoviceGiftClaimResult | None:
        with self._lock, DatabaseUnitOfWork(self._database) as uow:
            return self._repository.novice_result(uow, operation_id)

    def claim(
        self,
        operation_id: str,
        user_id: str,
        expected_create_time: Any,
        claimed_at: Any,
        max_age_days: int,
        stone: int,
        rewards: Iterable[dict[str, Any]],
        max_goods_num: int,
    ) -> NoviceGiftClaimResult:
        with self._lock, DatabaseUnitOfWork(self._database, immediate=True) as uow:
            return self._repository.claim_novice(
                uow,
                operation_id=operation_id,
                user_id=user_id,
                expected_create_time=expected_create_time,
                claimed_at=claimed_at,
                max_age_days=max_age_days,
                stone=stone,
                rewards=rewards,
                max_goods_num=max_goods_num,
            )


__all__ = [
    "BegDailyRewardResult",
    "BegDailyRewardService",
    "NoviceGiftClaimResult",
    "NoviceGiftClaimService",
]
