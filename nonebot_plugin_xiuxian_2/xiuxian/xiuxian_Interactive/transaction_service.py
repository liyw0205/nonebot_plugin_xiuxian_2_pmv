"""Compatibility facades for the migrated interactive transaction services.

The historical import path remains stable for one compatibility release.  The
actual writes now run through ``InteractiveRepository`` and
``DatabaseUnitOfWork``; this keeps the old method names while removing direct
``db_backend.connect`` ownership from the legacy package.
"""

from __future__ import annotations

from datetime import date, datetime
from pathlib import Path
from threading import RLock
from typing import Callable, Mapping

from ...features.interactive.domain import (
    DailyFortuneResult as InteractiveDailyFortuneResult,
    ExpRewardResult as InteractiveExpDailyRewardResult,
    GreetingClaimResult as InteractiveGreetingClaimResult,
    StoneRewardResult as InteractiveStoneDailyRewardResult,
)
from ...features.interactive.repository import InteractiveRepository
from ...infrastructure.database import DatabaseUnitOfWork


class InteractiveExpDailyRewardService:
    def __init__(self, database: str | Path, lock: RLock | None = None, failure_hook: Callable[[str], None] | None = None) -> None:
        self._database = Path(database)
        self._repository = InteractiveRepository(failure_hook)

    @staticmethod
    def _business_date(value: date | datetime | str) -> str:
        from ...features.interactive.domain import business_date

        return business_date(value)

    @staticmethod
    def _fixed_roll(operation_id: str) -> tuple[bool, float]:
        return InteractiveRepository.exp_fixed_roll(str(operation_id))

    def settle(self, operation_id, user_id, expected_exp, expected_level, rank_value, business_date):
        with DatabaseUnitOfWork(self._database, immediate=True) as uow:
            return self._repository.settle_exp(
                uow,
                operation_id=operation_id,
                user_id=user_id,
                expected_exp=expected_exp,
                expected_level=expected_level,
                rank_value=rank_value,
                business_day=business_date,
            )


class InteractiveStoneDailyRewardService:
    def __init__(self, database: str | Path, lock: RLock | None = None, failure_hook: Callable[[str], None] | None = None) -> None:
        self._database = Path(database)
        self._repository = InteractiveRepository(failure_hook)

    @staticmethod
    def _business_date(value: date | datetime | str) -> str:
        from ...features.interactive.domain import business_date

        return business_date(value)

    @staticmethod
    def _fixed_roll(operation_id: str) -> tuple[bool, int]:
        return InteractiveRepository.stone_fixed_roll(str(operation_id))

    def settle(self, operation_id, user_id, expected_stone, business_date):
        with DatabaseUnitOfWork(self._database, immediate=True) as uow:
            return self._repository.settle_stone(
                uow,
                operation_id=operation_id,
                user_id=user_id,
                expected_stone=expected_stone,
                business_day=business_date,
            )


class InteractiveGreetingClaimService:
    def __init__(self, database: str | Path, lock: RLock | None = None) -> None:
        self._database = Path(database)
        self._repository = InteractiveRepository()

    def _ensure_schema(self, conn) -> None:
        # Kept for historical callers that prepare triggers before a claim.
        self._repository.ensure_schema(conn)

    @staticmethod
    def _business_date(value: date | datetime | str) -> str:
        from ...features.interactive.domain import business_date

        return business_date(value)

    def claim(self, operation_id, user_id, kind, business_date):
        with DatabaseUnitOfWork(self._database, immediate=True) as uow:
            return self._repository.claim_greeting(
                uow,
                operation_id=operation_id,
                user_id=user_id,
                kind=kind,
                business_day=business_date,
            )

    def cleanup_before(self, cutoff: date | datetime | str) -> int:
        with DatabaseUnitOfWork(self._database, immediate=True) as uow:
            return self._repository.cleanup_greeting(uow, cutoff)


class InteractiveDailyFortuneService:
    def __init__(self, database: str | Path, lock: RLock | None = None) -> None:
        self._database = Path(database)
        self._repository = InteractiveRepository()

    def _ensure_schema(self, conn) -> None:
        self._repository.ensure_schema(conn)

    @staticmethod
    def _business_date(value: date | datetime | str) -> str:
        from ...features.interactive.domain import business_date

        return business_date(value)

    def resolve(self, operation_id, user_id, business_date, create_fortune: Callable[[], Mapping[str, str]]):
        with DatabaseUnitOfWork(self._database, immediate=True) as uow:
            return self._repository.resolve_fortune(
                uow,
                operation_id=operation_id,
                user_id=user_id,
                business_day=business_date,
                create_fortune=create_fortune,
            )

    def cleanup_before(self, cutoff: date | datetime | str) -> int:
        with DatabaseUnitOfWork(self._database, immediate=True) as uow:
            return self._repository.cleanup_fortune(uow, cutoff)


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
