from __future__ import annotations

from ...infrastructure.database import DatabaseUnitOfWork
from .repository import SignInRepository
from .statistics import SignInStatisticsRepository


def apply_sign_in(uow: DatabaseUnitOfWork) -> None:
    SignInRepository().ensure_schema(uow)


def apply_sign_in_statistics(uow: DatabaseUnitOfWork) -> None:
    # The repository is also responsible for the projection shape; this
    # migration only creates tables and never backfills legacy JSON counts.
    SignInStatisticsRepository.ensure_schema(uow)


def apply_sign_in_tasks(uow: DatabaseUnitOfWork) -> None:
    from .tasks import SignInTaskRepository

    SignInTaskRepository.ensure_schema(uow)


def apply_lottery(uow: DatabaseUnitOfWork) -> None:
    from .lottery_repository import LotteryRepository

    LotteryRepository.ensure_schema(uow)


__all__ = ["apply_lottery", "apply_sign_in", "apply_sign_in_statistics", "apply_sign_in_tasks"]
