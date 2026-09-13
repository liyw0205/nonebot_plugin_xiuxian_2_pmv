"""Versioned migration for the daily-fortune vertical slice."""

from ...infrastructure.database import DatabaseUnitOfWork
from .repository import DailyFortuneRepository

MIGRATION_VERSION = "daily_fortune.001"


def apply_daily_fortune(uow: DatabaseUnitOfWork) -> None:
    DailyFortuneRepository().ensure_schema(uow)

__all__ = ["MIGRATION_VERSION", "apply_daily_fortune"]
