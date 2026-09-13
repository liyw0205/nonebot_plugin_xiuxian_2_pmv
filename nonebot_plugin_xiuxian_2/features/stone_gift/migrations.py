from __future__ import annotations

from ...infrastructure.database import DatabaseUnitOfWork
from .repository import StoneGiftRepository


def apply_stone_gift(uow: DatabaseUnitOfWork) -> None:
    StoneGiftRepository().ensure_schema(uow)


def apply_stone_gift_limits(uow: DatabaseUnitOfWork) -> None:
    StoneGiftRepository().ensure_limit_schema(uow)


__all__ = ["apply_stone_gift", "apply_stone_gift_limits"]
