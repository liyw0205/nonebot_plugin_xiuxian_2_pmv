from __future__ import annotations

from ...infrastructure.database import DatabaseUnitOfWork
from .schema import LIMITS_DDL, OPERATIONS_DDL


def apply_stone_gift(uow: DatabaseUnitOfWork) -> None:
    for statement in OPERATIONS_DDL:
        uow.execute(statement)


def apply_stone_gift_limits(uow: DatabaseUnitOfWork) -> None:
    for statement in LIMITS_DDL:
        uow.execute(statement)


__all__ = ["apply_stone_gift", "apply_stone_gift_limits"]
