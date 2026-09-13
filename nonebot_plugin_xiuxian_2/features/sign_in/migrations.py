from __future__ import annotations

from ...infrastructure.database import DatabaseUnitOfWork
from .repository import SignInRepository


def apply_sign_in(uow: DatabaseUnitOfWork) -> None:
    SignInRepository().ensure_schema(uow)


__all__ = ["apply_sign_in"]
