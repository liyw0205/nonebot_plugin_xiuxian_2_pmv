from __future__ import annotations

from pathlib import Path

from ..migrations import apply_entertainment, apply_entertainment_newapi
from ....infrastructure.database import DatabaseUnitOfWork


def migrate_newapi_state(
    database: str | Path,
    accounts_directory: str | Path,
    history_directory: str | Path,
) -> None:
    with DatabaseUnitOfWork(database, immediate=True) as uow:
        apply_entertainment(uow)
        apply_entertainment_newapi(
            uow,
            accounts_directory=accounts_directory,
            history_directory=history_directory,
            occurred_at="2026-10-05 00:00:00",
        )


__all__ = ["migrate_newapi_state"]
