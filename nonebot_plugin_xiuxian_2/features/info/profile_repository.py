"""Read-only ownership for the shared player profile projection."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from ...core.numeric import normalize_user_row
from ...infrastructure.database import DatabaseUnitOfWork


class PlayerProfileSqlRepository:
    """Read the legacy ``user_xiuxian`` projection without opening a writer."""

    def __init__(self, database: str | Path) -> None:
        self.database = str(database)

    def get_user_profile(self, user_id: int | str) -> dict[str, Any] | None:
        try:
            with DatabaseUnitOfWork(self.database, read_only=True) as uow:
                row = uow.query_one(
                    "SELECT * FROM user_xiuxian WHERE user_id=? "
                    "ORDER BY rowid ASC LIMIT 1",
                    (str(user_id),),
                )
        except Exception:
            # A missing database/schema is not a reason to create one during a
            # command.  Callers treat the closed projection as an unknown user.
            return None
        return normalize_user_row(row)


__all__ = ["PlayerProfileSqlRepository"]
