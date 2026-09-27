from __future__ import annotations

from pathlib import Path
from typing import Any

from ...infrastructure.database import DatabaseUnitOfWork


class SectInactiveOwnerSqlRepository:
    def __init__(self, database: str | Path) -> None:
        self.database = str(database)

    def get_sect_state(self, sect_id: int) -> dict[str, Any] | None:
        with DatabaseUnitOfWork(self.database, read_only=True) as uow:
            return uow.query_one(
                "SELECT closed,sect_owner FROM sects WHERE sect_id=?",
                (int(sect_id),),
            )

    def get_owner_profile(self, owner_id: str) -> dict[str, Any] | None:
        with DatabaseUnitOfWork(self.database, read_only=True) as uow:
            return uow.query_one(
                "SELECT user_name FROM user_xiuxian "
                "WHERE user_id=? ORDER BY rowid ASC LIMIT 1",
                (owner_id,),
            )


__all__ = ["SectInactiveOwnerSqlRepository"]
