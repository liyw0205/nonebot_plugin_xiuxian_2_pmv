from __future__ import annotations

from pathlib import Path
from typing import Any

from ...core.numeric import normalize_user_row
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

    def list_members(self, sect_id: int) -> list[dict[str, Any]]:
        with DatabaseUnitOfWork(self.database, read_only=True) as uow:
            rows = uow.query_all(
                "SELECT * FROM user_xiuxian WHERE sect_id=?",
                (int(sect_id),),
            )
            return [normalize_user_row(row) for row in rows]


__all__ = ["SectInactiveOwnerSqlRepository"]
