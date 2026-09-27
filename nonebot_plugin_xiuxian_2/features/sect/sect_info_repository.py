from __future__ import annotations

from pathlib import Path
from typing import Any

from ...core.numeric import normalize_sect_row
from ...infrastructure.database import DatabaseUnitOfWork


class SectInfoSqlRepository:
    def __init__(self, database: str | Path) -> None:
        self.database = str(database)

    def get_by_id(self, sect_id: int | str) -> dict[str, Any] | None:
        with DatabaseUnitOfWork(self.database, read_only=True) as uow:
            row = uow.query_one(
                "SELECT * FROM sects WHERE sect_id=?",
                (sect_id,),
            )
            return normalize_sect_row(row)


__all__ = ["SectInfoSqlRepository"]
