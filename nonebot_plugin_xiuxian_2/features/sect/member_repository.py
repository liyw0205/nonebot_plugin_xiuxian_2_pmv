from __future__ import annotations

from pathlib import Path
from typing import Any

from ...core.numeric import normalize_user_row
from ...infrastructure.database import DatabaseUnitOfWork


class SectMemberSqlRepository:
    def __init__(self, database: str | Path) -> None:
        self.database = str(database)

    def list_by_sect_id(self, sect_id: int | str) -> list[dict[str, Any]]:
        with DatabaseUnitOfWork(self.database, read_only=True) as uow:
            rows = uow.query_all(
                "SELECT * FROM user_xiuxian WHERE sect_id=?",
                (sect_id,),
            )
            return [normalize_user_row(row) for row in rows]


__all__ = ["SectMemberSqlRepository"]
