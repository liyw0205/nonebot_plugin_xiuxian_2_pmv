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

    def get_user_profile(self, user_id: int | str) -> dict[str, Any] | None:
        with DatabaseUnitOfWork(self.database, read_only=True) as uow:
            row = uow.query_one(
                "SELECT * FROM user_xiuxian WHERE user_id=? "
                "ORDER BY rowid ASC LIMIT 1",
                (user_id,),
            )
            return normalize_user_row(row)

    def get_user_profile_by_name(self, user_name: str) -> dict[str, Any] | None:
        with DatabaseUnitOfWork(self.database, read_only=True) as uow:
            row = uow.query_one(
                "SELECT * FROM user_xiuxian WHERE user_name=? "
                "ORDER BY rowid ASC LIMIT 1",
                (user_name,),
            )
            return normalize_user_row(row)


__all__ = ["SectMemberSqlRepository"]
