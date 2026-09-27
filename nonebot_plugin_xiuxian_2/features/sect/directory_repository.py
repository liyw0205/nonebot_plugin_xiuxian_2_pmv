from __future__ import annotations

from pathlib import Path
from typing import Any

from ...infrastructure.database import DatabaseUnitOfWork


class SectDirectorySqlRepository:
    def __init__(self, database: str | Path) -> None:
        self.database = str(database)

    def list_with_member_count(self) -> list[tuple[Any, ...]]:
        with DatabaseUnitOfWork(self.database, read_only=True) as uow:
            rows = uow.query_all(
                "SELECT s.sect_id,s.sect_name,s.sect_scale,"
                "(SELECT user_name FROM user_xiuxian WHERE user_id=s.sect_owner) AS user_name,"
                "COUNT(ux.user_id) AS member_count "
                "FROM sects s LEFT JOIN user_xiuxian ux ON s.sect_id=ux.sect_id "
                "GROUP BY s.sect_id"
            )
            return [
                (
                    row["sect_id"],
                    row["sect_name"],
                    row["sect_scale"],
                    row["user_name"],
                    row["member_count"],
                )
                for row in rows
            ]

    def list_active_sect_names(self) -> list[str | None]:
        with DatabaseUnitOfWork(self.database, read_only=True) as uow:
            rows = uow.query_all(
                "SELECT sect_name FROM sects WHERE sect_owner IS NOT NULL"
            )
            return [row["sect_name"] for row in rows]


__all__ = ["SectDirectorySqlRepository"]
