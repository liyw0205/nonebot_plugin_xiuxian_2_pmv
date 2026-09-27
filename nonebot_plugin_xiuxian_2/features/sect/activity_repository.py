from __future__ import annotations

from pathlib import Path

from ...infrastructure.database import DatabaseUnitOfWork


class SectActivitySqlRepository:
    def __init__(self, database: str | Path) -> None:
        self.database = str(database)

    def update_last_check_info_time(self, user_id: str, occurred_at: str) -> int:
        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            cursor = uow.execute(
                "UPDATE user_cd SET last_check_info_time=? WHERE user_id=?",
                (str(occurred_at), str(user_id)),
            )
            return int(cursor.rowcount)


__all__ = ["SectActivitySqlRepository"]
