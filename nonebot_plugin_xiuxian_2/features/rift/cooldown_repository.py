from __future__ import annotations

from pathlib import Path
from typing import Any

from ...infrastructure.database import DatabaseUnitOfWork


class RiftCooldownSqlRepository:
    """Read the legacy cooldown projection without creating its schema."""

    table = "user_cd"

    def __init__(self, database: str | Path) -> None:
        self.database = Path(database)

    def read(self, user_id: str) -> dict[str, Any] | None:
        user_id = str(user_id).strip()
        if not user_id or not self.database.is_file():
            return None
        with DatabaseUnitOfWork(self.database, read_only=True) as uow:
            if uow.query_one(
                "SELECT 1 AS present FROM sqlite_master WHERE type='table' AND name=?",
                (self.table,),
            ) is None:
                return None
            columns = {
                str(row["name"])
                for row in uow.query_all(f"PRAGMA table_info({self.table})")
            }
            required = {"user_id", "type", "create_time", "scheduled_time"}
            if not required.issubset(columns):
                return None
            row = uow.query_one(
                f"SELECT type,create_time,scheduled_time FROM {self.table} WHERE user_id=?",
                (user_id,),
            )
        return dict(row) if row is not None else None


__all__ = ["RiftCooldownSqlRepository"]
