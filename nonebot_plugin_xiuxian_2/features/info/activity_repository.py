"""Persistence for the player's last information-view timestamp."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

from ...infrastructure.database import DatabaseUnitOfWork


class PlayerActivitySqlRepository:
    """Read/write the existing ``user_cd`` projection without request DDL."""

    def __init__(self, database: str | Path) -> None:
        self.database = Path(database)

    def update_last_check_info_time(self, user_id: str, occurred_at: str) -> int:
        if not self.database.exists():
            return 0
        try:
            with DatabaseUnitOfWork(self.database, immediate=True) as uow:
                cursor = uow.execute(
                    "UPDATE user_cd SET last_check_info_time=? WHERE user_id=?",
                    (str(occurred_at), str(user_id)),
                )
                return int(cursor.rowcount)
        except Exception:
            # A missing/old projection is a closed compatibility boundary.
            return 0

    def get_last_check_info_time(self, user_id: str) -> datetime | None:
        if not self.database.exists():
            return None
        try:
            with DatabaseUnitOfWork(self.database, read_only=True) as uow:
                row = uow.query_one(
                    "SELECT last_check_info_time FROM user_cd WHERE user_id=?",
                    (str(user_id),),
                )
        except Exception:
            return None
        if row is None or not row["last_check_info_time"]:
            return None
        value = row["last_check_info_time"]
        if isinstance(value, datetime):
            occurred_at = value
        else:
            occurred_at = None
            for timestamp_format in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S"):
                try:
                    occurred_at = datetime.strptime(str(value), timestamp_format)
                    break
                except ValueError:
                    continue
        if occurred_at is not None and occurred_at.tzinfo is None:
            return occurred_at.astimezone()
        return occurred_at


__all__ = ["PlayerActivitySqlRepository"]
