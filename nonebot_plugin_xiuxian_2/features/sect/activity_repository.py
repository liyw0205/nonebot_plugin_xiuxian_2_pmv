from __future__ import annotations

from collections.abc import Iterable
from datetime import datetime
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

    def get_last_check_info_time(self, user_id: str) -> datetime | None:
        with DatabaseUnitOfWork(self.database, read_only=True) as uow:
            row = uow.query_one(
                "SELECT last_check_info_time FROM user_cd WHERE user_id=?",
                (str(user_id),),
            )
        if row is None or not row["last_check_info_time"]:
            return None
        return self._parse_time(row["last_check_info_time"])

    def get_last_check_info_times(self, user_ids: Iterable[str]) -> dict[str, datetime | None]:
        identities = tuple(dict.fromkeys(str(value) for value in user_ids))
        result: dict[str, datetime | None] = dict.fromkeys(identities)
        if not identities:
            return result
        with DatabaseUnitOfWork(self.database, read_only=True) as uow:
            for offset in range(0, len(identities), 500):
                chunk = identities[offset:offset + 500]
                # VALUES has no column affinity, so c.user_id keeps the single-read comparison.
                rows = uow.query_all(
                    ("WITH requested AS (VALUES {values}) "
                     "SELECT requested.column1 AS request_id,c.last_check_info_time FROM requested "
                     "LEFT JOIN user_cd c ON c.user_id=requested.column1 ORDER BY c.rowid ASC"
                     ).format(values=",".join("(?)" for _ in chunk)), chunk,
                )
                seen = set()
                for row in rows:
                    user_id = str(row["request_id"])
                    if user_id not in seen:
                        result[user_id] = self._parse_time(row["last_check_info_time"])
                        seen.add(user_id)
        return result

    @staticmethod
    def _parse_time(value) -> datetime | None:
        if not value:
            return None
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


__all__ = ["SectActivitySqlRepository"]
