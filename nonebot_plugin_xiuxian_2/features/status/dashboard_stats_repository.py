from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path

from ...infrastructure.database import DatabaseUnitOfWork


@dataclass(frozen=True, slots=True)
class DashboardStatsSnapshot:
    total_users: int
    total_sects: int
    active_users: int
    yesterday_users: int
    seven_days_avg: int
    msg_received: int
    msg_sent: int

    def as_dict(self) -> dict[str, int]:
        return {
            "total_users": self.total_users,
            "total_sects": self.total_sects,
            "active_users": self.active_users,
            "yesterday_users": self.yesterday_users,
            "seven_days_avg": self.seven_days_avg,
            "msg_received": self.msg_received,
            "msg_sent": self.msg_sent,
        }


class DashboardStatsSqlRepository:
    """Read dashboard counters from the existing game and message databases."""

    def __init__(self, game_database: str | Path, message_database: str | Path) -> None:
        self.game_database = Path(game_database)
        self.message_database = Path(message_database)

    @staticmethod
    def _table_columns(uow: DatabaseUnitOfWork, table: str) -> set[str]:
        return {
            str(row["name"])
            for row in uow.query_all(f'PRAGMA table_info("{table}")')
        }

    def snapshot(self, *, now: datetime | None = None) -> DashboardStatsSnapshot:
        current = now or datetime.now()
        game_stats = self._game_stats(current)
        message_stats = self._message_stats()
        return DashboardStatsSnapshot(*game_stats, *message_stats)

    def _game_stats(self, now: datetime) -> tuple[int, int, int, int, int]:
        if not self.game_database.is_file():
            return (0, 0, 0, 0, 0)

        with DatabaseUnitOfWork(self.game_database, read_only=True) as uow:
            columns = {
                table: self._table_columns(uow, table)
                for table in ("user_xiuxian", "sects", "user_cd")
            }
            create_date = "substr(CAST(create_time AS TEXT), 1, 10)"
            today = now.strftime("%Y-%m-%d")
            yesterday = (now - timedelta(days=1)).strftime("%Y-%m-%d")
            seven_days_ago = (now - timedelta(days=6)).strftime("%Y-%m-%d")
            total_users = (
                "(SELECT COUNT(*) FROM user_xiuxian)"
                if columns["user_xiuxian"] else "0"
            )
            total_sects = (
                "(SELECT COUNT(*) FROM sects WHERE sect_owner IS NOT NULL)"
                if "sect_owner" in columns["sects"] else "0"
            )
            if {"user_id", "create_time"} <= columns["user_cd"]:
                activity_counts = (
                    f"COUNT(DISTINCT CASE WHEN {create_date} = ? THEN user_id END), "
                    f"COUNT(DISTINCT CASE WHEN {create_date} = ? THEN user_id END), "
                    f"COUNT(DISTINCT CASE WHEN {create_date} >= ? THEN user_id END)"
                )
                sql = f"SELECT {total_users}, {total_sects}, {activity_counts} FROM user_cd"
                params = (today, yesterday, seven_days_ago)
            else:
                sql = f"SELECT {total_users}, {total_sects}, 0, 0, 0"
                params = ()
            row = uow.execute(sql, params).fetchone()
        return tuple(int(value or 0) for value in row)

    def _message_stats(self) -> tuple[int, int]:
        if not self.message_database.is_file():
            return (0, 0)
        with DatabaseUnitOfWork(self.message_database, read_only=True) as uow:
            columns = self._table_columns(uow, "messages")
            if not {"direction"} <= columns:
                return (0, 0)
            row = uow.execute(
                "SELECT "
                "COALESCE(SUM(CASE WHEN direction='recv' THEN 1 ELSE 0 END), 0), "
                "COALESCE(SUM(CASE WHEN direction='send' THEN 1 ELSE 0 END), 0) "
                "FROM messages"
            ).fetchone()
        return int(row[0] or 0), int(row[1] or 0)


__all__ = ["DashboardStatsSnapshot", "DashboardStatsSqlRepository"]
