from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path
import tempfile
import unittest

from ....infrastructure.database import DatabaseUnitOfWork
from ..application import StatusApplication


class DashboardStatsTests(unittest.TestCase):
    def test_dashboard_stats_preserve_legacy_seven_day_meaning(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            game_database = Path(directory) / "game.db"
            message_database = Path(directory) / "message.db"
            now = datetime(2026, 10, 8, 12, 0)

            with DatabaseUnitOfWork(game_database) as uow:
                uow.execute("CREATE TABLE user_xiuxian(user_id TEXT)")
                uow.executemany(
                    "INSERT INTO user_xiuxian(user_id) VALUES (?)",
                    [("u1",), ("u2",), ("u3",)],
                )
                uow.execute("CREATE TABLE sects(sect_owner TEXT)")
                uow.executemany(
                    "INSERT INTO sects(sect_owner) VALUES (?)",
                    [("owner-1",), ("owner-2",), (None,)],
                )
                uow.execute("CREATE TABLE user_cd(user_id TEXT, create_time TEXT)")
                uow.executemany(
                    "INSERT INTO user_cd(user_id, create_time) VALUES (?, ?)",
                    [
                        ("today-1", now.strftime("%Y-%m-%d 01:00:00")),
                        ("today-1", now.strftime("%Y-%m-%d 02:00:00")),
                        ("today-2", now.strftime("%Y-%m-%d 03:00:00")),
                        ("yesterday", (now - timedelta(days=1)).strftime("%Y-%m-%d 01:00:00")),
                        ("six-days-ago", (now - timedelta(days=6)).strftime("%Y-%m-%d 01:00:00")),
                        ("seven-days-ago", (now - timedelta(days=7)).strftime("%Y-%m-%d 01:00:00")),
                    ],
                )

            with DatabaseUnitOfWork(message_database) as uow:
                uow.execute("CREATE TABLE messages(direction TEXT)")
                uow.executemany(
                    "INSERT INTO messages(direction) VALUES (?)",
                    [("recv",), ("recv",), ("send",), ("other",)],
                )

            application = StatusApplication(game_database, message_database=message_database)
            self.assertEqual(
                application.dashboard_stats(now=now),
                {
                    "total_users": 3,
                    "total_sects": 2,
                    "active_users": 2,
                    "yesterday_users": 1,
                    "seven_days_avg": 4,
                    "msg_received": 2,
                    "msg_sent": 1,
                },
            )

    def test_missing_databases_return_zeros_without_creating_them(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            game_database = Path(directory) / "game.db"
            message_database = Path(directory) / "message.db"
            application = StatusApplication(game_database, message_database=message_database)

            self.assertEqual(
                application.dashboard_stats(now=datetime(2026, 10, 8)),
                {
                    "total_users": 0,
                    "total_sects": 0,
                    "active_users": 0,
                    "yesterday_users": 0,
                    "seven_days_avg": 0,
                    "msg_received": 0,
                    "msg_sent": 0,
                },
            )
            self.assertFalse(game_database.exists())
            self.assertFalse(message_database.exists())


if __name__ == "__main__":
    unittest.main()
