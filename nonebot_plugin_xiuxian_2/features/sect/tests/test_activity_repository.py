from __future__ import annotations

import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from ....infrastructure.database import DatabaseUnitOfWork
from ..application import SectApplication


class _Clock:
    def now(self):
        return datetime(2026, 9, 27, 5, 30, tzinfo=timezone.utc)


class SectActivityRepositoryTests(unittest.TestCase):
    def test_application_writes_injected_time_without_creating_missing_users(self):
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "game.db"
            with DatabaseUnitOfWork(database) as uow:
                uow.execute(
                    "CREATE TABLE user_cd(user_id TEXT PRIMARY KEY,last_check_info_time TEXT)"
                )
                uow.execute("INSERT INTO user_cd VALUES('u','old')")

            application = SectApplication(database, clock=_Clock())
            self.assertEqual(1, application.update_last_check_info_time("u"))
            self.assertEqual(0, application.update_last_check_info_time("missing"))
            last_check = application.get_last_check_info_time("u")
            self.assertEqual(_Clock().now().astimezone(), last_check)
            self.assertIsNone(application.get_last_check_info_time("missing"))

            with DatabaseUnitOfWork(database, read_only=True) as uow:
                rows = uow.query_all(
                    "SELECT user_id,last_check_info_time FROM user_cd ORDER BY user_id"
                )
            self.assertEqual(
                [
                    {
                        "user_id": "u",
                        "last_check_info_time": _Clock()
                        .now()
                        .astimezone()
                        .replace(tzinfo=None)
                        .isoformat(sep=" "),
                    }
                ],
                rows,
            )


if __name__ == "__main__":
    unittest.main()
