from __future__ import annotations

import sqlite3
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from ..activity_application import PlayerActivityApplication
from ..activity_repository import PlayerActivitySqlRepository


class _Clock:
    def now(self):
        return datetime(2026, 10, 1, 4, 30, tzinfo=timezone.utc)


class PlayerActivityApplicationTest(unittest.TestCase):
    def test_updates_existing_row_with_legacy_local_format(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "game.db"
            with sqlite3.connect(database) as connection:
                connection.execute(
                    "CREATE TABLE user_cd(user_id TEXT PRIMARY KEY,last_check_info_time TEXT)"
                )
                connection.execute("INSERT INTO user_cd VALUES('u','old')")

            application = PlayerActivityApplication(database, clock=_Clock())
            self.assertEqual(1, application.update_last_check_info_time("u"))
            self.assertEqual(0, application.update_last_check_info_time("missing"))
            self.assertIsNotNone(application.get_last_check_info_time("u"))

            with sqlite3.connect(database) as connection:
                row = connection.execute(
                    "SELECT last_check_info_time FROM user_cd WHERE user_id='u'"
                ).fetchone()
            expected = _Clock().now().astimezone().replace(tzinfo=None).isoformat(sep=" ")
            self.assertEqual(expected, row[0])

    def test_missing_database_and_schema_fail_closed_without_ddl(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            missing = Path(directory) / "missing.db"
            application = PlayerActivityApplication(missing, clock=_Clock())
            self.assertEqual(0, application.update_last_check_info_time("u"))
            self.assertIsNone(application.get_last_check_info_time("u"))
            self.assertFalse(missing.exists())

            schema_only = Path(directory) / "schema-only.db"
            sqlite3.connect(schema_only).close()
            repository = PlayerActivitySqlRepository(schema_only)
            self.assertEqual(0, repository.update_last_check_info_time("u", "now"))
            self.assertIsNone(repository.get_last_check_info_time("u"))
            with sqlite3.connect(schema_only) as connection:
                self.assertEqual([], connection.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                ).fetchall())


if __name__ == "__main__":
    unittest.main()
