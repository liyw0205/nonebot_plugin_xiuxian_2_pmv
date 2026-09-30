from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path

from ..profile_application import PlayerProfileApplication
from ..profile_repository import PlayerProfileSqlRepository


class PlayerProfileReadTest(unittest.TestCase):
    def test_reads_normalized_profile_without_writing(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "game.db"
            with sqlite3.connect(database) as connection:
                connection.execute(
                    "CREATE TABLE user_xiuxian "
                    "(id INTEGER PRIMARY KEY,user_id TEXT,user_name TEXT,stone TEXT,user_stamina TEXT)"
                )
                connection.execute(
                    "INSERT INTO user_xiuxian VALUES (1,?,?,?,?)",
                    ("u", "道友", "1e3", "42"),
                )
            before = database.stat().st_mtime_ns
            profile = PlayerProfileApplication(database).get_user_profile("u")
            self.assertEqual(profile["stone"], 1000)
            self.assertEqual(profile["user_stamina"], "42")
            self.assertEqual(database.stat().st_mtime_ns, before)

    def test_missing_database_fails_closed_without_creating_file(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "missing.db"
            self.assertIsNone(PlayerProfileSqlRepository(database).get_user_profile("u"))
            self.assertFalse(database.exists())

    def test_duplicate_user_ids_keep_legacy_first_row_semantics(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "game.db"
            with sqlite3.connect(database) as connection:
                connection.execute("CREATE TABLE user_xiuxian (user_id TEXT,user_name TEXT)")
                connection.execute("INSERT INTO user_xiuxian VALUES ('u','first')")
                connection.execute("INSERT INTO user_xiuxian VALUES ('u','second')")
            profile = PlayerProfileSqlRepository(database).get_user_profile("u")
            self.assertEqual(profile["user_name"], "first")

    def test_duplicate_names_keep_legacy_first_row_semantics(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "game.db"
            with sqlite3.connect(database) as connection:
                connection.execute("CREATE TABLE user_xiuxian (user_id TEXT,user_name TEXT)")
                connection.execute("INSERT INTO user_xiuxian VALUES ('u1','same')")
                connection.execute("INSERT INTO user_xiuxian VALUES ('u2','same')")
            profile = PlayerProfileApplication(database).get_user_profile_by_name("same")
            self.assertEqual(profile["user_id"], "u1")


if __name__ == "__main__":
    unittest.main()
