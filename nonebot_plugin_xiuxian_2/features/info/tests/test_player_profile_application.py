from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path

from ..profile_application import PlayerProfileApplication
from ..profile_repository import (
    MAX_USER_SEARCH_QUERY_CHARS,
    MAX_USER_SEARCH_RESULTS,
    PlayerProfileSqlRepository,
)


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

    def test_search_is_fuzzy_bounded_and_read_only(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "game.db"
            with sqlite3.connect(database) as connection:
                connection.execute("CREATE TABLE user_xiuxian (user_id TEXT,user_name TEXT)")
                connection.executemany(
                    "INSERT INTO user_xiuxian VALUES (?, ?)",
                    [(f"u{i}", f"道友-{i}") for i in range(MAX_USER_SEARCH_RESULTS + 2)],
                )
            before = database.stat().st_mtime_ns

            result = PlayerProfileApplication(database).search_users("道友")

            self.assertEqual(len(result), MAX_USER_SEARCH_RESULTS)
            self.assertEqual(result[0], {"id": "u0", "name": "道友-0"})
            self.assertEqual(database.stat().st_mtime_ns, before)
            self.assertFalse((database.parent / "game.db-wal").exists())

    def test_search_escapes_like_metacharacters(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "game.db"
            with sqlite3.connect(database) as connection:
                connection.execute("CREATE TABLE user_xiuxian (user_id TEXT,user_name TEXT)")
                connection.executemany(
                    "INSERT INTO user_xiuxian VALUES (?, ?)",
                    [("percent", "a%b"), ("underscore", "a_b"), ("slash", r"a\b"), ("wild", "axb")],
                )

            repository = PlayerProfileSqlRepository(database)
            self.assertEqual(repository.search_users("%"), [{"id": "percent", "name": "a%b"}])
            self.assertEqual(repository.search_users("_"), [{"id": "underscore", "name": "a_b"}])
            self.assertEqual(repository.search_users("\\"), [{"id": "slash", "name": r"a\b"}])

    def test_search_rejects_overlong_query_without_opening_database(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "missing" / "game.db"
            with self.assertRaises(ValueError):
                PlayerProfileSqlRepository(database).search_users("x" * (MAX_USER_SEARCH_QUERY_CHARS + 1))
            self.assertFalse(database.exists())
            self.assertFalse(database.parent.exists())

    def test_search_missing_database_or_schema_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "missing" / "game.db"
            self.assertEqual(PlayerProfileSqlRepository(database).search_users("u"), [])
            self.assertFalse(database.exists())
            self.assertFalse(database.parent.exists())

            schema_database = Path(directory) / "schema.db"
            sqlite3.connect(schema_database).close()
            self.assertEqual(PlayerProfileSqlRepository(schema_database).search_users("u"), [])


if __name__ == "__main__":
    unittest.main()
