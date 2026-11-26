from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from ....core.errors import DomainError
from ..profile_application import PlayerProfileApplication
from .. import profile_repository
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

    def test_display_profiles_are_current_and_keep_first_row_and_empty_names(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "game.db"
            with sqlite3.connect(database) as connection:
                connection.execute(
                    "CREATE TABLE user_xiuxian (user_id TEXT,user_name TEXT,level TEXT)"
                )
                connection.executemany(
                    "INSERT INTO user_xiuxian VALUES (?,?,?)",
                    [("u", "first", "one"), ("u", "second", "two"),
                     ("empty", "", "three"), ("null", None, None)],
                )
            application = PlayerProfileApplication(database)
            before = database.stat().st_mtime_ns
            self.assertEqual(
                application.get_user_display_profiles_by_ids(["u", "u", "empty", "null", "missing"]),
                {"u": {"user_name": "first", "level": "one"},
                 "empty": {"user_name": "", "level": "three"},
                 "null": {"user_name": None, "level": None}},
            )
            self.assertEqual(database.stat().st_mtime_ns, before)
            self.assertFalse((database.parent / "game.db-wal").exists())
            with sqlite3.connect(database) as connection:
                connection.execute(
                    "UPDATE user_xiuxian SET user_name='updated',level='new' WHERE rowid=1"
                )
            self.assertEqual(
                application.get_user_display_profiles_by_ids(["u"]),
                {"u": {"user_name": "updated", "level": "new"}},
            )

    def test_display_profiles_batch_401_ids_in_one_read_only_connection(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "game.db"
            rows = [(str(index), f"user-{index}", "one") for index in range(401)]
            with sqlite3.connect(database) as connection:
                connection.execute(
                    "CREATE TABLE user_xiuxian (user_id TEXT,user_name TEXT,level TEXT)"
                )
                connection.executemany("INSERT INTO user_xiuxian VALUES (?,?,?)", rows)

            calls = []
            entered = []
            unit_of_work = profile_repository.DatabaseUnitOfWork

            class ObservedRead(unit_of_work):
                def __enter__(self):
                    entered.append(self.read_only)
                    return super().__enter__()

                def query_all(self, sql, params=()):
                    calls.append((sql, params))
                    return super().query_all(sql, params)

            with patch.object(profile_repository, "DatabaseUnitOfWork", ObservedRead):
                profiles = PlayerProfileApplication(database).get_user_display_profiles_by_ids(
                    [row[0] for row in rows] + ["0", None, ""]
                )
            self.assertEqual(len(profiles), 401)
            self.assertEqual(entered, [True])
            self.assertEqual([len(params) for _, params in calls], [400, 1])
            self.assertTrue(all("ORDER BY rowid ASC" in sql for sql, _ in calls))

    def test_empty_display_profiles_do_not_open_database(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "missing" / "game.db"
            with patch.object(profile_repository, "DatabaseUnitOfWork") as unit_of_work:
                self.assertEqual(
                    PlayerProfileApplication(database).get_user_display_profiles_by_ids([None, ""]),
                    {},
                )
            unit_of_work.assert_not_called()
            self.assertFalse(database.parent.exists())

    def test_display_profiles_ignore_unrelated_real_currency_but_full_profile_still_rejects(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "game.db"
            with sqlite3.connect(database) as connection:
                connection.execute(
                    "CREATE TABLE user_xiuxian (user_id TEXT,user_name TEXT,level TEXT,stone REAL)"
                )
                connection.execute(
                    "INSERT INTO user_xiuxian VALUES ('unsafe','name','realm',?)",
                    (float(2**60),),
                )
            application = PlayerProfileApplication(database)
            self.assertEqual(
                application.get_user_display_profiles_by_ids(["unsafe"]),
                {"unsafe": {"user_name": "name", "level": "realm"}},
            )
            with self.assertRaises(DomainError) as caught:
                application.get_user_profile("unsafe", raise_errors=True)
            self.assertEqual(caught.exception.code, "currency_precision_unavailable")
            self.assertEqual(
                caught.exception.details,
                {"reason": "stone_precision_lost_real", "subject": "actor", "field": "stone"},
            )

    def test_display_profile_read_failure_keeps_cause_and_recovers(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "game.db"
            with sqlite3.connect(database) as connection:
                connection.execute(
                    "CREATE TABLE user_xiuxian (user_id TEXT,user_name TEXT,level TEXT)"
                )
                connection.execute("INSERT INTO user_xiuxian VALUES ('u','name','realm')")
            unit_of_work = profile_repository.DatabaseUnitOfWork

            class DenyDisplayRead(unit_of_work):
                def __enter__(self):
                    result = super().__enter__()
                    self.connection.set_authorizer(
                        lambda action, table, *_: sqlite3.SQLITE_DENY
                        if action == sqlite3.SQLITE_READ and table == "user_xiuxian"
                        else sqlite3.SQLITE_OK
                    )
                    return result

            application = PlayerProfileApplication(database)
            with patch.object(profile_repository, "DatabaseUnitOfWork", DenyDisplayRead):
                with self.assertRaises(DomainError) as caught:
                    application.get_user_display_profiles_by_ids(["u"])
            self.assertEqual(caught.exception.code, "profile_read_failed")
            self.assertEqual(
                caught.exception.details,
                {"reason": "profile_read_failed", "subject": "player_profile"},
            )
            self.assertIsInstance(caught.exception.__cause__, sqlite3.DatabaseError)
            self.assertEqual(
                application.get_user_display_profiles_by_ids(["u"]),
                {"u": {"user_name": "name", "level": "realm"}},
            )

    def test_display_profiles_missing_database_or_schema_is_strict_and_never_creates_file(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "missing" / "game.db"
            with self.assertRaises(DomainError) as caught:
                PlayerProfileApplication(database).get_user_display_profiles_by_ids(["u"])
            self.assertEqual(caught.exception.code, "profile_read_failed")
            self.assertIsInstance(caught.exception.__cause__, sqlite3.OperationalError)
            self.assertFalse(database.parent.exists())

            database = Path(directory) / "empty.db"
            sqlite3.connect(database).close()
            before = database.stat().st_mtime_ns
            with self.assertRaises(DomainError):
                PlayerProfileApplication(database).get_user_display_profiles_by_ids(["u"])
            self.assertEqual(database.stat().st_mtime_ns, before)
            with sqlite3.connect(database) as connection:
                self.assertEqual(connection.execute("SELECT name FROM sqlite_master").fetchall(), [])

    def test_missing_database_fails_closed_without_creating_file(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "missing.db"
            self.assertIsNone(PlayerProfileSqlRepository(database).get_user_profile("u"))
            self.assertFalse(database.exists())

    def test_strict_read_failure_keeps_cause_and_does_not_alias_missing_user(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "game.db"
            with sqlite3.connect(database) as connection:
                connection.execute("CREATE TABLE user_xiuxian (user_id TEXT,user_name TEXT)")
                connection.execute("INSERT INTO user_xiuxian VALUES ('u','道友')")

            unit_of_work = profile_repository.DatabaseUnitOfWork

            class DenyProfileRead(unit_of_work):
                def __enter__(self):
                    result = super().__enter__()
                    self.connection.set_authorizer(
                        lambda action, table, *_: sqlite3.SQLITE_DENY
                        if action == sqlite3.SQLITE_READ and table == "user_xiuxian"
                        else sqlite3.SQLITE_OK
                    )
                    return result

            application = PlayerProfileApplication(database)
            with patch.object(profile_repository, "DatabaseUnitOfWork", DenyProfileRead):
                with self.assertRaises(DomainError) as caught:
                    application.get_user_profile("u", raise_errors=True)
                self.assertEqual(
                    caught.exception.details,
                    {"reason": "profile_read_failed", "subject": "player_profile"},
                )
                self.assertIsInstance(caught.exception.__cause__, sqlite3.DatabaseError)
                self.assertIsNone(application.get_user_profile("u"))

            self.assertIsNone(application.get_user_profile("missing", raise_errors=True))

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
