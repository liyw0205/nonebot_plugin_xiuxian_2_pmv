"""Keep legacy owner equality while seeking across INTEGER/TEXT affinities."""
from __future__ import annotations

from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

from nonebot_plugin_xiuxian_2.features.sect.directory_repository import SectDirectorySqlRepository
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils import db_backend
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils.db_migrations import (
    _create_sect_owner_numeric_index,
    run_core_migrations,
)


INDEX_NAME = "idx_user_xiuxian_user_id_numeric_v1"
OLD_DIRECTORY_SQL = (
    "WITH first_sects AS ("
    "SELECT MIN(rowid) AS first_rowid FROM sects GROUP BY sect_id) "
    "SELECT s.sect_id,s.sect_name,s.sect_scale,s.closed,s.join_open,"
    "(SELECT user_name FROM user_xiuxian WHERE user_id=s.sect_owner "
    "ORDER BY rowid ASC LIMIT 1) AS owner_name,"
    "(SELECT COUNT(*) FROM user_xiuxian WHERE sect_id=s.sect_id) AS member_count "
    "FROM first_sects f JOIN sects s ON s.rowid=f.first_rowid ORDER BY s.sect_id"
)


class SectOwnerNumericSeekTests(unittest.TestCase):
    def database(self, owner_affinity="INTEGER", *, indexed=True):
        self.assertIn(owner_affinity, ("INTEGER", "TEXT", ""))
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        database = Path(directory.name) / "sect.db"
        with sqlite3.connect(database) as connection:
            connection.execute(
                "CREATE TABLE user_xiuxian(user_id TEXT,user_name TEXT,"
                "sect_id INTEGER,stone,exp)"
            )
            connection.execute(
                "CREATE TABLE sects(sect_id INTEGER,sect_name TEXT,sect_scale,"
                f"sect_owner {owner_affinity},closed,join_open)"
            )
            connection.execute("CREATE INDEX member_user ON user_xiuxian(user_id)")
            connection.execute("CREATE INDEX member_sect ON user_xiuxian(sect_id)")
        if indexed:
            with db_backend.transaction(database) as connection:
                _create_sect_owner_numeric_index(connection)
        return database

    def legacy_rows(self, database):
        with sqlite3.connect(database) as connection:
            connection.row_factory = sqlite3.Row
            return [dict(row) for row in connection.execute(OLD_DIRECTORY_SQL)]

    def assert_legacy_rows(self, database):
        self.assertEqual(
            self.legacy_rows(database),
            SectDirectorySqlRepository(database).list_join_display(),
        )

    def test_owner_affinities_keep_original_equality_and_first_rowid(self):
        values = [
            None, "001", "1", "1.0", "1e0", "+1", " 1 ", "1x", "abc", "0",
            "0x10", "16", "2.5", "02.50", "2.5e0", "-0", "-0.0", "1e-999",
            "1e999", "-1e999", "inf", "NaN", "9223372036854775807",
            "9223372036854775808", "9223372036854775807.0", "-9223372036854775809",
            "9007199254740993", "9007199254740993.0", "9007199254740992",
            "123456789012345678901234567890", "1.2345678901234567890123456789e29",
            "12\x00tail", sqlite3.Binary(b"1"), sqlite3.Binary(b"001"),
            sqlite3.Binary(b"abc"), sqlite3.Binary(b"\xff1"), sqlite3.Binary(b"\x00"),
            1, 1.0, 2.5, 9007199254740992, 9223372036854775807,
            float("inf"), -float("inf"),
        ]
        for owner_affinity in ("INTEGER", "TEXT", ""):
            with self.subTest(owner_affinity=owner_affinity):
                database = self.database(owner_affinity)
                with sqlite3.connect(database) as connection:
                    connection.executemany(
                        "INSERT INTO user_xiuxian VALUES(?,?,1,0,0)",
                        [(value, f"Owner-{index}") for index, value in enumerate(values)],
                    )
                    connection.execute(
                        "INSERT INTO user_xiuxian VALUES('001','Later duplicate',1,0,0)"
                    )
                    connection.executemany(
                        "INSERT INTO sects VALUES(?,?,0,?,0,1)",
                        [(index + 1, f"Sect-{index}", value)
                         for index, value in enumerate(values)],
                    )
                    connection.execute(
                        "INSERT INTO sects VALUES(2,'Later sect',0,'abc',1,0)"
                    )
                    connection.execute(
                        "INSERT INTO sects VALUES(NULL,'Null identity',0,'001',0,1)"
                    )
                self.assert_legacy_rows(database)
                rows = SectDirectorySqlRepository(database).list_join_display()
                row = next(row for row in rows if row["sect_id"] == 2)
                self.assertEqual(("Sect-1", "Owner-1"), (row["sect_name"], row["owner_name"]))
                self.assertEqual(0, rows[0]["member_count"])

    def test_original_predicate_rejects_numeric_cast_collisions(self):
        database = self.database()
        with sqlite3.connect(database) as connection:
            connection.executemany(
                "INSERT INTO user_xiuxian VALUES(?,?,1,0,0)",
                [("garbage", "Wrong zero"), ("1tail", "Wrong one"),
                 ("0", "Zero"), ("001", "First one"), ("1", "Later one")],
            )
            connection.executemany(
                "INSERT INTO sects VALUES(?,?,0,?,0,1)",
                [(1, "Zero sect", 0), (2, "One sect", 1), (3, "Missing", "absent")],
            )
        self.assert_legacy_rows(database)
        self.assertEqual(
            ["Zero", "First one", None],
            [row["owner_name"] for row in SectDirectorySqlRepository(database).list_join_display()],
        )

    def test_real_integer_owner_plan_seeks_numeric_index_in_one_readonly_statement(self):
        database = self.database()
        with sqlite3.connect(database) as connection:
            connection.execute("INSERT INTO sects VALUES(1,'Sect',0,123,0,1)")
            connection.execute("INSERT INTO user_xiuxian VALUES('123','Owner',1,0,0)")
            old_plan = [row[3] for row in connection.execute("EXPLAIN QUERY PLAN " + OLD_DIRECTORY_SQL)]
            before = tuple(connection.iterdump())
        self.assertTrue(any("SCAN user_xiuxian" in detail for detail in old_plan))
        statements = []
        query = DatabaseUnitOfWork.query_all

        def collect(uow, sql, params=()):
            self.assertTrue(uow.read_only)
            statements.append(sql)
            return query(uow, sql, params)

        with patch.object(DatabaseUnitOfWork, "query_all", collect):
            rows = SectDirectorySqlRepository(database).list_join_display()
        self.assertEqual("Owner", rows[0]["owner_name"])
        self.assertEqual(1, len(statements))
        with sqlite3.connect(database) as connection:
            plan = [row[3] for row in connection.execute("EXPLAIN QUERY PLAN " + statements[0])]
            self.assertEqual(before, tuple(connection.iterdump()))
        self.assertTrue(any("SEARCH user_xiuxian USING INDEX " + INDEX_NAME in detail for detail in plan))
        self.assertFalse(any("SCAN user_xiuxian" in detail for detail in plan))

    def test_live_reads_follow_identity_name_membership_and_balance_changes(self):
        database = self.database()
        repository = SectDirectorySqlRepository(database)
        with sqlite3.connect(database) as connection:
            connection.execute("INSERT INTO sects VALUES(1,'Sect',0,1,0,1)")
            connection.execute("INSERT INTO user_xiuxian VALUES('001','First',1,0,0)")
            connection.execute("INSERT INTO user_xiuxian VALUES('1','Second',1,0,0)")
        self.assertEqual("First", repository.list_join_display()[0]["owner_name"])
        exact_value = str(2**100 + 7)
        with sqlite3.connect(database) as connection:
            update_plan = list(connection.execute(
                "EXPLAIN UPDATE user_xiuxian SET stone=?,exp=? WHERE rowid=1",
                (exact_value, exact_value),
            ))
            self.assertFalse(any(row[1] in {"IdxInsert", "IdxDelete"} for row in update_plan))
            connection.execute("UPDATE user_xiuxian SET stone=?,exp=? WHERE rowid=1",
                               (exact_value, exact_value))
        self.assertEqual("First", repository.list_join_display()[0]["owner_name"])
        with sqlite3.connect(database) as connection:
            self.assertEqual((exact_value, exact_value), connection.execute(
                "SELECT stone,exp FROM user_xiuxian WHERE rowid=1").fetchone())
            connection.execute("UPDATE user_xiuxian SET user_name='Renamed' WHERE rowid=1")
        self.assertEqual("Renamed", repository.list_join_display()[0]["owner_name"])
        with sqlite3.connect(database) as connection:
            connection.execute("UPDATE user_xiuxian SET user_id='2',sect_id=NULL WHERE rowid=1")
        self.assertEqual(("Second", 1), (
            repository.list_join_display()[0]["owner_name"],
            repository.list_join_display()[0]["member_count"],
        ))
        with sqlite3.connect(database) as connection:
            connection.execute("UPDATE sects SET sect_owner=2")
        self.assertEqual("Renamed", repository.list_join_display()[0]["owner_name"])
        with sqlite3.connect(database) as connection:
            connection.execute("DELETE FROM user_xiuxian WHERE rowid=1")
        self.assertIsNone(repository.list_join_display()[0]["owner_name"])
        with sqlite3.connect(database) as connection:
            connection.execute("INSERT INTO user_xiuxian VALUES('2','New owner',1,0,0)")
        self.assertEqual("New owner", repository.list_join_display()[0]["owner_name"])
        self.assert_legacy_rows(database)

    def test_absent_index_remains_correct_and_denied_reads_propagate_then_recover(self):
        database = self.database(indexed=False)
        with sqlite3.connect(database) as connection:
            connection.execute("INSERT INTO sects VALUES(1,'Sect',0,1,0,1)")
            connection.execute("INSERT INTO user_xiuxian VALUES('001','Owner',1,0,0)")
        self.assert_legacy_rows(database)
        enter = DatabaseUnitOfWork.__enter__

        def deny_owner(uow):
            result = enter(uow)
            uow.connection.set_authorizer(
                lambda action, table, column, *_: sqlite3.SQLITE_DENY
                if action == sqlite3.SQLITE_READ and table == "user_xiuxian"
                and column == "user_name" else sqlite3.SQLITE_OK
            )
            return result

        with patch.object(DatabaseUnitOfWork, "__enter__", deny_owner), \
                self.assertRaises(sqlite3.DatabaseError):
            SectDirectorySqlRepository(database).list_join_display()
        self.assert_legacy_rows(database)

    def test_core_index_maintenance_is_idempotent_and_guards_old_schemas(self):
        for table_sql in (None, "CREATE TABLE user_xiuxian(user_name TEXT)"):
            with self.subTest(table_sql=table_sql):
                directory = tempfile.TemporaryDirectory()
                self.addCleanup(directory.cleanup)
                database = Path(directory.name) / "old.db"
                with db_backend.transaction(database) as connection:
                    if table_sql:
                        connection.execute(table_sql)
                    _create_sect_owner_numeric_index(connection)
                    self.assertIsNone(connection.execute(
                        "SELECT name FROM sqlite_master WHERE name=?", (INDEX_NAME,)
                    ).fetchone())

        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        database = Path(directory.name) / "core.db"
        with sqlite3.connect(database) as connection:
            connection.execute("CREATE TABLE user_xiuxian(id INTEGER PRIMARY KEY,user_id TEXT)")
            connection.execute("INSERT INTO user_xiuxian(user_id) VALUES('001')")
        run_core_migrations(database)
        with sqlite3.connect(database) as connection:
            definition = connection.execute(
                "SELECT rootpage,sql FROM sqlite_master WHERE name=?", (INDEX_NAME,)
            ).fetchone()
        self.assertIsNotNone(definition)
        self.assertIn("CAST(user_id AS NUMERIC)", definition[1])
        self.assertEqual([], run_core_migrations(database))
        with sqlite3.connect(database) as connection:
            self.assertEqual(definition, connection.execute(
                "SELECT rootpage,sql FROM sqlite_master WHERE name=?", (INDEX_NAME,)
            ).fetchone())
            connection.execute("DROP INDEX " + INDEX_NAME)
        self.assertEqual([], run_core_migrations(database))
        with sqlite3.connect(database) as connection:
            self.assertIsNotNone(connection.execute(
                "SELECT name FROM sqlite_master WHERE name=?", (INDEX_NAME,)
            ).fetchone())

    def test_index_creation_error_is_not_hidden(self):
        class BrokenConnection:
            def table_exists(self, _):
                return True

            def column_names(self, _):
                return ["user_id"]

            def execute(self, _):
                raise sqlite3.OperationalError("index creation denied")

        with self.assertRaisesRegex(sqlite3.OperationalError, "index creation denied"):
            _create_sect_owner_numeric_index(BrokenConnection())

    def test_maintenance_projection_reads_only_identity_and_preserves_first_rows(self):
        database = self.database()
        with sqlite3.connect(database) as connection:
            connection.executemany("INSERT INTO sects VALUES(?,?,0,1,0,1)", [
                (2, "First two"), (1, "First one"), (None, "First null"),
                (2, "Later two"), (None, "Later null"),
            ])
            connection.execute("INSERT INTO user_xiuxian VALUES('1','Owner',1,0,0)")
            before = tuple(connection.iterdump())
        enter = DatabaseUnitOfWork.__enter__
        query = DatabaseUnitOfWork.query_all
        statements = []

        def only_identity(uow):
            self.assertTrue(uow.read_only)
            result = enter(uow)
            uow.connection.set_authorizer(
                lambda action, table, column, *_: sqlite3.SQLITE_DENY
                if action == sqlite3.SQLITE_READ and (
                    table == "user_xiuxian"
                    or table == "sects" and column.lower() not in {"sect_id", "sect_name", "rowid"}
                ) else sqlite3.SQLITE_OK
            )
            return result

        def collect(uow, sql, params=()):
            statements.append(sql)
            return query(uow, sql, params)

        repository = SectDirectorySqlRepository(database)
        with patch.object(DatabaseUnitOfWork, "__enter__", only_identity), \
                patch.object(DatabaseUnitOfWork, "query_all", collect):
            self.assertEqual(
                [(None, "First null"), (1, "First one"), (2, "First two")],
                repository.list_maintenance_sects(),
            )
        self.assertEqual(1, len(statements))
        with sqlite3.connect(database) as connection:
            self.assertEqual(before, tuple(connection.iterdump()))
            connection.execute("UPDATE sects SET sect_name='Changed' WHERE rowid=2")
            connection.execute("DROP TABLE user_xiuxian")
        self.assertEqual(
            [(None, "First null"), (1, "Changed"), (2, "First two")],
            repository.list_maintenance_sects(),
        )
        with sqlite3.connect(database) as connection:
            connection.execute("DELETE FROM sects")
        self.assertEqual([], repository.list_maintenance_sects())
