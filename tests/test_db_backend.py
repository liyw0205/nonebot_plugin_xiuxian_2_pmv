from __future__ import annotations

import importlib.util
import sqlite3
import tempfile
import unittest
from pathlib import Path


MODULE_PATH = (
    Path(__file__).resolve().parents[1]
    / "nonebot_plugin_xiuxian_2"
    / "xiuxian"
    / "xiuxian_utils"
    / "db_backend.py"
)
SPEC = importlib.util.spec_from_file_location("xiuxian_db_backend", MODULE_PATH)
if SPEC is None or SPEC.loader is None:  # pragma: no cover
    raise RuntimeError(f"Unable to load {MODULE_PATH}")
db_backend = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(db_backend)


class SqlConversionTests(unittest.TestCase):
    def test_converts_only_executable_sql(self) -> None:
        sql = "SELECT '%s ILIKE', \"%s\", value ILIKE %s -- %s\n/* %s */"

        converted = db_backend._convert_sql(sql)

        self.assertEqual(
            converted,
            "SELECT '%s ILIKE', \"%s\", value LIKE ? -- %s\n/* %s */",
        )

    def test_preserves_escaped_quotes(self) -> None:
        sql = "SELECT 'it''s %s', \"a\"\"%s\", %s"

        self.assertEqual(
            db_backend._convert_sql(sql),
            "SELECT 'it''s %s', \"a\"\"%s\", ?",
        )

    def test_preserves_sqlite_format_tokens(self) -> None:
        sql = "SELECT strftime('%s', created_at) FROM users WHERE id = %s"

        self.assertEqual(
            db_backend._convert_sql(sql),
            "SELECT strftime('%s', created_at) FROM users WHERE id = ?",
        )

    def test_converts_postgres_compatibility_syntax(self) -> None:
        sql = "SELECT btrim(name) FROM users ORDER BY score NULLS LAST FOR UPDATE SKIP LOCKED"

        self.assertEqual(
            db_backend._convert_sql(sql),
            "SELECT trim(name) FROM users ORDER BY score",
        )


class DatabaseHelperTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.database = Path(self.temp_dir.name) / "test.sqlite3"

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def test_query_and_write_helpers(self) -> None:
        db_backend.execute_write(
            self.database,
            "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, enabled INTEGER)",
        )
        db_backend.execute_write(
            self.database,
            "INSERT INTO users (name, enabled) VALUES (%s, %s)",
            ("Alice", True),
        )

        row = db_backend.query_one(
            self.database,
            "SELECT name, enabled FROM users WHERE name = %s",
            "Alice",
        )

        self.assertEqual(row, {"name": "Alice", "enabled": 1})

    def test_readonly_connection_does_not_create_missing_database(self) -> None:
        with self.assertRaises(sqlite3.OperationalError):
            db_backend.connect_readonly(self.database)

        self.assertFalse(self.database.exists())

    def test_readonly_connection_rejects_writes(self) -> None:
        db_backend.execute_write(self.database, "CREATE TABLE values_table (value INTEGER)")
        conn = db_backend.connect_readonly(self.database)
        try:
            with self.assertRaises(sqlite3.OperationalError):
                conn.execute("INSERT INTO values_table VALUES (%s)", (1,))
        finally:
            conn.close()

    def test_transaction_rolls_back_on_error(self) -> None:
        db_backend.execute_write(self.database, "CREATE TABLE values_table (value INTEGER)")

        with self.assertRaises(RuntimeError):
            with db_backend.transaction(self.database) as connection:
                connection.execute("INSERT INTO values_table VALUES (%s)", (1,))
                raise RuntimeError("abort")

        self.assertEqual(
            db_backend.query_one(self.database, "SELECT COUNT(*) AS count FROM values_table"),
            {"count": 0},
        )

    def test_executemany_accepts_generators(self) -> None:
        db_backend.execute_write(self.database, "CREATE TABLE values_table (value INTEGER)")

        with db_backend.transaction(self.database) as connection:
            connection.executemany(
                "INSERT INTO values_table VALUES (%s)",
                ((value,) for value in range(3)),
            )

        self.assertEqual(
            db_backend.query_one(self.database, "SELECT SUM(value) AS total FROM values_table"),
            {"total": 3},
        )

    def test_ensure_columns_quotes_identifiers(self) -> None:
        db_backend.execute_write(self.database, 'CREATE TABLE "odd table" (id INTEGER)')

        added = db_backend.ensure_columns(
            self.database,
            "odd table",
            {"select": "TEXT DEFAULT NULL"},
        )

        self.assertEqual(added, ["select"])
        self.assertTrue(db_backend.column_exists(self.database, "odd table", "select"))

    def test_legacy_impart_updates_scope_to_effective_row(self) -> None:
        db_backend.execute_write(
            self.database,
            "CREATE TABLE xiuxian_impart (user_id TEXT, stone_num INTEGER)",
        )
        with db_backend.transaction(self.database) as conn:
            conn.execute("INSERT INTO xiuxian_impart VALUES (%s,%s)", ("u", 670))
            conn.execute("INSERT INTO xiuxian_impart VALUES (%s,%s)", ("u", 460))
            changed = conn.execute(
                "UPDATE xiuxian_impart SET stone_num=stone_num-10 WHERE user_id=%s",
                ("u",),
            )
            self.assertEqual(changed.rowcount, 1)
        self.assertEqual(
            db_backend.query_all(
                self.database,
                "SELECT stone_num FROM xiuxian_impart WHERE user_id=%s ORDER BY rowid",
                ("u",),
            ),
            [{"stone_num": 660}, {"stone_num": 460}],
        )

    def test_legacy_impart_update_preserves_parameters_for_qualified_and_batch_sql(self) -> None:
        attached = Path(self.temp_dir.name) / "impart.sqlite3"
        db_backend.execute_write(
            attached,
            "CREATE TABLE xiuxian_impart (user_id TEXT, stone_num INTEGER)",
        )
        with db_backend.transaction(self.database) as conn:
            conn.execute("ATTACH DATABASE %s AS impart_data", (str(attached),))
            conn.execute("CREATE TABLE xiuxian_impart (user_id TEXT, stone_num INTEGER)")
            for database in ("main", "impart_data"):
                conn.executemany(
                    f"INSERT INTO {database}.xiuxian_impart VALUES (?, ?)",
                    (("u", 670), ("u", 460), ("v", 100), ("v", 30)),
                )
            changed = conn.execute(
                "UPDATE impart_data.xiuxian_impart SET stone_num=stone_num-? "
                "WHERE user_id = ? AND stone_num = ?",
                (10, "u", 670),
            )
            self.assertEqual(changed.rowcount, 1)
            stale = conn.execute(
                "UPDATE impart_data.xiuxian_impart SET stone_num=stone_num-? "
                "WHERE user_id=? AND stone_num=?",
                (10, "u", 460),
            )
            self.assertEqual(stale.rowcount, 0)
            changed_many = conn.executemany(
                "UPDATE xiuxian_impart SET stone_num=stone_num-? WHERE user_id=? AND stone_num=?",
                ((10, "u", 670), (10, "u", 460), (3, "v", 100)),
            )
            self.assertEqual(changed_many.rowcount, 2)
        self.assertEqual(
            db_backend.query_all(
                self.database,
                "SELECT stone_num FROM xiuxian_impart ORDER BY rowid",
            ),
            [{"stone_num": 660}, {"stone_num": 460}, {"stone_num": 97}, {"stone_num": 30}],
        )

        self.assertEqual(
            db_backend.query_all(
                attached,
                "SELECT stone_num FROM xiuxian_impart ORDER BY rowid",
            ),
            [{"stone_num": 660}, {"stone_num": 460}, {"stone_num": 100}, {"stone_num": 30}],
        )

    def test_impart_rewrite_preserves_literals_comments_and_explicit_row_ids(self) -> None:
        statements = (
            "SELECT 'UPDATE xiuxian_impart SET stone_num=1 WHERE user_id=?'",
            "-- UPDATE xiuxian_impart SET stone_num=1 WHERE user_id=?\nSELECT 1",
            "/* UPDATE xiuxian_impart SET stone_num=1 WHERE user_id=? */ SELECT 1",
            "UPDATE another_table SET note='WHERE user_id=?' WHERE user_id=?",
            "UPDATE xiuxian_impart SET stone_num=? WHERE rowid=? AND user_id=?",
            "UPDATE xiuxian_impart SET stone_num=? WHERE rowid=(SELECT MIN(rowid) "
            "FROM xiuxian_impart WHERE user_id=? GROUP BY user_id)",
        )
        for statement in statements:
            with self.subTest(statement=statement):
                self.assertEqual(db_backend._scope_effective_impart_update(statement), statement)
        statement = (
            "UPDATE xiuxian_impart SET note='WHERE user_id=?',stone_num=? "
            "WHERE user_id=? AND stone_num=?;"
        )
        self.assertEqual(
            db_backend._scope_effective_impart_update(statement),
            "UPDATE xiuxian_impart SET note='WHERE user_id=?',stone_num=? "
            "WHERE rowid=(SELECT rowid FROM xiuxian_impart WHERE user_id=? "
            "ORDER BY rowid LIMIT 1) AND stone_num=?;",
        )


if __name__ == "__main__":
    unittest.main()
