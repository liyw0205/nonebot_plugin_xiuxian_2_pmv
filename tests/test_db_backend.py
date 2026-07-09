from __future__ import annotations

import importlib.util
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


if __name__ == "__main__":
    unittest.main()
