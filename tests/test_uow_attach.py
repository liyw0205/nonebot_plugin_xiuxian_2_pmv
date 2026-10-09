import sqlite3
import tempfile
import unittest
from pathlib import Path

from nonebot_plugin_xiuxian_2.infrastructure.database.uow import DatabaseUnitOfWork


class UnitOfWorkAttachTests(unittest.TestCase):
    def test_attach_and_detach_secondary_database(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            main = root / "main.db"
            secondary = root / "secondary.db"
            with sqlite3.connect(secondary) as conn:
                conn.execute("CREATE TABLE values_table (value TEXT)")
            with DatabaseUnitOfWork(main, immediate=True) as uow:
                uow.attach_database(secondary, "player_data")
                uow.execute("CREATE TABLE main_table (value TEXT)")
                uow.execute("INSERT INTO main_table VALUES (?)", ("main",))
                uow.execute("INSERT INTO player_data.values_table VALUES (?)", ("player",))
                self.assertEqual(uow.query_one("SELECT value FROM player_data.values_table"), {"value": "player"})

            with sqlite3.connect(main) as conn:
                self.assertEqual(conn.execute("SELECT value FROM main_table").fetchone()[0], "main")

    def test_read_only_attach_cannot_write_secondary_database(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            main = root / "main.db"
            secondary = root / "secondary.db"
            with sqlite3.connect(main) as conn:
                conn.execute("CREATE TABLE main_table (value TEXT)")
            with sqlite3.connect(secondary) as conn:
                conn.execute("CREATE TABLE values_table (value TEXT)")
                conn.execute("INSERT INTO values_table VALUES (?)", ("player",))
            with DatabaseUnitOfWork(main, read_only=True, query_only=True) as uow:
                uow.attach_database(secondary, "player_data", read_only=True)
                self.assertEqual(
                    uow.query_one("SELECT value FROM player_data.values_table"),
                    {"value": "player"},
                )
                with self.assertRaises(sqlite3.OperationalError):
                    uow.execute("INSERT INTO player_data.values_table VALUES (?)", ("write",))
                with self.assertRaises(sqlite3.OperationalError):
                    uow.execute("INSERT INTO main_table VALUES (?)", ("write",))
            with sqlite3.connect(secondary) as conn:
                self.assertEqual(
                    [row[0] for row in conn.execute("SELECT value FROM values_table")],
                    ["player"],
                )

    def test_require_exists_never_creates_a_missing_database(self):
        with tempfile.TemporaryDirectory() as directory:
            missing = Path(directory) / "nested" / "message.db"
            with self.assertRaises(FileNotFoundError):
                with DatabaseUnitOfWork(missing, require_exists=True):
                    self.fail("a missing database must not open a transaction")
            self.assertFalse(missing.exists())
            self.assertFalse(missing.parent.exists())

    def test_require_exists_writes_an_existing_database(self):
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "message.db"
            with sqlite3.connect(database) as conn:
                conn.execute("CREATE TABLE messages (id INTEGER PRIMARY KEY, content TEXT)")
                conn.execute("INSERT INTO messages (content) VALUES (?)", ("original",))
            with DatabaseUnitOfWork(database, require_exists=True) as uow:
                cursor = uow.execute("UPDATE messages SET content=? WHERE id=1", ("recalled",))
                self.assertEqual(cursor.rowcount, 1)
            with sqlite3.connect(database) as conn:
                self.assertEqual(conn.execute("SELECT content FROM messages").fetchone()[0], "recalled")

    def test_rolled_back_require_exists_transaction_keeps_previous_state(self):
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "message.db"
            with sqlite3.connect(database) as conn:
                conn.execute("CREATE TABLE messages (id INTEGER PRIMARY KEY, content TEXT)")
                conn.execute("INSERT INTO messages (content) VALUES (?)", ("original",))
            with self.assertRaises(RuntimeError):
                with DatabaseUnitOfWork(database, require_exists=True) as uow:
                    uow.execute("UPDATE messages SET content=? WHERE id=1", ("recalled",))
                    raise RuntimeError("boom")
            with sqlite3.connect(database) as conn:
                self.assertEqual(conn.execute("SELECT content FROM messages").fetchone()[0], "original")


if __name__ == "__main__":
    unittest.main()
