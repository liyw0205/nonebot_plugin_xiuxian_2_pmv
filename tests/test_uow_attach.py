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


if __name__ == "__main__":
    unittest.main()
