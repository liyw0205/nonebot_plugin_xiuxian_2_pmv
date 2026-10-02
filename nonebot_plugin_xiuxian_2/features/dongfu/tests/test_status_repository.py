import tempfile
import unittest
from pathlib import Path

from ....infrastructure.database import DatabaseUnitOfWork
from ...map.migrations import apply_map_dongfu_status_schema
from ..status_repository import DongfuStatusSqlQueryRepository


class DongfuStatusQueryRepositoryTests(unittest.TestCase):
    def test_reads_existing_projection_without_writing_or_normalizing(self):
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "player.db"
            with DatabaseUnitOfWork(database) as uow:
                uow.execute(
                    "CREATE TABLE dongfu_status ("
                    "user_id TEXT PRIMARY KEY,built INTEGER,plant_slots TEXT)"
                )
                uow.execute(
                    "INSERT INTO dongfu_status VALUES(?,?,?)",
                    ("u", 1, '[{"slot": 1, "seed_id": 21001}]'),
                )

            result = DongfuStatusSqlQueryRepository(database).get("u")

            self.assertEqual(1, result["built"])
            self.assertEqual([{"slot": 1, "seed_id": 21001}], result["plant_slots"])
            with DatabaseUnitOfWork(database, read_only=True) as uow:
                columns = {row["name"] for row in uow.query_all("PRAGMA table_info(dongfu_status)")}
                self.assertEqual({"user_id", "built", "plant_slots"}, columns)

    def test_missing_database_or_table_is_fail_closed_without_creation(self):
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "missing.db"
            repository = DongfuStatusSqlQueryRepository(database)
            self.assertIsNone(repository.get("u"))
            self.assertFalse(database.exists())

            database.touch()
            self.assertIsNone(repository.get("u"))
            with DatabaseUnitOfWork(database, read_only=True) as uow:
                self.assertIsNone(
                    uow.query_one(
                        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='dongfu_status'"
                    )
                )

    def test_startup_schema_migration_extends_legacy_row_and_preserves_values(self):
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "player.db"
            with DatabaseUnitOfWork(database) as uow:
                uow.execute(
                    "CREATE TABLE dongfu_status ("
                    "user_id TEXT PRIMARY KEY,built INTEGER,realm TEXT,node_type TEXT)"
                )
                uow.execute("INSERT INTO dongfu_status VALUES('u',1,'仙界','水域')")
                apply_map_dongfu_status_schema(uow)

            status = DongfuStatusSqlQueryRepository(database).get("u")
            self.assertEqual((1, "仙界", "水域"), (status["built"], status["realm"], status["node_type"]))
            self.assertEqual(3, status["plot_count"])
            self.assertEqual("", status["plant_slots"])


if __name__ == "__main__":
    unittest.main()
