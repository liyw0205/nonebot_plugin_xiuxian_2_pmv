import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

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

    def test_incomplete_legacy_schema_remains_compatible_without_schema_repair(self):
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "player.db"
            with DatabaseUnitOfWork(database) as uow:
                uow.execute("CREATE TABLE dongfu_status (user_id TEXT PRIMARY KEY)")
                uow.execute("INSERT INTO dongfu_status VALUES('u')")

            self.assertIsNone(DongfuStatusSqlQueryRepository(database).get("u"))
            with DatabaseUnitOfWork(database, read_only=True) as uow:
                self.assertEqual(["user_id"], [row["name"] for row in uow.query_all(
                    "PRAGMA table_info(dongfu_status)"
                )])
                self.assertEqual("u", uow.query_one("SELECT user_id FROM dongfu_status")["user_id"])

    def test_sql_read_failure_is_not_misreported_as_missing_cave(self):
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "player.db"
            with DatabaseUnitOfWork(database) as uow:
                uow.execute(
                    "CREATE TABLE dongfu_status (user_id TEXT PRIMARY KEY,built INTEGER)"
                )
                uow.execute("INSERT INTO dongfu_status VALUES('u',1)")

            repository = DongfuStatusSqlQueryRepository(database)
            original_uow = DatabaseUnitOfWork

            class DenyBuiltReadUnitOfWork:
                def __init__(self, *args, **kwargs):
                    self.delegate = original_uow(*args, **kwargs)

                def __enter__(self):
                    active = self.delegate.__enter__()

                    def authorize(action, arg1, arg2, db_name, trigger):
                        if action == sqlite3.SQLITE_READ and arg1 == "dongfu_status" and arg2 == "built":
                            return sqlite3.SQLITE_DENY
                        return sqlite3.SQLITE_OK

                    active.connection.set_authorizer(authorize)
                    return active

                def __exit__(self, exc_type, exc, traceback):
                    return self.delegate.__exit__(exc_type, exc, traceback)

            with patch(
                "nonebot_plugin_xiuxian_2.features.dongfu.status_repository.DatabaseUnitOfWork",
                DenyBuiltReadUnitOfWork,
            ):
                with self.assertRaisesRegex(sqlite3.DatabaseError, "built is prohibited"):
                    repository.get("u")

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
