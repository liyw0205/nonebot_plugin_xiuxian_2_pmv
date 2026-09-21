import tempfile
import unittest
from pathlib import Path

from ..repository import MapExploreStatusSqlWriteRepository
from tests.test_db_backend import db_backend


class MapExploreStatusWriteRepositoryTests(unittest.TestCase):
    def test_save_modern_status(self):
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "player.db"
            with db_backend.transaction(database) as conn:
                conn.execute(
                    "CREATE TABLE map_explore_status ("
                    "user_id TEXT PRIMARY KEY,running INTEGER,node_type TEXT,node_name TEXT,start_time TEXT,"
                    "duration_min INTEGER,settlement TEXT,max_duration_min INTEGER,interval_min INTEGER)"
                )
            repository = MapExploreStatusSqlWriteRepository(database)
            state = {
                "running": 1,
                "node_type": "遗迹",
                "node_name": "古迹",
                "start_time": "2026-09-22 10:00:00",
                "duration_min": 20,
                "settlement": '{"stone": 3}',
                "max_duration_min": 120,
                "interval_min": 20,
            }
            self.assertEqual(repository.save("u", state), state)

    def test_legacy_reward_plan_schema_is_upgraded_and_cleared(self):
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "player.db"
            with db_backend.transaction(database) as conn:
                conn.execute(
                    "CREATE TABLE map_explore_status ("
                    "user_id TEXT PRIMARY KEY,running INTEGER,node_type TEXT,node_name TEXT,"
                    "start_time TEXT,duration_min INTEGER,max_duration_min INTEGER,interval_min INTEGER,reward_plan TEXT)"
                )
            repository = MapExploreStatusSqlWriteRepository(database)
            repository.save(
                "u",
                {
                    "running": 1,
                    "node_type": "遗迹",
                    "node_name": "古迹",
                    "start_time": "2026-09-22 10:00:00",
                    "duration_min": 20,
                    "settlement": '{"stone": 3}',
                    "max_duration_min": 120,
                    "interval_min": 20,
                },
            )
            with db_backend.connection(database) as conn:
                self.assertIn("settlement", conn.column_names("map_explore_status"))
                row = conn.execute("SELECT settlement,reward_plan FROM map_explore_status WHERE user_id='u'").fetchone()
            self.assertEqual(tuple(row), ('{"stone": 3}', ""))

    def test_missing_schema_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaises(RuntimeError):
                MapExploreStatusSqlWriteRepository(Path(directory) / "player.db").save("u", {"running": 0})


if __name__ == "__main__":
    unittest.main()
