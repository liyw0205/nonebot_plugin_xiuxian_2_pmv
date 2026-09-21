import tempfile
import unittest
from pathlib import Path

from ..repository import MapExploreStatusSqlQueryRepository
from tests.test_db_backend import db_backend


class MapExploreStatusQueryRepositoryTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.database = Path(self.tmp.name) / "player.db"
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "CREATE TABLE map_explore_status ("
                "user_id TEXT PRIMARY KEY,running INTEGER,node_type TEXT,node_name TEXT,"
                "start_time TEXT,duration_min INTEGER,settlement TEXT,max_duration_min INTEGER,"
                "interval_min INTEGER,reward_plan TEXT)"
            )
            conn.execute(
                "INSERT INTO map_explore_status VALUES "
                "('u',1,'遗迹','古迹','2026-09-22 01:00:00',20,'',120,20,?)",
                ('{"stone": 3}',),
            )
        self.repository = MapExploreStatusSqlQueryRepository(self.database)

    def tearDown(self):
        self.tmp.cleanup()

    def test_existing_status_prefers_settlement_snapshot(self):
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "UPDATE map_explore_status SET settlement=?,reward_plan=? WHERE user_id='u'",
                ('{"stone": 8}', '[1,2]'),
            )
        result = self.repository.get("u")
        self.assertEqual(result["settlement"], '{"stone": 8}')
        self.assertEqual(result["running"], 1)

    def test_legacy_object_reward_plan_is_used_as_settlement_snapshot(self):
        result = self.repository.get("u")
        self.assertEqual(result["settlement"], '{"stone": 3}')
        self.assertEqual(result["reward_plan"], "")

    def test_missing_status_returns_none(self):
        self.assertIsNone(self.repository.get("missing"))

    def test_null_legacy_reward_plan_is_normalized_to_empty_snapshot(self):
        with db_backend.transaction(self.database) as conn:
            conn.execute("UPDATE map_explore_status SET settlement='',reward_plan='null' WHERE user_id='u'")
        result = self.repository.get("u")
        self.assertEqual(result["settlement"], "")
        self.assertEqual(result["reward_plan"], "")

    def test_modern_schema_without_legacy_reward_plan_is_supported(self):
        with db_backend.transaction(self.database) as conn:
            conn.execute("DROP TABLE map_explore_status")
            conn.execute(
                "CREATE TABLE map_explore_status ("
                "user_id TEXT PRIMARY KEY,running INTEGER,node_type TEXT,node_name TEXT,"
                "start_time TEXT,duration_min INTEGER,settlement TEXT,max_duration_min INTEGER,"
                "interval_min INTEGER)"
            )
            conn.execute(
                "INSERT INTO map_explore_status VALUES ('u',0,'','','',0,'',0,0)"
            )
        result = self.repository.get("u")
        self.assertEqual(result["settlement"], "")
        self.assertEqual(result["reward_plan"], "")


if __name__ == "__main__":
    unittest.main()
