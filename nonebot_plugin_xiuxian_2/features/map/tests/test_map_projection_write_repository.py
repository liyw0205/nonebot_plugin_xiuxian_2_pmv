import tempfile
import unittest
from pathlib import Path

from ..repository import MapProjectionSqlWriteRepository
from tests.test_db_backend import db_backend


class MapProjectionWriteRepositoryTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.database = Path(self.tmp.name) / "player.db"
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "CREATE TABLE map_daily_limit ("
                "user_id TEXT PRIMARY KEY,date TEXT,gather_count INTEGER,combat_count INTEGER,"
                "explore_count INTEGER,resource_total_count INTEGER)"
            )
            conn.execute(
                "CREATE TABLE map_cooldown (user_id TEXT PRIMARY KEY,gather_cd_until TEXT)"
            )
        self.repository = MapProjectionSqlWriteRepository(self.database)

    def tearDown(self):
        self.tmp.cleanup()

    def test_save_daily_limit_upserts_complete_state(self):
        state = {
            "date": "2026-09-22",
            "gather_count": 2,
            "combat_count": 1,
            "explore_count": 3,
            "resource_total_count": 6,
        }
        self.assertEqual(self.repository.save_daily_limit("u", state), state)
        self.assertEqual(self.repository.save_daily_limit("u", {**state, "gather_count": 3})["gather_count"], 3)
        with db_backend.connection(self.database) as conn:
            row = conn.execute(
                "SELECT date,gather_count,combat_count,explore_count,resource_total_count "
                "FROM map_daily_limit WHERE user_id='u'"
            ).fetchone()
        self.assertEqual(tuple(row), ("2026-09-22", 3, 1, 3, 6))

    def test_set_cooldown_adds_missing_known_column_and_upserts_user(self):
        self.assertEqual(
            self.repository.set_cooldown("u", "explore_start_cd_until", "2026-09-22 10:00:00"),
            "2026-09-22 10:00:00",
        )
        with db_backend.connection(self.database) as conn:
            self.assertIn("explore_start_cd_until", conn.column_names("map_cooldown"))
            row = conn.execute(
                "SELECT explore_start_cd_until FROM map_cooldown WHERE user_id='u'"
            ).fetchone()
        self.assertEqual(row[0], "2026-09-22 10:00:00")

    def test_unknown_cooldown_field_is_rejected(self):
        with self.assertRaises(ValueError):
            self.repository.set_cooldown("u", "not_a_cooldown", "value")

    def test_legacy_daily_schema_is_extended_before_upsert(self):
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "legacy-player.db"
            with db_backend.transaction(database) as conn:
                conn.execute(
                    "CREATE TABLE map_daily_limit ("
                    "user_id TEXT PRIMARY KEY,date TEXT,gather_count INTEGER,resource_total_count INTEGER)"
                )
            repository = MapProjectionSqlWriteRepository(database)
            result = repository.save_daily_limit(
                "u",
                {"date": "2026-09-22", "gather_count": 1, "combat_count": 2, "explore_count": 3, "resource_total_count": 6},
            )
            self.assertEqual(result["explore_count"], 3)
            with db_backend.connection(database) as conn:
                self.assertIn("combat_count", conn.column_names("map_daily_limit"))
                self.assertIn("explore_count", conn.column_names("map_daily_limit"))


if __name__ == "__main__":
    unittest.main()
