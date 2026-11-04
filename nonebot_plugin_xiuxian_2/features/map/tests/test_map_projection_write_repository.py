import tempfile
import unittest
from pathlib import Path

from ..migrations import apply_map_player_state_schema
from ..repository import MapProjectionSqlWriteRepository
from ....infrastructure.database import DatabaseUnitOfWork
from tests.test_db_backend import db_backend


class MapProjectionWriteRepositoryTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.database = Path(self.tmp.name) / "player.db"
        with DatabaseUnitOfWork(str(self.database), immediate=True) as uow:
            apply_map_player_state_schema(uow)
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

    def test_set_cooldown_upserts_with_the_migrated_columns(self):
        self.assertEqual(
            self.repository.set_cooldown("u", "combat_cd_until", "2026-09-22 10:00:00"),
            "2026-09-22 10:00:00",
        )
        with db_backend.connection(self.database) as conn:
            row = conn.execute(
                "SELECT combat_cd_until FROM map_cooldown WHERE user_id='u'"
            ).fetchone()
        self.assertEqual(row[0], "2026-09-22 10:00:00")

    def test_unknown_cooldown_field_is_rejected(self):
        with self.assertRaises(ValueError):
            self.repository.set_cooldown("u", "not_a_cooldown", "value")

    def test_legacy_daily_schema_without_columns_is_rejected(self):
        """`map.018` owns the columns; the request path must not add them."""
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
            self.assertEqual(result["status"], "schema_missing")
            with db_backend.connection(database) as conn:
                columns = conn.column_names("map_daily_limit")
            self.assertNotIn("combat_count", columns)
            self.assertNotIn("explore_count", columns)


if __name__ == "__main__":
    unittest.main()
