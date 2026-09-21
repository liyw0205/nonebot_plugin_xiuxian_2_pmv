import tempfile
import unittest
from pathlib import Path

from ..repository import MapProjectionSqlRepository
from tests.test_db_backend import db_backend


class MapProjectionRepositoryTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.database = Path(self.tmp.name) / "player.db"
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "CREATE TABLE map_daily_limit ("
                "user_id TEXT PRIMARY KEY,date TEXT,gather_count INTEGER,"
                "combat_count INTEGER,explore_count INTEGER,resource_total_count INTEGER)"
            )
            conn.execute(
                "CREATE TABLE map_cooldown ("
                "user_id TEXT PRIMARY KEY,gather_cd_until TEXT,"
                "combat_cd_until TEXT,explore_start_cd_until TEXT)"
            )
            conn.execute(
                "INSERT INTO map_daily_limit VALUES ('u','2026-09-21',2,1,0,4)"
            )
            conn.execute(
                "INSERT INTO map_cooldown VALUES ('u','2026-09-21 12:00:00',NULL,'')"
            )
        self.repository = MapProjectionSqlRepository(self.database)

    def tearDown(self):
        self.tmp.cleanup()

    def test_same_day_daily_limit_preserves_counts(self):
        self.assertEqual(
            self.repository.daily_limit("u", "2026-09-21"),
            {
                "date": "2026-09-21",
                "gather_count": 2,
                "combat_count": 1,
                "explore_count": 0,
                "resource_total_count": 4,
            },
        )

    def test_new_day_resets_daily_limit_atomically(self):
        self.assertEqual(
            self.repository.daily_limit("u", "2026-09-22"),
            {
                "date": "2026-09-22",
                "gather_count": 0,
                "combat_count": 0,
                "explore_count": 0,
                "resource_total_count": 0,
            },
        )
        with db_backend.connection(self.database) as conn:
            self.assertEqual(
                tuple(conn.execute("SELECT date,gather_count,combat_count,explore_count,resource_total_count FROM map_daily_limit WHERE user_id='u'").fetchone()),
                ("2026-09-22", 0, 0, 0, 0),
            )

    def test_missing_daily_limit_is_created(self):
        result = self.repository.daily_limit("new", "2026-09-22")
        self.assertEqual(result["date"], "2026-09-22")
        self.assertEqual(result["resource_total_count"], 0)

    def test_legacy_daily_schema_missing_counts_reads_missing_values_as_zero(self):
        with db_backend.transaction(self.database) as conn:
            conn.execute("DROP TABLE map_daily_limit")
            conn.execute(
                "CREATE TABLE map_daily_limit ("
                "user_id TEXT PRIMARY KEY,date TEXT,gather_count INTEGER,resource_total_count INTEGER)"
            )
            conn.execute("INSERT INTO map_daily_limit VALUES ('u','2026-09-22',2,5)")
        self.assertEqual(
            self.repository.daily_limit("u", "2026-09-22"),
            {
                "date": "2026-09-22",
                "gather_count": 2,
                "combat_count": 0,
                "explore_count": 0,
                "resource_total_count": 5,
            },
        )

    def test_cooldown_reads_whitelisted_field(self):
        self.assertEqual(
            self.repository.cooldown_until("u", "gather_cd_until"),
            "2026-09-21 12:00:00",
        )
        self.assertEqual(self.repository.cooldown_until("u", "combat_cd_until"), None)

    def test_cooldown_rejects_unknown_field(self):
        with self.assertRaises(ValueError):
            self.repository.cooldown_until("u", "arbitrary_sql")


if __name__ == "__main__":
    unittest.main()
