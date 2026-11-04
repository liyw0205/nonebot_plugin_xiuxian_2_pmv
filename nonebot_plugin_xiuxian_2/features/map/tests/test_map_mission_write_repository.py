import tempfile
import unittest
from pathlib import Path

from ..repository import MapMissionSqlWriteRepository
from tests.test_db_backend import db_backend


class MapMissionWriteRepositoryTests(unittest.TestCase):
    def test_save_upserts_complete_mission_state(self):
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "player.db"
            with db_backend.transaction(database) as conn:
                conn.execute(
                    "CREATE TABLE map_mission ("
                    "user_id TEXT PRIMARY KEY,date TEXT,mission_type TEXT,target INTEGER,"
                    "claimed INTEGER,settlement TEXT)"
                )
            repository = MapMissionSqlWriteRepository(database)
            state = {
                "date": "2026-09-22",
                "mission_type": "gather",
                "target": 5,
                "claimed": 0,
                "settlement": '{"stone": 10}',
            }
            self.assertEqual(repository.save("u", state), state)
            self.assertEqual(repository.save("u", {**state, "claimed": 1})["claimed"], 1)

    def test_legacy_schema_without_columns_is_rejected(self):
        """`map.018` owns `claimed`/`settlement`; the request path must not add them."""
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "player.db"
            with db_backend.transaction(database) as conn:
                conn.execute(
                    "CREATE TABLE map_mission (user_id TEXT PRIMARY KEY,date TEXT,mission_type TEXT,target INTEGER)"
                )
            repository = MapMissionSqlWriteRepository(database)
            result = repository.save(
                "u",
                {"date": "2026-09-22", "mission_type": "combat", "target": 2, "claimed": 0, "settlement": ""},
            )
            self.assertEqual(result["status"], "schema_missing")
            with db_backend.connection(database) as conn:
                columns = conn.column_names("map_mission")
                self.assertEqual(int(conn.execute("SELECT COUNT(*) FROM map_mission").fetchone()[0]), 0)
            self.assertNotIn("claimed", columns)
            self.assertNotIn("settlement", columns)

    def test_missing_schema_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            result = MapMissionSqlWriteRepository(Path(directory) / "player.db").save(
                "u", {"date": "2026-09-22", "mission_type": "", "target": 0, "claimed": 0, "settlement": ""}
            )
            self.assertEqual(result["status"], "schema_missing")


if __name__ == "__main__":
    unittest.main()
