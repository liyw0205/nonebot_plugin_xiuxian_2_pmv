import tempfile
import unittest
from pathlib import Path

from ..repository import MapMissionSqlQueryRepository
from tests.test_db_backend import db_backend


class MapMissionQueryRepositoryTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.database = Path(self.tmp.name) / "player.db"
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "CREATE TABLE map_mission ("
                "user_id TEXT PRIMARY KEY,date TEXT,mission_type TEXT,target INTEGER,"
                "claimed INTEGER,settlement TEXT)"
            )
            conn.execute(
                "INSERT INTO map_mission VALUES ('u','2026-09-22','gather',5,0,'snapshot')"
            )
        self.repository = MapMissionSqlQueryRepository(self.database)

    def tearDown(self):
        self.tmp.cleanup()

    def test_existing_mission_is_returned(self):
        self.assertEqual(
            self.repository.get("u"),
            {
                "date": "2026-09-22",
                "mission_type": "gather",
                "target": 5,
                "claimed": 0,
                "settlement": "snapshot",
            },
        )

    def test_missing_mission_returns_none(self):
        self.assertIsNone(self.repository.get("missing"))

    def test_null_fields_are_normalized(self):
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "UPDATE map_mission SET mission_type=NULL,target=NULL,claimed=NULL,settlement=NULL WHERE user_id='u'"
            )
        self.assertEqual(
            self.repository.get("u"),
            {
                "date": "2026-09-22",
                "mission_type": "",
                "target": 0,
                "claimed": 0,
                "settlement": "",
            },
        )


if __name__ == "__main__":
    unittest.main()
