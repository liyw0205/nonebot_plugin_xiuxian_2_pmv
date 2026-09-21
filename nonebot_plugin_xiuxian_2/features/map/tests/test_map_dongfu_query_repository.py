import tempfile
import unittest
from pathlib import Path

from ..repository import MapDongfuSqlQueryRepository
from tests.test_db_backend import db_backend


class MapDongfuQueryRepositoryTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.database = Path(self.tmp.name) / "player.db"
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "CREATE TABLE dongfu_status ("
                "user_id TEXT PRIMARY KEY,built INTEGER,realm TEXT,heaven TEXT,"
                "node_id TEXT,node_name TEXT,node_type TEXT)"
            )
            conn.execute(
                "INSERT INTO dongfu_status VALUES ('u',1,'凡界','一重天','n1','青山','山脉')"
            )
        self.repository = MapDongfuSqlQueryRepository(self.database)

    def tearDown(self):
        self.tmp.cleanup()

    def test_existing_dongfu_is_returned(self):
        self.assertEqual(
            self.repository.get("u"),
            {
                "built": 1,
                "realm": "凡界",
                "heaven": "一重天",
                "node_id": "n1",
                "node_name": "青山",
                "node_type": "山脉",
            },
        )

    def test_missing_dongfu_returns_none(self):
        self.assertIsNone(self.repository.get("missing"))


if __name__ == "__main__":
    unittest.main()
