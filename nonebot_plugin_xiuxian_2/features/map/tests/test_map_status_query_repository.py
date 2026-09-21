import json
import tempfile
import unittest
from pathlib import Path

from ..repository import MapStatusSqlQueryRepository
from tests.test_db_backend import db_backend


class MapStatusQueryRepositoryTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.database = Path(self.tmp.name) / "player.db"
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "CREATE TABLE map_status ("
                "user_id TEXT PRIMARY KEY, realm TEXT, heaven TEXT, node_id TEXT, visited_nodes TEXT)"
            )
            conn.execute(
                "INSERT INTO map_status VALUES ('u','凡界','一重天','n1',?)",
                (json.dumps(["n1", "n2"], ensure_ascii=False),),
            )
        self.repository = MapStatusSqlQueryRepository(self.database)

    def tearDown(self):
        self.tmp.cleanup()

    def test_existing_status_is_returned_with_normalized_visited_nodes(self):
        self.assertEqual(
            self.repository.get("u"),
            {
                "user_id": "u",
                "realm": "凡界",
                "heaven": "一重天",
                "node_id": "n1",
                "visited_nodes": ["n1", "n2"],
            },
        )

    def test_missing_status_returns_none(self):
        self.assertIsNone(self.repository.get("missing"))

    def test_invalid_visited_nodes_are_normalized_to_empty_list(self):
        with db_backend.transaction(self.database) as conn:
            conn.execute("UPDATE map_status SET visited_nodes=? WHERE user_id='u'", ("invalid",))
        result = self.repository.get("u")
        self.assertEqual(result["visited_nodes"], [])


if __name__ == "__main__":
    unittest.main()
