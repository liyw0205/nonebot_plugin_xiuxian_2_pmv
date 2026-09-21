import json
import tempfile
import unittest
from pathlib import Path

from ..repository import MapStatusSqlWriteRepository
from tests.test_db_backend import db_backend


class MapStatusWriteRepositoryTests(unittest.TestCase):
    def test_upsert_creates_modern_status_row_with_visited_nodes(self):
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "player.db"
            with db_backend.transaction(database) as conn:
                conn.execute(
                    "CREATE TABLE map_status (user_id TEXT PRIMARY KEY,realm TEXT,heaven TEXT,node_id TEXT,visited_nodes TEXT)"
                )
            repository = MapStatusSqlWriteRepository(database)
            self.assertEqual(
                repository.upsert("u", "凡界", "一重天", "n1", ["n1"]),
                {"realm": "凡界", "heaven": "一重天", "node_id": "n1", "visited_nodes": ["n1"]},
            )
            with db_backend.connection(database) as conn:
                row = conn.execute(
                    "SELECT realm,heaven,node_id,visited_nodes FROM map_status WHERE user_id='u'"
                ).fetchone()
            self.assertEqual(tuple(row[:3]), ("凡界", "一重天", "n1"))
            self.assertEqual(json.loads(row[3]), ["n1"])

    def test_upsert_adds_visited_nodes_to_legacy_status_schema(self):
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "player.db"
            with db_backend.transaction(database) as conn:
                conn.execute(
                    "CREATE TABLE map_status (user_id TEXT PRIMARY KEY,realm TEXT,heaven TEXT,node_id TEXT)"
                )
            repository = MapStatusSqlWriteRepository(database)
            result = repository.upsert("u", "凡界", "一重天", "n2", ["n1", "n2"])
            self.assertEqual(result["node_id"], "n2")
            with db_backend.connection(database) as conn:
                self.assertIn("visited_nodes", conn.column_names("map_status"))

    def test_upsert_merges_existing_visited_nodes_without_duplicates(self):
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "player.db"
            with db_backend.transaction(database) as conn:
                conn.execute(
                    "CREATE TABLE map_status (user_id TEXT PRIMARY KEY,realm TEXT,heaven TEXT,node_id TEXT,visited_nodes TEXT)"
                )
            repository = MapStatusSqlWriteRepository(database)
            repository.upsert("u", "凡界", "一重天", "n1", ["n1"])
            result = repository.upsert("u", "凡界", "一重天", "n2", ["n1", "n2"])
            self.assertEqual(result["visited_nodes"], ["n1", "n2"])


if __name__ == "__main__":
    unittest.main()
