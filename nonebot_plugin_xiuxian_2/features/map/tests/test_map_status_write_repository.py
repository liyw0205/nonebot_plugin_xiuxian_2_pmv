import json
import tempfile
import unittest
from pathlib import Path

from ....core.errors import DomainError
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

    def test_upsert_rejects_legacy_status_schema_without_visited_nodes(self):
        """`map.018` owns `visited_nodes`; the request path must not add it."""
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "player.db"
            with db_backend.transaction(database) as conn:
                conn.execute(
                    "CREATE TABLE map_status (user_id TEXT PRIMARY KEY,realm TEXT,heaven TEXT,node_id TEXT)"
                )
            repository = MapStatusSqlWriteRepository(database)
            with self.assertRaises(DomainError) as raised:
                repository.upsert("u", "凡界", "一重天", "n2", ["n1", "n2"])
            self.assertEqual(raised.exception.details["reason"], "map_status_schema_missing")
            with db_backend.connection(database) as conn:
                self.assertNotIn("visited_nodes", conn.column_names("map_status"))
                self.assertEqual(
                    int(conn.execute("SELECT COUNT(*) FROM map_status").fetchone()[0]), 0
                )

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

    def test_upsert_rolls_back_ignored_write(self):
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "player.db"
            with db_backend.transaction(database) as conn:
                conn.execute(
                    "CREATE TABLE map_status (user_id TEXT PRIMARY KEY,realm TEXT,heaven TEXT,node_id TEXT,visited_nodes TEXT)"
                )
                conn.execute("CREATE TRIGGER ignore_status BEFORE INSERT ON map_status BEGIN SELECT RAISE(IGNORE); END")
            with self.assertRaises(DomainError) as raised:
                MapStatusSqlWriteRepository(database).upsert("u", "凡界", "一重天", "n1", ["n1"])
            self.assertEqual(raised.exception.details["reason"], "map_status_write_rejected")
            with db_backend.connection(database) as conn:
                self.assertEqual(conn.execute("SELECT COUNT(*) FROM map_status").fetchone()[0], 0)

    def test_upsert_rolls_back_rewritten_projection(self):
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "player.db"
            with db_backend.transaction(database) as conn:
                conn.execute(
                    "CREATE TABLE map_status (user_id TEXT PRIMARY KEY,realm TEXT,heaven TEXT,node_id TEXT,visited_nodes TEXT)"
                )
                conn.execute(
                    "CREATE TRIGGER rewrite_status AFTER INSERT ON map_status BEGIN "
                    "UPDATE map_status SET node_id='rewritten' WHERE user_id=NEW.user_id; END"
                )
            with self.assertRaises(DomainError) as raised:
                MapStatusSqlWriteRepository(database).upsert("u", "凡界", "一重天", "n1", ["n1"])
            self.assertEqual(raised.exception.details["reason"], "map_status_projection_mismatch")
            with db_backend.connection(database) as conn:
                self.assertEqual(conn.execute("SELECT COUNT(*) FROM map_status").fetchone()[0], 0)

    def test_upsert_rolls_back_abort_with_original_sqlite_error(self):
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "player.db"
            with db_backend.transaction(database) as conn:
                conn.execute(
                    "CREATE TABLE map_status (user_id TEXT PRIMARY KEY,realm TEXT,heaven TEXT,node_id TEXT,visited_nodes TEXT)"
                )
                conn.execute(
                    "CREATE TRIGGER abort_status BEFORE INSERT ON map_status BEGIN "
                    "SELECT RAISE(ABORT,'isolated position failure'); END"
                )
            with self.assertRaisesRegex(Exception, "isolated position failure"):
                MapStatusSqlWriteRepository(database).upsert("u", "凡界", "一重天", "n1", ["n1"])
            with db_backend.connection(database) as conn:
                self.assertEqual(conn.execute("SELECT COUNT(*) FROM map_status").fetchone()[0], 0)


if __name__ == "__main__":
    unittest.main()
