import tempfile
import unittest
from pathlib import Path

from ..repository import MapNearbyPlayersSqlQueryRepository
from tests.test_db_backend import db_backend


class MapNearbyPlayersQueryRepositoryTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.player = Path(self.tmp.name) / "player.db"
        self.game = Path(self.tmp.name) / "game.db"
        with db_backend.transaction(self.player) as conn:
            conn.execute(
                "CREATE TABLE map_status ("
                "user_id TEXT PRIMARY KEY,realm TEXT,heaven TEXT,node_id TEXT)"
            )
            conn.executemany(
                "INSERT INTO map_status VALUES (?,?,?,?)",
                [("u1", "凡界", "一重天", "n1"), ("u2", "凡界", "一重天", "n1"), ("u3", "凡界", "一重天", "n2")],
            )
        with db_backend.transaction(self.game) as conn:
            conn.execute(
                "CREATE TABLE user_xiuxian ("
                "user_id TEXT PRIMARY KEY,user_name TEXT,level TEXT,power INTEGER)"
            )
            conn.executemany(
                "INSERT INTO user_xiuxian VALUES (?,?,?,?)",
                [("u1", "甲", "练气一层", 10), ("u2", "乙", "筑基一层", 20)],
            )
        self.repository = MapNearbyPlayersSqlQueryRepository(self.player, self.game)

    def tearDown(self):
        self.tmp.cleanup()

    def test_same_node_returns_public_profiles(self):
        self.assertEqual(
            self.repository.list("凡界", "一重天", "n1"),
            [
                {"user_id": "u1", "user_name": "甲", "level": "练气一层", "power": 10},
                {"user_id": "u2", "user_name": "乙", "level": "筑基一层", "power": 20},
            ],
        )

    def test_different_node_and_missing_profile_are_excluded(self):
        with db_backend.transaction(self.player) as conn:
            conn.execute("INSERT INTO map_status VALUES ('missing','凡界','一重天','n1')")
        self.assertEqual([row["user_id"] for row in self.repository.list("凡界", "一重天", "n1")], ["u1", "u2"])

    def test_empty_position_returns_empty(self):
        self.assertEqual(self.repository.list("", "一重天", "n1"), [])


if __name__ == "__main__":
    unittest.main()
