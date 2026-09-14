import sqlite3
import tempfile
import unittest
from pathlib import Path

from ..repository import DaoBattleSqlRepository


class DaoBattleRepositoryTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        root = Path(self.tmp.name)
        self.player, self.game = root / "player.db", root / "game.db"
        with sqlite3.connect(self.game) as conn:
            conn.execute("CREATE TABLE user_xiuxian (user_id TEXT PRIMARY KEY)")
            conn.executemany("INSERT INTO user_xiuxian VALUES (?)", [("a",), ("b",)])
        with sqlite3.connect(self.player) as conn:
            conn.execute("CREATE TABLE map_status (user_id TEXT PRIMARY KEY,realm TEXT,heaven TEXT,node_id TEXT)")
            conn.executemany("INSERT INTO map_status VALUES (?,?,?,?)", [("a", "凡界", "一重天", "n1"), ("b", "凡界", "一重天", "n1")])
            conn.execute("CREATE TABLE dao_record (user_id TEXT PRIMARY KEY,total INTEGER NOT NULL DEFAULT 0,win INTEGER NOT NULL DEFAULT 0,lose INTEGER NOT NULL DEFAULT 0)")
            conn.execute("CREATE TABLE map_dao_battle_operations (operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL)")
        self.repo = DaoBattleSqlRepository(self.player, self.game)

    def tearDown(self):
        self.tmp.cleanup()

    def test_symmetric_settlement_and_replay(self):
        position = {"realm": "凡界", "heaven": "一重天", "node_id": "n1"}
        first = self.repo.settle("op", "a", "b", position, True)
        second = self.repo.settle("op", "a", "b", position, True)
        self.assertEqual(first["status"], "applied")
        self.assertEqual(second["status"], "duplicate")
        with sqlite3.connect(self.player) as conn:
            self.assertEqual(conn.execute("SELECT user_id,total,win,lose FROM dao_record ORDER BY user_id").fetchall(), [("a", 1, 1, 0), ("b", 1, 0, 1)])

    def test_position_change_rejects_without_records(self):
        with sqlite3.connect(self.player) as conn:
            conn.execute("UPDATE map_status SET node_id='n2' WHERE user_id='b'")
        result = self.repo.settle("moved", "a", "b", {"realm": "凡界", "heaven": "一重天", "node_id": "n1"}, True)
        self.assertEqual(result["status"], "position_changed")


if __name__ == "__main__":
    unittest.main()
