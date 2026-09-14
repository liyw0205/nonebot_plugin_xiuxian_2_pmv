import sqlite3
import tempfile
import unittest
from pathlib import Path

from ..repository import MapMovementSqlRepository


class MapMovementRepositoryTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        root = Path(self.tmp.name)
        self.game = root / "game.db"
        self.player = root / "player.db"
        with sqlite3.connect(self.game) as conn:
            conn.execute("CREATE TABLE user_xiuxian (user_id TEXT PRIMARY KEY, user_stamina INTEGER)")
            conn.execute("INSERT INTO user_xiuxian VALUES ('u', 20)")
            conn.execute("CREATE TABLE map_movement_operations (operation_id TEXT PRIMARY KEY, payload TEXT NOT NULL, stamina INTEGER NOT NULL)")
        with sqlite3.connect(self.player) as conn:
            conn.execute("CREATE TABLE map_status (user_id TEXT PRIMARY KEY, realm TEXT, heaven TEXT, node_id TEXT, visited_nodes TEXT)")
            conn.execute("INSERT INTO map_status VALUES ('u', '凡界', '一重天', 'n1', '[\"n1\"]')")

    def tearDown(self):
        self.tmp.cleanup()

    def test_move_and_replay(self):
        repo = MapMovementSqlRepository(self.game, self.player)
        kwargs: dict[str, object] = dict(operation_id="move-1", user_id="u", expected_position={"realm": "凡界", "heaven": "一重天", "node_id": "n1"}, target_position={"realm": "凡界", "heaven": "一重天", "node_id": "n2"}, expected_stamina=20, cost=5)
        first = repo.move(**kwargs)  # type: ignore[arg-type]
        second = repo.move(**kwargs)  # type: ignore[arg-type]
        self.assertEqual((first["status"], first["stamina"], second["status"]), ("applied", 15, "duplicate"))
        with sqlite3.connect(self.player) as conn:
            self.assertEqual(conn.execute("SELECT node_id,visited_nodes FROM map_status").fetchone(), ("n2", '["n1", "n2"]'))

    def test_stale_snapshot_does_not_write(self):
        repo = MapMovementSqlRepository(self.game, self.player)
        result = repo.move("move-2", "u", {"realm": "凡界", "heaven": "一重天", "node_id": "n2"}, {"realm": "凡界", "heaven": "一重天", "node_id": "n3"}, 20, 5)
        self.assertEqual(result["status"], "state_changed")


if __name__ == "__main__":
    unittest.main()
