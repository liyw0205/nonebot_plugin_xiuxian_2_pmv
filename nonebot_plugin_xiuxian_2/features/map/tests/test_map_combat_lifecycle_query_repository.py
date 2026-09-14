import json
import tempfile
import unittest
from pathlib import Path

from ..repository import MapCombatLifecycleQueryRepository
from tests.test_db_backend import db_backend


class CombatLifecycleQueryRepositoryTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        root = Path(self.temp.name)
        self.game, self.player = root / "game.db", root / "player.db"
        with db_backend.transaction(self.game) as conn:
            conn.execute("CREATE TABLE map_combat_start_operations(operation_id TEXT PRIMARY KEY,payload TEXT,result_status TEXT,stamina INTEGER,task_json TEXT)")
            task = {"task_id": "op", "status": "running"}
            conn.execute("INSERT INTO map_combat_start_operations VALUES(?,?,?,?,?)", ("op", json.dumps(["u"], separators=(",", ":")), "applied", 4, json.dumps(task)))
        with db_backend.transaction(self.player) as conn:
            conn.execute("CREATE TABLE map_combat_settlement(user_id TEXT PRIMARY KEY,snapshot TEXT)")
            conn.execute("INSERT INTO map_combat_settlement VALUES(?,?)", ("u", json.dumps({"task_id": "op", "status": "running"})))
        self.repo = MapCombatLifecycleQueryRepository(self.game, self.player)

    def tearDown(self):
        self.temp.cleanup()

    def test_replay_and_conflict(self):
        replay = self.repo.replay_start("op", "u")
        assert replay is not None
        self.assertEqual("duplicate", replay["status"])
        conflict = self.repo.replay_start("op", "other")
        assert conflict is not None
        self.assertEqual("operation_conflict", conflict["status"])

    def test_pending_snapshot_is_parsed(self):
        pending = self.repo.get_pending("u")
        assert pending is not None
        self.assertEqual(("pending", "op"), (pending["status"], pending["task"]["task_id"]))

    def test_missing_pending_is_none(self):
        self.assertIsNone(self.repo.get_pending("missing"))


if __name__ == "__main__":
    unittest.main()
