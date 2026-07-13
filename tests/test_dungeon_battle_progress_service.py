import tempfile
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_dungeon.battle_progress_service import DungeonBattleProgressService
from tests.test_db_backend import db_backend


class DungeonBattleProgressServiceTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        root = Path(self.temp.name)
        self.game, self.player = root / "game.db", root / "player.db"
        with db_backend.transaction(self.game) as conn:
            conn.execute("CREATE TABLE user_xiuxian (user_id TEXT PRIMARY KEY,stone INTEGER,exp INTEGER,hp INTEGER,mp INTEGER)")
            conn.execute("INSERT INTO user_xiuxian VALUES (%s,%s,%s,%s,%s)", ("u", 10, 100, 80, 20))
            conn.execute("CREATE TABLE back (user_id TEXT,goods_id INTEGER,goods_name TEXT,goods_type TEXT,goods_num INTEGER,create_time TEXT,update_time TEXT,bind_num INTEGER,UNIQUE(user_id,goods_id))")
        with db_backend.transaction(self.player) as conn:
            conn.execute("CREATE TABLE player_dungeon_status (user_id TEXT PRIMARY KEY,dungeon_id TEXT,dungeon_status TEXT,current_layer INTEGER,total_layers INTEGER,last_reset_date TEXT)")
            conn.execute("INSERT INTO player_dungeon_status VALUES (%s,%s,%s,%s,%s,%s)", ("u", "d", "exploring", 1, 4, "2026-07-13"))
        self.service = DungeonBattleProgressService(self.game, self.player)
        self.expected = {"dungeon_id": "d", "dungeon_status": "exploring", "current_layer": 1, "total_layers": 4, "last_reset_date": "2026-07-13"}
        self.rewards = [{"user_id": "u", "expected_stone": 10, "expected_exp": 100, "stone": 7, "exp": 9, "items": []}]

    def tearDown(self):
        self.temp.cleanup()

    def state(self):
        with db_backend.connection(self.game) as conn:
            user = conn.execute("SELECT stone,exp FROM user_xiuxian WHERE user_id=%s", ("u",)).fetchone()
        with db_backend.connection(self.player) as conn:
            status = conn.execute("SELECT current_layer,dungeon_status FROM player_dungeon_status WHERE user_id=%s", ("u",)).fetchone()
        return tuple(user), tuple(status)

    def test_rewards_and_progress_are_idempotent(self):
        first = self.service.settle("op", "u", self.expected, self.rewards, False, 99)
        duplicate = self.service.settle("op", "u", self.expected, self.rewards, False, 99)
        self.assertEqual((first.status, duplicate.status), ("applied", "duplicate"))
        self.assertEqual(self.state(), ((17, 109), (2, "exploring")))

    def test_resource_snapshot_change_rejects_progress(self):
        rewards = [dict(self.rewards[0], expected_exp=99)]
        self.assertEqual(self.service.settle("stale", "u", self.expected, rewards, False, 99).status, "state_changed")
        self.assertEqual(self.state(), ((10, 100), (1, "exploring")))

    def test_operation_failure_rolls_back_reward_and_progress(self):
        with db_backend.transaction(self.game) as conn:
            conn.execute("CREATE TABLE dungeon_battle_progress_operations (operation_id TEXT PRIMARY KEY,payload TEXT,current_layer INTEGER,dungeon_status TEXT,created_at TIMESTAMP)")
            conn.execute("CREATE TRIGGER fail_battle BEFORE INSERT ON dungeon_battle_progress_operations BEGIN SELECT RAISE(ABORT,'fail'); END")
        with self.assertRaises(db_backend.IntegrityError):
            self.service.settle("rollback", "u", self.expected, self.rewards, True, 99)
        self.assertEqual(self.state(), ((10, 100), (1, "exploring")))

    def test_real_entry_has_no_reward_or_progress_write_bypass(self):
        source = (Path(__file__).parents[1] / "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_dungeon/__init__.py").read_text(encoding="utf-8")
        handler = source[source.index("async def handle_explore_dungeon"):source.index("async def handle_dungeon_status")]
        self.assertIn("dungeon_battle_progress_service.settle", handler)
        self.assertNotIn("dungeon_reward_service.award", handler)
        self.assertNotIn("dungeon_manager.update_player_progress", handler)


if __name__ == "__main__":
    unittest.main()
