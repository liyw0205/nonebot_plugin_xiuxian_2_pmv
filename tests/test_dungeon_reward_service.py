import tempfile
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_dungeon.reward_service import DungeonRewardService
from tests.test_db_backend import db_backend


class DungeonRewardServiceTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.database = Path(self.temp_dir.name) / "game.sqlite3"
        with db_backend.transaction(self.database) as conn:
            conn.execute("CREATE TABLE user_xiuxian (user_id TEXT PRIMARY KEY, stone INTEGER, exp INTEGER)")
            conn.executemany("INSERT INTO user_xiuxian VALUES (%s,%s,%s)", [("a", 1, 10), ("b", 2, 20)])
            conn.execute("CREATE TABLE back (user_id TEXT, goods_id INTEGER, goods_name TEXT, goods_type TEXT, goods_num INTEGER, create_time TEXT, update_time TEXT, bind_num INTEGER, UNIQUE(user_id,goods_id))")
        self.service = DungeonRewardService(self.database)
        self.rewards = [{"user_id": "a", "stone": 3, "exp": 4, "items": [{"id": 1, "name": "item", "type": "type", "amount": 1}]}, {"user_id": "b", "stone": 5, "exp": 6, "items": []}]

    def tearDown(self):
        self.temp_dir.cleanup()

    def state(self):
        with db_backend.connection(self.database) as conn:
            users = conn.execute("SELECT user_id,stone,exp FROM user_xiuxian ORDER BY user_id").fetchall()
            item = conn.execute("SELECT goods_num,bind_num FROM back WHERE user_id=%s AND goods_id=%s", ("a", 1)).fetchone()
        return [tuple(row) for row in users], tuple(map(int, item)) if item else None

    def test_awards_all_members_in_one_transaction(self):
        self.assertEqual(self.service.award("reward", self.rewards, 99).status, "applied")
        self.assertEqual(self.state(), ([("a", 4, 14), ("b", 7, 26)], (1, 1)))

    def test_duplicate_and_failure_do_not_partially_award_team(self):
        self.assertEqual(self.service.award("repeat", self.rewards, 99).status, "applied")
        self.assertEqual(self.service.award("repeat", self.rewards, 99).status, "duplicate")
        with db_backend.transaction(self.database) as conn:
            conn.execute("INSERT INTO back VALUES (%s,%s,%s,%s,%s,%s,%s,%s)", ("a", 2, "full", "type", 99, "", "", 99))
        rewards = [{"user_id": "a", "stone": 9, "exp": 9, "items": [{"id": 2, "name": "full", "type": "type", "amount": 1}]}, {"user_id": "b", "stone": 9, "exp": 9, "items": []}]
        self.assertEqual(self.service.award("full", rewards, 99).status, "inventory_full")
        self.assertEqual(self.state(), ([("a", 4, 14), ("b", 7, 26)], (1, 1)))

    def test_operation_insert_failure_rolls_back_every_member(self):
        with db_backend.transaction(self.database) as conn:
            conn.execute("CREATE TABLE dungeon_reward_operations (operation_id TEXT PRIMARY KEY, payload TEXT NOT NULL, created_at TIMESTAMP)")
            conn.execute("CREATE TRIGGER fail_reward BEFORE INSERT ON dungeon_reward_operations BEGIN SELECT RAISE(ABORT, 'fail'); END")
        with self.assertRaises(db_backend.IntegrityError):
            self.service.award("rollback", self.rewards, 99)
        self.assertEqual(self.state(), ([("a", 1, 10), ("b", 2, 20)], None))
