import tempfile
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_activity.transaction_service import BossRewardClaimService
from tests.test_db_backend import db_backend


class ActivityBossRewardClaimTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(); root = Path(self.tmp.name)
        self.activity, self.game = root / "activity.db", root / "game.db"
        with db_backend.transaction(self.activity) as conn:
            conn.execute("CREATE TABLE activity_boss_milestone(activity_key TEXT,milestone_key TEXT,unlocked_time TEXT,PRIMARY KEY(activity_key,milestone_key))")
            conn.execute("CREATE TABLE activity_boss_milestone_claim(activity_key TEXT,user_id TEXT,milestone_key TEXT,create_time TEXT,PRIMARY KEY(activity_key,user_id,milestone_key))")
            conn.execute("CREATE TABLE activity_boss_damage(activity_key TEXT,user_id TEXT,total_damage INTEGER,update_time TEXT,PRIMARY KEY(activity_key,user_id))")
            conn.execute("CREATE TABLE activity_boss_rank_claim(activity_key TEXT,user_id TEXT,tier_key TEXT,create_time TEXT,PRIMARY KEY(activity_key,user_id,tier_key))")
            conn.execute("INSERT INTO activity_boss_milestone VALUES('a','m1','')")
            conn.execute("INSERT INTO activity_boss_damage VALUES('a','u',100,'')")
        with db_backend.transaction(self.game) as conn:
            conn.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,stone INTEGER)")
            conn.execute("INSERT INTO user_xiuxian VALUES('u',0)")
            conn.execute("CREATE TABLE back(user_id TEXT,goods_id INTEGER,goods_name TEXT,goods_type TEXT,goods_num INTEGER,create_time TEXT,update_time TEXT,bind_num INTEGER,UNIQUE(user_id,goods_id))")
        self.service = BossRewardClaimService(self.activity, self.game, max_goods_num=100)

    def tearDown(self): self.tmp.cleanup()

    def test_milestone_assets_and_marker_are_atomic(self):
        rows = [{"key": "m1", "name": "进度奖", "reward": "灵石x50"}]
        self.assertEqual("applied", self.service.claim_milestones("u", "a", rows, "milestone-op").status)
        self.assertEqual("duplicate", self.service.claim_milestones("u", "a", rows, "milestone-op").status)
        self.assertEqual("duplicate", self.service.get_result("milestone-op", "u").status)
        self.assertEqual("operation_conflict", self.service.get_result("milestone-op", "other").status)
        with db_backend.connection(self.game) as conn: self.assertEqual(50, conn.execute("SELECT stone FROM user_xiuxian").fetchone()[0])

    def test_rank_claim_and_failure_rollback(self):
        tiers = [{"rank_min": 1, "rank_max": 1, "name": "第一名", "reward": "灵石x80"}]
        self.assertEqual("applied", self.service.claim_rank("u", "a", tiers).status)
        with db_backend.transaction(self.activity) as conn:
            conn.execute("DELETE FROM activity_boss_rank_claim"); conn.execute("DELETE FROM activity_boss_reward_claim_operations")
            conn.execute("CREATE TRIGGER fail_rank BEFORE INSERT ON activity_boss_rank_claim BEGIN SELECT RAISE(ABORT,'x'); END")
        with db_backend.transaction(self.game) as conn: conn.execute("UPDATE user_xiuxian SET stone=0")
        with self.assertRaises(Exception): self.service.claim_rank("u", "a", tiers)
        with db_backend.connection(self.game) as conn: self.assertEqual(0, conn.execute("SELECT stone FROM user_xiuxian").fetchone()[0])


if __name__ == "__main__": unittest.main()
