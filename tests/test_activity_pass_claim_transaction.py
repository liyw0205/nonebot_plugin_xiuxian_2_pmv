import tempfile
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_activity.pass_claim_service import (
    ActivityPassClaimService,
)
from tests.test_db_backend import db_backend


class ActivityPassClaimTransactionTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        root = Path(self.tmp.name)
        self.activity = root / "activity.db"
        self.game = root / "game.db"
        with db_backend.transaction(self.activity) as conn:
            conn.execute(
                "CREATE TABLE activity_pass_balance(activity_key TEXT,user_id TEXT,exp INTEGER,"
                "total_exp INTEGER,level INTEGER,update_time TEXT,PRIMARY KEY(activity_key,user_id))"
            )
            conn.execute(
                "CREATE TABLE activity_pass_reward_claim(activity_key TEXT,user_id TEXT,level INTEGER,"
                "create_time TEXT,PRIMARY KEY(activity_key,user_id,level))"
            )
            conn.execute("INSERT INTO activity_pass_balance VALUES('festival','u',20,220,2,'')")
        with db_backend.transaction(self.game) as conn:
            conn.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,stone INTEGER)")
            conn.execute("INSERT INTO user_xiuxian VALUES('u',10)")
            conn.execute(
                "CREATE TABLE back(user_id TEXT,goods_id INTEGER,goods_name TEXT,goods_type TEXT,"
                "goods_num INTEGER,create_time TEXT,update_time TEXT,bind_num INTEGER,"
                "UNIQUE(user_id,goods_id))"
            )
        self.service = ActivityPassClaimService(self.activity, self.game)
        self.rewards = (
            {
                "level": 1,
                "name": "初入庆典",
                "reward": "灵石x50",
                "reward_items": ({"type": "stone", "quantity": 50},),
            },
            {
                "level": 2,
                "name": "勤修补给",
                "reward": "战令牌x2",
                "reward_items": (
                    {"id": 101, "name": "战令牌", "type": "道具", "quantity": 2},
                ),
            },
        )

    def tearDown(self):
        self.tmp.cleanup()

    def test_claims_all_levels_and_replays_idempotently(self):
        result = self.service.claim("op", "u", "festival", 2, self.rewards, 100)
        self.assertEqual("applied", result.status)
        replay = self.service.claim("op", "u", "festival", 2, self.rewards, 100)
        self.assertEqual("duplicate", replay.status)
        self.assertEqual(result.rewards, replay.rewards)
        self.assertEqual(result.rewards, self.service.get_result("op", "u").rewards)
        self.assertEqual("operation_conflict", self.service.get_result("op", "other").status)
        with db_backend.connection(self.activity) as conn:
            self.assertEqual(
                [1, 2],
                [row[0] for row in conn.execute("SELECT level FROM activity_pass_reward_claim ORDER BY level")],
            )
            self.assertEqual(1, conn.execute("SELECT COUNT(*) FROM activity_pass_claim_operations").fetchone()[0])
        with db_backend.connection(self.game) as conn:
            self.assertEqual(60, conn.execute("SELECT stone FROM user_xiuxian WHERE user_id='u'").fetchone()[0])
            self.assertEqual(2, conn.execute("SELECT goods_num FROM back WHERE goods_id=101").fetchone()[0])

    def test_operation_conflict_does_not_mutate_state(self):
        self.service.claim("op", "u", "festival", 2, self.rewards, 100)
        changed = (dict(self.rewards[0]),)
        changed[0]["name"] = "不同奖励"
        self.assertEqual(
            "operation_conflict",
            self.service.claim("op", "u", "festival", 2, changed, 100).status,
        )
        with db_backend.connection(self.game) as conn:
            self.assertEqual(60, conn.execute("SELECT stone FROM user_xiuxian").fetchone()[0])

    def test_rechecks_level_and_claimed_state(self):
        self.assertEqual(
            "state_changed",
            self.service.claim("stale-level", "u", "festival", 1, self.rewards[:1], 100).status,
        )
        with db_backend.transaction(self.activity) as conn:
            conn.execute("INSERT INTO activity_pass_reward_claim VALUES('festival','u',1,'')")
        self.assertEqual(
            "state_changed",
            self.service.claim("claimed", "u", "festival", 2, self.rewards, 100).status,
        )

    def test_inventory_limit_rolls_back_all_rewards(self):
        self.assertEqual(
            "inventory_full",
            self.service.claim("full", "u", "festival", 2, self.rewards, 1).status,
        )
        with db_backend.connection(self.activity) as conn:
            self.assertEqual(0, conn.execute("SELECT COUNT(*) FROM activity_pass_reward_claim").fetchone()[0])
        with db_backend.connection(self.game) as conn:
            self.assertEqual(10, conn.execute("SELECT stone FROM user_xiuxian").fetchone()[0])
            self.assertEqual(0, conn.execute("SELECT COUNT(*) FROM back").fetchone()[0])

    def test_operation_failure_rolls_back_activity_and_game_databases(self):
        with db_backend.transaction(self.activity) as conn:
            conn.execute(
                "CREATE TABLE activity_pass_claim_operations(operation_id TEXT PRIMARY KEY,payload TEXT,"
                "result_json TEXT,created_at TEXT)"
            )
            conn.execute(
                "CREATE TRIGGER fail_pass_operation BEFORE INSERT ON activity_pass_claim_operations "
                "BEGIN SELECT RAISE(ABORT,'operation failure'); END"
            )
        with self.assertRaises(Exception):
            self.service.claim("rollback", "u", "festival", 2, self.rewards, 100)
        with db_backend.connection(self.activity) as conn:
            self.assertEqual(0, conn.execute("SELECT COUNT(*) FROM activity_pass_reward_claim").fetchone()[0])
        with db_backend.connection(self.game) as conn:
            self.assertEqual(10, conn.execute("SELECT stone FROM user_xiuxian").fetchone()[0])
            self.assertEqual(0, conn.execute("SELECT COUNT(*) FROM back").fetchone()[0])


if __name__ == "__main__":
    unittest.main()
