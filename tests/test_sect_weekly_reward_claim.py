import tempfile
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_sect.sect_weekly_reward_service import SectWeeklyRewardClaimService
from tests.test_db_backend import db_backend


class SectWeeklyRewardClaimTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        root = Path(self.tmp.name)
        self.game, self.player = root / "game.db", root / "player.db"
        with db_backend.transaction(self.game) as conn:
            conn.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,sect_id INTEGER,stone INTEGER,exp INTEGER,sect_contribution INTEGER)")
            conn.execute("INSERT INTO user_xiuxian VALUES('u',1,10,20,30)")
            conn.execute("CREATE TABLE sects(sect_id INTEGER PRIMARY KEY,sect_scale INTEGER,sect_materials INTEGER)")
            conn.execute("INSERT INTO sects VALUES(1,100,200)")
            conn.execute("CREATE TABLE back(user_id TEXT,goods_id INTEGER,goods_name TEXT,goods_type TEXT,goods_num INTEGER,create_time TEXT,update_time TEXT,bind_num INTEGER,UNIQUE(user_id,goods_id))")
            conn.execute("CREATE TABLE sect_weekly_goal(sect_id INTEGER,week_key TEXT,goal_key TEXT,progress INTEGER,target INTEGER,claimed_users TEXT,updated_at TEXT,PRIMARY KEY(sect_id,week_key,goal_key))")
            conn.executemany("INSERT INTO sect_weekly_goal VALUES(1,'2026-W29',%s,%s,%s,'[]','')", (("g1", 10, 10), ("g2", 20, 20), ("pending", 1, 5)))
        with db_backend.transaction(self.player) as conn:
            conn.execute("CREATE TABLE boss_limit(user_id TEXT PRIMARY KEY,integral INTEGER)")
            conn.execute("INSERT INTO boss_limit VALUES('u',7)")
        self.service = SectWeeklyRewardClaimService(self.game, self.player)
        self.goals = [
            {"key": "g1", "name": "目标一", "target": 10, "rewards": {"stone": 5, "exp": 6, "sect_contribution": 7, "sect_scale": 8, "sect_materials": 9, "boss_integral": 10, "items": [{"id": 101, "name": "周常令", "type": "道具", "amount": 2}]}},
            {"key": "g2", "name": "目标二", "target": 20, "rewards": {"stone": 11, "sect_contribution": 12, "sect_scale": 13, "sect_materials": 14, "boss_integral": 15, "items": [{"id": 101, "name": "周常令", "type": "道具", "amount": 3}]}},
        ]

    def tearDown(self):
        self.tmp.cleanup()

    def claim(self, operation_id="op", **changes):
        args = dict(operation_id=operation_id, user_id="u", sect_id=1, week_key="2026-W29", goals=self.goals, max_goods_num=100)
        args.update(changes)
        return self.service.claim(**args)

    def test_batch_claim_updates_all_assets_and_is_idempotent(self):
        self.assertEqual("applied", self.claim().status)
        self.assertEqual("duplicate", self.claim().status)
        with db_backend.connection(self.game) as conn:
            self.assertEqual((26, 26, 49), tuple(conn.execute("SELECT stone,exp,sect_contribution FROM user_xiuxian").fetchone()))
            self.assertEqual((121, 223), tuple(conn.execute("SELECT sect_scale,sect_materials FROM sects").fetchone()))
            self.assertEqual(5, conn.execute("SELECT goods_num FROM back").fetchone()[0])
            self.assertEqual(['["u"]', '["u"]'], [row[0] for row in conn.execute("SELECT claimed_users FROM sect_weekly_goal WHERE goal_key IN ('g1','g2') ORDER BY goal_key")])
        with db_backend.connection(self.player) as conn:
            self.assertEqual(32, conn.execute("SELECT integral FROM boss_limit").fetchone()[0])

    def test_single_goal_uses_same_transaction_path(self):
        result = self.claim(goals=self.goals[:1])
        self.assertEqual("applied", result.status)
        self.assertEqual(("目标一", "周常令x2、灵石5、修为6、宗门贡献7、宗门建设度8、宗门资材9、BOSS积分10"), result.rewards[0])
        with db_backend.connection(self.game) as conn:
            self.assertEqual('["u"]', conn.execute("SELECT claimed_users FROM sect_weekly_goal WHERE goal_key='g1'").fetchone()[0])
            self.assertEqual('[]', conn.execute("SELECT claimed_users FROM sect_weekly_goal WHERE goal_key='g2'").fetchone()[0])

    def test_rechecks_week_membership_completion_and_claim_state(self):
        pending = [{**self.goals[0], "key": "pending", "target": 5}]
        self.assertEqual("not_completed", self.claim(goals=pending).status)
        self.assertEqual("not_completed", self.claim(week_key="2026-W28").status)
        with db_backend.transaction(self.game) as conn:
            conn.execute("UPDATE user_xiuxian SET sect_id=2 WHERE user_id='u'")
        self.assertEqual("sect_changed", self.claim().status)

    def test_inventory_full_and_operation_conflict_do_not_mutate(self):
        self.assertEqual("inventory_full", self.claim(max_goods_num=4).status)
        self.assertEqual("applied", self.claim().status)
        self.assertEqual("operation_conflict", self.claim(goals=self.goals[:1]).status)

    def test_operation_failure_rolls_back_both_databases(self):
        with db_backend.transaction(self.game) as conn:
            conn.execute("CREATE TABLE sect_weekly_reward_operations(operation_id TEXT PRIMARY KEY,payload TEXT,result_json TEXT,created_at TEXT)")
            conn.execute("CREATE TRIGGER fail_weekly_operation BEFORE INSERT ON sect_weekly_reward_operations BEGIN SELECT RAISE(ABORT,'operation failed'); END")
        with self.assertRaises(Exception):
            self.claim()
        with db_backend.connection(self.game) as conn:
            self.assertEqual((10, 20, 30), tuple(conn.execute("SELECT stone,exp,sect_contribution FROM user_xiuxian").fetchone()))
            self.assertEqual(0, conn.execute("SELECT COUNT(*) FROM back").fetchone()[0])
            self.assertEqual(['[]', '[]'], [row[0] for row in conn.execute("SELECT claimed_users FROM sect_weekly_goal WHERE goal_key IN ('g1','g2') ORDER BY goal_key")])
        with db_backend.connection(self.player) as conn:
            self.assertEqual(7, conn.execute("SELECT integral FROM boss_limit").fetchone()[0])


if __name__ == "__main__":
    unittest.main()
