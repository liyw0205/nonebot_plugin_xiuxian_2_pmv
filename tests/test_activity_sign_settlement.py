import tempfile
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_activity.transaction_service import (
    ActivitySignSettlementService,
)
from tests.test_db_backend import db_backend


class ActivitySignSettlementTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        root = Path(self.tmp.name)
        self.activity = root / "activity.db"
        self.game = root / "game.db"
        with db_backend.transaction(self.activity) as conn:
            conn.execute(
                "CREATE TABLE activity_user("
                "user_id TEXT PRIMARY KEY,sign_days INTEGER,last_sign_date TEXT,"
                "total_sign_days INTEGER,create_time TEXT,update_time TEXT)"
            )
            conn.execute(
                "CREATE TABLE activity_sign_log("
                "id INTEGER PRIMARY KEY AUTOINCREMENT,user_id TEXT,sign_date TEXT,"
                "day_index INTEGER,reward TEXT,milestone_reward TEXT,reward_status TEXT,"
                "reward_message TEXT,create_time TEXT,finish_time TEXT,"
                "UNIQUE(user_id,sign_date))"
            )
            conn.execute("INSERT INTO activity_user VALUES('u',2,'2026-07-13',5,'','')")
        with db_backend.transaction(self.game) as conn:
            conn.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,stone INTEGER)")
            conn.execute("INSERT INTO user_xiuxian VALUES('u',10)")
            conn.execute(
                "CREATE TABLE back("
                "user_id TEXT,goods_id INTEGER,goods_name TEXT,goods_type TEXT,"
                "goods_num INTEGER,create_time TEXT,update_time TEXT,bind_num INTEGER,"
                "UNIQUE(user_id,goods_id))"
            )
        self.service = ActivitySignSettlementService(self.activity, self.game)
        self.daily = (
            {"type": "stone", "quantity": 50},
            {"id": 101, "name": "签到令", "type": "道具", "quantity": 2},
        )
        self.milestone = (
            {"type": "stone", "quantity": 30},
            {"id": 102, "name": "里程碑丹", "type": "丹药", "quantity": 1},
        )

    def tearDown(self):
        self.tmp.cleanup()

    def settle(self, operation_id="op", **changes):
        args = {
            "operation_id": operation_id,
            "user_id": "u",
            "sign_date": "2026-07-14",
            "expected_sign_days": 2,
            "expected_total_sign_days": 5,
            "daily_rewards": self.daily,
            "milestone_rewards": self.milestone,
            "max_goods_num": 100,
            "daily_reward_text": "灵石x50,签到令x2",
            "milestone_reward_text": "灵石x30,里程碑丹x1",
        }
        args.update(changes)
        return self.service.settle(**args)

    def test_atomic_daily_and_milestone_settlement(self):
        result = self.settle()
        self.assertEqual(("applied", 3, 6), (result.status, result.sign_days, result.total_sign_days))
        with db_backend.connection(self.activity) as conn:
            self.assertEqual(
                (3, "2026-07-14", 6),
                tuple(
                    conn.execute(
                        "SELECT sign_days,last_sign_date,total_sign_days FROM activity_user"
                    ).fetchone()
                ),
            )
            self.assertEqual(
                (3, "灵石x50,签到令x2", "灵石x30,里程碑丹x1", "success"),
                tuple(
                    conn.execute(
                        "SELECT day_index,reward,milestone_reward,reward_status "
                        "FROM activity_sign_log"
                    ).fetchone()
                ),
            )
        with db_backend.connection(self.game) as conn:
            self.assertEqual(90, conn.execute("SELECT stone FROM user_xiuxian").fetchone()[0])
            self.assertEqual(
                [(101, 2), (102, 1)],
                [tuple(row) for row in conn.execute("SELECT goods_id,goods_num FROM back ORDER BY goods_id")],
            )

    def test_operation_idempotency_and_conflict(self):
        self.assertEqual("applied", self.settle().status)
        duplicate = self.settle()
        self.assertEqual(("duplicate", 3, 6), (duplicate.status, duplicate.sign_days, duplicate.total_sign_days))
        # request identity differs (sign_date); mutable counters/rewards are not the payload key
        self.assertEqual("operation_conflict", self.settle(sign_date="2026-07-15").status)
        with db_backend.connection(self.game) as conn:
            self.assertEqual(90, conn.execute("SELECT stone FROM user_xiuxian").fetchone()[0])

    def test_rejects_already_signed_and_changed_counters(self):
        with db_backend.transaction(self.activity) as conn:
            conn.execute("UPDATE activity_user SET last_sign_date='2026-07-14'")
        self.assertEqual("already_signed", self.settle().status)
        with db_backend.transaction(self.activity) as conn:
            conn.execute("UPDATE activity_user SET last_sign_date='',sign_days=4")
        self.assertEqual("state_changed", self.settle("changed").status)

    def test_inventory_limit_rolls_back_all_state(self):
        with db_backend.transaction(self.game) as conn:
            conn.execute("INSERT INTO back VALUES('u',101,'签到令','道具',99,'','',99)")
        self.assertEqual("inventory_full", self.settle(max_goods_num=100).status)
        with db_backend.connection(self.activity) as conn:
            self.assertEqual(0, conn.execute("SELECT COUNT(*) FROM activity_sign_log").fetchone()[0])
            self.assertEqual((2, 5), tuple(conn.execute("SELECT sign_days,total_sign_days FROM activity_user").fetchone()))
        with db_backend.connection(self.game) as conn:
            self.assertEqual(10, conn.execute("SELECT stone FROM user_xiuxian").fetchone()[0])
            self.assertEqual(99, conn.execute("SELECT goods_num FROM back").fetchone()[0])

    def test_operation_failure_rolls_back_activity_and_game_databases(self):
        with db_backend.transaction(self.activity) as conn:
            conn.execute(
                "CREATE TABLE activity_sign_settlement_operations("
                "operation_id TEXT PRIMARY KEY,payload TEXT,sign_days INTEGER,"
                "total_sign_days INTEGER,created_at TEXT)"
            )
            conn.execute(
                "CREATE TRIGGER fail_sign_operation BEFORE INSERT "
                "ON activity_sign_settlement_operations BEGIN SELECT RAISE(ABORT,'x'); END"
            )
        with self.assertRaises(Exception):
            self.settle("rollback")
        with db_backend.connection(self.activity) as conn:
            self.assertEqual(0, conn.execute("SELECT COUNT(*) FROM activity_sign_log").fetchone()[0])
            self.assertEqual((2, 5), tuple(conn.execute("SELECT sign_days,total_sign_days FROM activity_user").fetchone()))
        with db_backend.connection(self.game) as conn:
            self.assertEqual(10, conn.execute("SELECT stone FROM user_xiuxian").fetchone()[0])
            self.assertEqual(0, conn.execute("SELECT COUNT(*) FROM back").fetchone()[0])


if __name__ == "__main__":
    unittest.main()
