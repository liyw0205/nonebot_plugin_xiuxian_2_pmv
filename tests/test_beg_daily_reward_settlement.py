import tempfile
import unittest
from datetime import datetime, timedelta
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_beg.daily_reward_service import BegDailyRewardService
from tests.test_db_backend import db_backend


class BegDailyRewardServiceTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.database = Path(self.temp_dir.name) / "game.sqlite3"
        self.created_at = datetime(2026, 7, 10, 8, 30)
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "CREATE TABLE user_xiuxian (user_id TEXT PRIMARY KEY,stone INTEGER,"
                "create_time TEXT,is_beg INTEGER,sect_id INTEGER,root_type TEXT,level TEXT)"
            )
            conn.execute(
                "INSERT INTO user_xiuxian VALUES (%s,%s,%s,%s,%s,%s,%s)",
                ("u1", 100, self.created_at, 0, None, "天灵根", "练气境初期"),
            )
        self.service = BegDailyRewardService(self.database)

    def tearDown(self):
        self.temp_dir.cleanup()

    def settle(self, operation_id="beg-op", **overrides):
        arguments = {
            "operation_id": operation_id,
            "user_id": "u1",
            "expected_create_time": self.created_at,
            "expected_stone": 100,
            "expected_sect_id": None,
            "expected_root_type": "天灵根",
            "expected_level": "练气境初期",
            "settled_at": self.created_at + timedelta(days=1),
            "max_age_days": 7,
            "eligible_levels": ("练气境初期", "练气境中期"),
            "stone_reward": 456,
        }
        arguments.update(overrides)
        return self.service.settle(**arguments)

    def test_atomic_settlement_records_fixed_reward_and_operation(self):
        result = self.settle()
        self.assertEqual(("applied", 456, 556), (result.status, result.stone_reward, result.stone))
        with db_backend.connection(self.database) as conn:
            self.assertEqual((556, 1), tuple(conn.execute("SELECT stone,is_beg FROM user_xiuxian").fetchone()))
            self.assertEqual(
                (456, 556),
                tuple(conn.execute("SELECT stone_reward,stone FROM beg_daily_reward_operations").fetchone()),
            )

    def test_operation_is_idempotent_and_conflicting_reward_is_rejected(self):
        self.assertEqual("applied", self.settle().status)
        # mutable stone/reward snapshots must not break same-op replay
        duplicate = self.settle(expected_stone=999, stone_reward=1)
        self.assertEqual(("duplicate", 456, 556), (duplicate.status, duplicate.stone_reward, duplicate.stone))
        prior = self.service.get_result("beg-op")
        self.assertIsNotNone(prior)
        self.assertEqual(prior.stone_reward, 456)
        # identity is user_id only — different user on same op id conflicts
        self.assertEqual("operation_conflict", self.settle(user_id="other").status)
        with db_backend.connection(self.database) as conn:
            self.assertEqual(556, conn.execute("SELECT stone FROM user_xiuxian").fetchone()[0])
            self.assertEqual(1, conn.execute("SELECT COUNT(*) FROM beg_daily_reward_operations").fetchone()[0])
            payload = conn.execute(
                "SELECT payload FROM beg_daily_reward_operations WHERE operation_id=%s", ("beg-op",)
            ).fetchone()[0]
            self.assertEqual(payload, '["u1"]')

    def test_rechecks_daily_flag_eligibility_and_asset_snapshot(self):
        with db_backend.transaction(self.database) as conn:
            conn.execute("UPDATE user_xiuxian SET is_beg=1")
        self.assertEqual("already_claimed", self.settle().status)
        with db_backend.transaction(self.database) as conn:
            conn.execute("UPDATE user_xiuxian SET is_beg=0,stone=101")
        self.assertEqual("state_changed", self.settle("asset-changed").status)
        with db_backend.transaction(self.database) as conn:
            conn.execute("UPDATE user_xiuxian SET stone=100,sect_id=7,root_type='伪灵根'")
        self.assertEqual(
            "ineligible_sect",
            self.settle("sect-ineligible", expected_sect_id=7, expected_root_type="伪灵根").status,
        )
        self.assert_unchanged(100, 0)

    def test_expired_and_level_ineligible_claims_do_not_mutate(self):
        self.assertEqual("expired", self.settle(settled_at=self.created_at + timedelta(days=8)).status)
        with db_backend.transaction(self.database) as conn:
            conn.execute("UPDATE user_xiuxian SET level='结丹境圆满'")
        self.assertEqual(
            "ineligible_level",
            self.settle("level-ineligible", expected_level="结丹境圆满").status,
        )
        self.assert_unchanged(100, 0)

    def test_failure_after_asset_update_rolls_back_flag_stone_and_operation(self):
        def fail(checkpoint):
            if checkpoint == "after_user_update":
                raise RuntimeError("injected failure")

        self.service = BegDailyRewardService(self.database, failure_hook=fail)
        with self.assertRaisesRegex(RuntimeError, "injected failure"):
            self.settle("rollback")
        self.assert_unchanged(100, 0)

    def assert_unchanged(self, stone, is_beg):
        with db_backend.connection(self.database) as conn:
            actual = tuple(conn.execute("SELECT stone,is_beg FROM user_xiuxian").fetchone())
            count = (
                conn.execute("SELECT COUNT(*) FROM beg_daily_reward_operations").fetchone()[0]
                if conn.table_exists("beg_daily_reward_operations")
                else 0
            )
        self.assertEqual((stone, is_beg), actual)
        self.assertEqual(0, count)


if __name__ == "__main__":
    unittest.main()
