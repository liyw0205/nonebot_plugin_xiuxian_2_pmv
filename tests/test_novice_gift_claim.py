import tempfile
import unittest
from datetime import datetime, timedelta
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_beg.novice_gift_service import NoviceGiftClaimService
from tests.test_db_backend import db_backend


class NoviceGiftClaimServiceTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.database = Path(self.temp_dir.name) / "game.sqlite3"
        self.created_at = datetime(2026, 7, 10, 8, 30)
        with db_backend.transaction(self.database) as conn:
            conn.execute("CREATE TABLE user_xiuxian (user_id TEXT PRIMARY KEY,stone INTEGER,create_time TEXT,is_novice INTEGER)")
            conn.execute("INSERT INTO user_xiuxian VALUES (%s,%s,%s,%s)", ("u1", 10, self.created_at, 0))
            conn.execute("CREATE TABLE back (user_id TEXT,goods_id INTEGER,goods_name TEXT,goods_type TEXT,goods_num INTEGER,create_time TEXT,update_time TEXT,bind_num INTEGER,UNIQUE(user_id,goods_id))")
        self.service = NoviceGiftClaimService(self.database)
        self.rewards = [
            {"id": 101, "name": "青木剑", "type": "装备", "amount": 1},
            {"id": 102, "name": "引气诀", "type": "技能", "amount": 2},
        ]

    def tearDown(self):
        self.temp_dir.cleanup()

    def claim(self, operation_id="op-1", **overrides):
        arguments = {
            "operation_id": operation_id, "user_id": "u1",
            "expected_create_time": self.created_at,
            "claimed_at": self.created_at + timedelta(days=1),
            "max_age_days": 7, "stone": 500, "rewards": self.rewards,
            "max_goods_num": 1000,
        }
        arguments.update(overrides)
        return self.service.claim(**arguments)

    def test_success_commits_all_assets_flag_and_operation_once(self):
        self.assertEqual(self.claim().status, "applied")
        with db_backend.connection(self.database) as conn:
            user = conn.execute("SELECT stone,is_novice FROM user_xiuxian").fetchone()
            items = conn.execute("SELECT goods_id,goods_num,bind_num FROM back ORDER BY goods_id").fetchall()
            operations = conn.execute("SELECT COUNT(*) FROM novice_gift_claim_operations").fetchone()[0]
        self.assertEqual(tuple(user), (510, 1))
        self.assertEqual([tuple(row) for row in items], [(101, 1, 1), (102, 2, 2)])
        self.assertEqual(operations, 1)

    def test_same_operation_is_idempotent_and_new_operation_is_rejected(self):
        self.assertEqual(self.claim().status, "applied")
        self.assertEqual(self.claim().status, "duplicate")
        self.assertEqual(self.claim("op-2").status, "already_claimed")
        with db_backend.connection(self.database) as conn:
            self.assertEqual(tuple(conn.execute("SELECT stone,is_novice FROM user_xiuxian").fetchone()), (510, 1))
            self.assertEqual(conn.execute("SELECT SUM(goods_num) FROM back").fetchone()[0], 3)

    def test_expired_character_is_unchanged(self):
        result = self.claim(claimed_at=self.created_at + timedelta(days=7, seconds=1))
        self.assertEqual(result.status, "expired")
        self.assert_unchanged()

    def test_inventory_full_rolls_back_every_asset(self):
        with db_backend.transaction(self.database) as conn:
            conn.execute("INSERT INTO back VALUES (%s,%s,%s,%s,%s,%s,%s,%s)", ("u1", 102, "引气诀", "技能", 999, "", "", 999))
        self.assertEqual(self.claim().status, "inventory_full")
        with db_backend.connection(self.database) as conn:
            self.assertEqual(tuple(conn.execute("SELECT stone,is_novice FROM user_xiuxian").fetchone()), (10, 0))
            self.assertEqual(conn.execute("SELECT goods_num FROM back").fetchone()[0], 999)

    def test_create_time_change_rejects_stale_claim(self):
        changed_time = self.created_at + timedelta(hours=1)
        with db_backend.transaction(self.database) as conn:
            conn.execute("UPDATE user_xiuxian SET create_time=%s WHERE user_id=%s", (changed_time, "u1"))
        self.assertEqual(self.claim().status, "state_changed")
        self.assert_unchanged(expected_create_time=changed_time)

    def test_injected_failure_rolls_back_everything(self):
        def fail_after_rewards(checkpoint):
            if checkpoint == "after_rewards":
                raise RuntimeError("injected failure")
        self.service = NoviceGiftClaimService(self.database, failure_hook=fail_after_rewards)
        with self.assertRaisesRegex(RuntimeError, "injected failure"):
            self.claim()
        self.assert_unchanged()
        with db_backend.connection(self.database) as conn:
            self.assertFalse(conn.table_exists("novice_gift_claim_operations"))

    def assert_unchanged(self, expected_create_time=None):
        with db_backend.connection(self.database) as conn:
            user = conn.execute("SELECT stone,is_novice,create_time FROM user_xiuxian").fetchone()
            item_count = conn.execute("SELECT COUNT(*) FROM back").fetchone()[0]
        self.assertEqual(tuple(user[:2]), (10, 0))
        self.assertEqual(datetime.fromisoformat(str(user[2])), expected_create_time or self.created_at)
        self.assertEqual(item_count, 0)


if __name__ == "__main__":
    unittest.main()
