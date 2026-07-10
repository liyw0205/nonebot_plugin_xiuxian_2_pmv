from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_compensation.reward_service import (
    RewardClaimService,
)
from tests.test_db_backend import db_backend


class RewardClaimServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.database = Path(self.temp_dir.name) / "reward.sqlite3"
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "CREATE TABLE user_xiuxian (user_id TEXT PRIMARY KEY, stone INTEGER NOT NULL)"
            )
            conn.execute(
                """
                CREATE TABLE back (
                    user_id TEXT NOT NULL,
                    goods_id INTEGER NOT NULL,
                    goods_name TEXT,
                    goods_type TEXT,
                    goods_num INTEGER,
                    create_time TEXT,
                    update_time TEXT,
                    bind_num INTEGER DEFAULT 0,
                    UNIQUE (user_id, goods_id)
                )
                """
            )
            conn.execute("INSERT INTO user_xiuxian VALUES (%s, %s)", ("u1", 100))
        self.service = RewardClaimService(self.database, max_goods_num=999)
        self.items = [
            {"type": "stone", "id": "stone", "name": "灵石", "quantity": 50},
            {"type": "功法", "id": 101, "name": "太玄经", "quantity": 2},
        ]

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def scalar(self, sql, params=()):
        with db_backend.connection(self.database) as conn:
            row = conn.execute(sql, params).fetchone()
            return row[0] if row else None

    def test_claim_grants_all_assets_and_records_claim_atomically(self) -> None:
        result = self.service.claim("补偿", "C1", "u1", self.items)

        self.assertEqual(result.status, "claimed")
        self.assertEqual(self.scalar("SELECT stone FROM user_xiuxian WHERE user_id=%s", ("u1",)), 150)
        self.assertEqual(self.scalar("SELECT goods_num FROM back WHERE user_id=%s AND goods_id=%s", ("u1", 101)), 2)
        self.assertEqual(self.scalar("SELECT goods_type FROM back WHERE user_id=%s AND goods_id=%s", ("u1", 101)), "技能")
        self.assertTrue(self.service.has_claimed("补偿", "C1", "u1"))

    def test_duplicate_claim_does_not_grant_twice(self) -> None:
        first = self.service.claim("礼包", "G1", "u1", self.items)
        second = self.service.claim("礼包", "G1", "u1", self.items)

        self.assertEqual(first.status, "claimed")
        self.assertEqual(second.status, "duplicate")
        self.assertEqual(self.scalar("SELECT stone FROM user_xiuxian WHERE user_id=%s", ("u1",)), 150)

    def test_missing_user_is_rejected(self) -> None:
        result = self.service.claim("补偿", "C2", "missing", self.items)
        self.assertEqual(result.status, "user_missing")

    def test_database_failure_rolls_back_every_asset_and_claim(self) -> None:
        with db_backend.transaction(self.database) as conn:
            self.service._ensure_claims(conn)
            conn.execute(
                """
                CREATE TRIGGER fail_claim BEFORE INSERT ON reward_claims
                BEGIN SELECT RAISE(ABORT, 'claim failed'); END
                """
            )

        with self.assertRaises(db_backend.IntegrityError):
            self.service.claim("补偿", "C3", "u1", self.items)

        self.assertEqual(self.scalar("SELECT stone FROM user_xiuxian WHERE user_id=%s", ("u1",)), 100)
        self.assertIsNone(self.scalar("SELECT goods_num FROM back WHERE user_id=%s", ("u1",)))
        self.assertFalse(self.service.has_claimed("补偿", "C3", "u1"))

    def test_delete_claims_supports_record_and_reward_type(self) -> None:
        self.service.claim("补偿", "C1", "u1", [])
        self.service.claim("礼包", "G1", "u1", [])
        self.service.delete_claims("补偿", "C1")
        self.assertFalse(self.service.has_claimed("补偿", "C1", "u1"))
        self.assertTrue(self.service.has_claimed("礼包", "G1", "u1"))
        self.service.delete_claims("礼包")
        self.assertFalse(self.service.has_claimed("礼包", "G1", "u1"))


if __name__ == "__main__":
    unittest.main()
