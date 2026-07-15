import tempfile
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_compensation.invitation_reward_service import (
    InvitationRewardClaimService,
)
from tests.test_db_backend import db_backend


class InvitationRewardClaimTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.database = Path(self.tmp.name) / "game.db"
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,stone INTEGER)"
            )
            conn.execute("INSERT INTO user_xiuxian VALUES (%s,%s)", ("u1", 10))
            conn.execute(
                "CREATE TABLE back(user_id TEXT,goods_id INTEGER,goods_name TEXT,"
                "goods_type TEXT,goods_num INTEGER,create_time TEXT,update_time TEXT,"
                "bind_num INTEGER,UNIQUE(user_id,goods_id))"
            )
        self.service = InvitationRewardClaimService(self.database)
        self.rewards = {
            "1": [{"type": "stone", "id": "stone", "name": "灵石", "quantity": 50}],
            "3": [{"type": "道具", "id": 101, "name": "邀请令", "quantity": 2}],
            "5": [{"type": "stone", "id": "stone", "name": "灵石", "quantity": 100}],
        }

    def tearDown(self):
        self.tmp.cleanup()

    def claim(self, operation_id="op-1", **changes):
        args = dict(
            operation_id=operation_id,
            user_id="u1",
            invited_user_ids=["a", "b", "c"],
            rewards_by_threshold=self.rewards,
            requested_thresholds=[1, 3, 5],
            legacy_claimed_thresholds=[],
            max_goods_num=1000,
        )
        args.update(changes)
        return self.service.claim(**args)

    def snapshot(self):
        with db_backend.connection(self.database) as conn:
            stone = conn.execute(
                "SELECT stone FROM user_xiuxian WHERE user_id='u1'"
            ).fetchone()[0]
            item = conn.execute(
                "SELECT goods_num FROM back WHERE user_id='u1' AND goods_id=101"
            ).fetchone()
            claims = conn.execute(
                "SELECT threshold,source FROM invitation_reward_claims "
                "WHERE user_id='u1' ORDER BY threshold"
            ).fetchall() if conn.table_exists("invitation_reward_claims") else []
            operations = conn.execute(
                "SELECT COUNT(*) FROM invitation_reward_operations"
            ).fetchone()[0] if conn.table_exists("invitation_reward_operations") else 0
        return int(stone), int(item[0]) if item else 0, [tuple(row) for row in claims], int(operations)

    def test_batch_claim_is_atomic_and_operation_is_idempotent(self):
        result = self.claim()
        self.assertEqual(("applied", (1, 3), 3), (result.status, result.thresholds, result.invitation_count))
        # mutable invite list / rewards must not break same-op replay
        self.assertEqual("duplicate", self.claim(invited_user_ids=["x"], rewards_by_threshold={"1": self.rewards["1"]}).status)
        prior = self.service.get_result("op-1")
        self.assertIsNotNone(prior)
        self.assertEqual(prior.thresholds, (1, 3))
        self.assertEqual((60, 2, [(1, "transaction"), (3, "transaction")], 1), self.snapshot())
        with db_backend.connection(self.database) as conn:
            payload = conn.execute(
                "SELECT payload FROM invitation_reward_operations WHERE operation_id=%s", ("op-1",)
            ).fetchone()[0]
        self.assertEqual(payload, '["u1",[1,3,5]]')

    def test_single_threshold_and_qualification_are_rechecked(self):
        result = self.claim(requested_thresholds=[3], invited_user_ids=["a", "b"])
        self.assertEqual("no_available", result.status)
        self.assertEqual((10, 0, [], 0), self.snapshot())
        result = self.claim("op-2", requested_thresholds=[1], invited_user_ids=["a", "b"])
        self.assertEqual(("applied", (1,)), (result.status, result.thresholds))

    def test_legacy_json_claims_are_migrated_and_not_paid_again(self):
        result = self.claim(legacy_claimed_thresholds=["1"])
        self.assertEqual(("applied", (3,)), (result.status, result.thresholds))
        self.assertEqual((10, 2, [(1, "legacy_json"), (3, "transaction")], 1), self.snapshot())

    def test_invitation_snapshot_is_migrated_without_double_counting(self):
        result = self.claim(requested_thresholds=[1], invited_user_ids=["a", "a", "u1"])
        self.assertEqual(("applied", 1), (result.status, result.invitation_count))
        with db_backend.connection(self.database) as conn:
            count = conn.execute(
                "SELECT COUNT(*) FROM invitation_reward_invites WHERE inviter_id='u1'"
            ).fetchone()[0]
        self.assertEqual(1, count)

    def test_inventory_limit_rolls_back_claim_and_all_assets(self):
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "INSERT INTO back VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",
                ("u1", 101, "邀请令", "道具", 999, "", "", 999),
            )
        result = self.claim(max_goods_num=1000)
        self.assertEqual("inventory_full", result.status)
        self.assertEqual((10, 999, [], 0), self.snapshot())

    def test_operation_write_failure_rolls_back_migration_rewards_and_claims(self):
        with db_backend.transaction(self.database) as conn:
            self.service._ensure_tables(conn)
            conn.execute(
                "CREATE TRIGGER fail_invitation_operation BEFORE INSERT ON "
                "invitation_reward_operations BEGIN SELECT RAISE(ABORT,'failed'); END"
            )
        with self.assertRaises(Exception):
            self.claim(legacy_claimed_thresholds=[5])
        self.assertEqual((10, 0, [], 0), self.snapshot())
        with db_backend.connection(self.database) as conn:
            invites = conn.execute(
                "SELECT COUNT(*) FROM invitation_reward_invites"
            ).fetchone()[0]
        self.assertEqual(0, invites)


if __name__ == "__main__":
    unittest.main()
