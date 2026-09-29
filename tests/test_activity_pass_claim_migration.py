import json
import tempfile
import unittest
from pathlib import Path

from nonebot_plugin_xiuxian_2.features.activity_reward.migrations import (
    apply_activity_pass_claim,
    apply_activity_pass_claim_legacy_receipts,
)
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork
from tests.test_db_backend import db_backend


class ActivityPassClaimMigrationTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.game = self.root / "game.db"
        legacy_dir = self.root / "activity"
        legacy_dir.mkdir()
        self.legacy = legacy_dir / "activity.db"
        payload = ["u", "festival", 2, [[1, "初入庆典", "灵石x50"]], 50, [], 100]
        with db_backend.transaction(self.legacy) as conn:
            conn.execute(
                "CREATE TABLE activity_pass_claim_operations(operation_id TEXT PRIMARY KEY,payload TEXT,"
                "result_json TEXT,created_at TEXT)"
            )
            conn.execute(
                "CREATE TABLE activity_pass_reward_claim(activity_key TEXT,user_id TEXT,level INTEGER,"
                "create_time TEXT,PRIMARY KEY(activity_key,user_id,level))"
            )
            conn.execute(
                "INSERT INTO activity_pass_claim_operations VALUES(?,?,?,?)",
                ("old-op", json.dumps(payload, separators=(",", ":")),
                 json.dumps(payload[3], separators=(",", ":")), "2026-01-02T03:04:05"),
            )
            conn.execute("INSERT INTO activity_pass_reward_claim VALUES('festival','u',1,'old')")
            conn.execute("INSERT INTO activity_pass_reward_claim VALUES('festival','u',2,'old')")

    def tearDown(self):
        self.tmp.cleanup()

    def test_imports_receipts_and_claimed_level_reservations_idempotently(self):
        with DatabaseUnitOfWork(self.game, immediate=True) as uow:
            apply_activity_pass_claim(uow)
            apply_activity_pass_claim_legacy_receipts(uow)
        with DatabaseUnitOfWork(self.game, read_only=True) as uow:
            operation = uow.query_one(
                "SELECT payload,result_json,result_status,status,created_at,request_json "
                "FROM activity_pass_reward_claim_operations WHERE operation_id='old-op'"
            )
            self.assertEqual("applied", operation["result_status"])
            self.assertEqual("applied", operation["status"])
            self.assertEqual("2026-01-02T03:04:05", operation["created_at"])
            request = json.loads(operation["request_json"])
            self.assertTrue(request["legacy_import"])
            self.assertEqual("old-op", uow.query_one(
                "SELECT operation_id FROM activity_pass_reward_claim_reservations "
                "WHERE activity_key='festival' AND user_id='u' AND level=1"
            )["operation_id"])
            self.assertEqual("legacy-claimed:", uow.query_one(
                "SELECT operation_id FROM activity_pass_reward_claim_reservations "
                "WHERE activity_key='festival' AND user_id='u' AND level=2"
            )["operation_id"][:15])
        with DatabaseUnitOfWork(self.game, immediate=True) as uow:
            apply_activity_pass_claim_legacy_receipts(uow)
        with DatabaseUnitOfWork(self.game, read_only=True) as uow:
            self.assertEqual(1, uow.query_one(
                "SELECT COUNT(*) AS count FROM activity_pass_reward_claim_operations"
            )["count"])
            self.assertEqual(2, uow.query_one(
                "SELECT COUNT(*) AS count FROM activity_pass_reward_claim_reservations"
            )["count"])

    def test_rejects_malformed_historical_receipt_without_importing(self):
        with db_backend.transaction(self.legacy) as conn:
            conn.execute("UPDATE activity_pass_claim_operations SET payload='{}' WHERE operation_id='old-op'")
        with DatabaseUnitOfWork(self.game, immediate=True) as uow:
            apply_activity_pass_claim(uow)
        with DatabaseUnitOfWork(self.game, immediate=True) as uow:
            with self.assertRaisesRegex(RuntimeError, "invalid legacy activity pass payload"):
                apply_activity_pass_claim_legacy_receipts(uow)
        with DatabaseUnitOfWork(self.game, read_only=True) as uow:
            self.assertEqual(0, uow.query_one(
                "SELECT COUNT(*) AS count FROM activity_pass_reward_claim_operations"
            )["count"])


if __name__ == "__main__":
    unittest.main()
