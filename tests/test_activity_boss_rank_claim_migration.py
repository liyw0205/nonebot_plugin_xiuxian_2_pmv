import json
import tempfile
import unittest
from pathlib import Path

from nonebot_plugin_xiuxian_2.features.activity_reward.migrations import (
    apply_activity_boss_rank_claim,
    apply_activity_boss_rank_legacy_receipts,
)
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork
from tests.test_db_backend import db_backend


class ActivityBossRankClaimMigrationTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.game = self.root / "game.db"
        activity_dir = self.root / "activity"
        activity_dir.mkdir()
        self.legacy = activity_dir / "activity.db"
        with db_backend.transaction(self.legacy) as conn:
            conn.execute(
                "CREATE TABLE activity_boss_reward_claim_operations(operation_id TEXT PRIMARY KEY,"
                "payload TEXT,result_json TEXT,created_at TEXT)"
            )
            conn.execute(
                "CREATE TABLE activity_boss_milestone_claim(activity_key TEXT,user_id TEXT,"
                "milestone_key TEXT,create_time TEXT,PRIMARY KEY(activity_key,user_id,milestone_key))"
            )
            conn.execute(
                "CREATE TABLE activity_boss_rank_claim(activity_key TEXT,user_id TEXT,"
                "tier_key TEXT,create_time TEXT,PRIMARY KEY(activity_key,user_id,tier_key))"
            )
            conn.execute("INSERT INTO activity_boss_reward_claim_operations VALUES(?,?,?,?)", (
                "old-milestone",
                json.dumps(["u", "a", [["m1", "进度奖", "stone"]]], separators=(",", ":")),
                json.dumps({"names": ["进度奖"], "rank": 0}, separators=(",", ":")),
                "2026-02-03T04:05:06",
            ))
            conn.execute("INSERT INTO activity_boss_reward_claim_operations VALUES(?,?,?,?)", (
                "old-rank",
                json.dumps(["u", "a", 1, "1-1", "灵石x80"], separators=(",", ":")),
                json.dumps({"names": ["第一名"], "rank": 1}, separators=(",", ":")),
                "2026-02-03T04:05:07",
            ))
            conn.execute("INSERT INTO activity_boss_rank_claim VALUES('a','u','1-1','old')")
            conn.execute("INSERT INTO activity_boss_rank_claim VALUES('a','u','2-5','old')")

    def tearDown(self):
        self.tmp.cleanup()

    def test_imports_rank_receipts_and_tier_markers_idempotently(self):
        with DatabaseUnitOfWork(self.game, immediate=True) as uow:
            apply_activity_boss_rank_claim(uow)
            apply_activity_boss_rank_legacy_receipts(uow)
        with DatabaseUnitOfWork(self.game, read_only=True) as uow:
            self.assertEqual(1, uow.query_one(
                "SELECT COUNT(*) AS count FROM activity_boss_rank_claim_operations"
            )["count"])
            receipt = uow.query_one(
                "SELECT status,result_status,created_at,request_json FROM activity_boss_rank_claim_operations "
                "WHERE operation_id='old-rank'"
            )
            self.assertEqual(("applied", "applied", "2026-02-03T04:05:07"),
                             (receipt["status"], receipt["result_status"], receipt["created_at"]))
            self.assertTrue(json.loads(receipt["request_json"])["legacy_import"])
            self.assertEqual("old-rank", uow.query_one(
                "SELECT operation_id FROM activity_boss_rank_claim_reservations "
                "WHERE activity_key='a' AND user_id='u' AND tier_key='1-1'"
            )["operation_id"])
            self.assertTrue(uow.query_one(
                "SELECT operation_id FROM activity_boss_rank_claim_reservations "
                "WHERE activity_key='a' AND user_id='u' AND tier_key='2-5'"
            )["operation_id"].startswith("legacy-claimed:"))
        with DatabaseUnitOfWork(self.game, immediate=True) as uow:
            apply_activity_boss_rank_legacy_receipts(uow)
        with DatabaseUnitOfWork(self.game, read_only=True) as uow:
            self.assertEqual(1, uow.query_one(
                "SELECT COUNT(*) AS count FROM activity_boss_rank_claim_operations"
            )["count"])
            self.assertEqual(2, uow.query_one(
                "SELECT COUNT(*) AS count FROM activity_boss_rank_claim_reservations"
            )["count"])

    def test_rank_backfill_skips_milestone_rows_and_never_imports_milestone_claims(self):
        with db_backend.transaction(self.legacy) as conn:
            conn.execute("INSERT INTO activity_boss_milestone_claim VALUES('a','u','m1','old')")
        with DatabaseUnitOfWork(self.game, immediate=True) as uow:
            apply_activity_boss_rank_claim(uow)
            apply_activity_boss_rank_legacy_receipts(uow)
        with DatabaseUnitOfWork(self.game, read_only=True) as uow:
            self.assertEqual(1, uow.query_one(
                "SELECT COUNT(*) AS count FROM activity_boss_rank_claim_operations"
            )["count"])
            self.assertEqual(2, uow.query_one(
                "SELECT COUNT(*) AS count FROM activity_boss_rank_claim_reservations"
            )["count"])

    def test_rejects_malformed_rank_receipt_without_importing(self):
        with db_backend.transaction(self.legacy) as conn:
            conn.execute(
                "UPDATE activity_boss_reward_claim_operations SET result_json='{}' WHERE operation_id='old-rank'"
            )
        with DatabaseUnitOfWork(self.game, immediate=True) as uow:
            apply_activity_boss_rank_claim(uow)
        with self.assertRaisesRegex(RuntimeError, "rank result mismatch"):
            with DatabaseUnitOfWork(self.game, immediate=True) as uow:
                apply_activity_boss_rank_legacy_receipts(uow)
        with DatabaseUnitOfWork(self.game, read_only=True) as uow:
            self.assertEqual(0, uow.query_one(
                "SELECT COUNT(*) AS count FROM activity_boss_rank_claim_operations"
            )["count"])

    def test_conflicting_reservation_rolls_back_receipt_import(self):
        with DatabaseUnitOfWork(self.game, immediate=True) as uow:
            apply_activity_boss_rank_claim(uow)
            uow.execute(
                "INSERT INTO activity_boss_rank_claim_reservations "
                "(activity_key,user_id,tier_key,operation_id) VALUES('a','u','1-1','other')"
            )
        with self.assertRaisesRegex(RuntimeError, "rank reservation conflict"):
            with DatabaseUnitOfWork(self.game, immediate=True) as uow:
                apply_activity_boss_rank_legacy_receipts(uow)
        with DatabaseUnitOfWork(self.game, read_only=True) as uow:
            self.assertEqual(0, uow.query_one(
                "SELECT COUNT(*) AS count FROM activity_boss_rank_claim_operations"
            )["count"])
            self.assertEqual("other", uow.query_one(
                "SELECT operation_id FROM activity_boss_rank_claim_reservations"
            )["operation_id"])


if __name__ == "__main__":
    unittest.main()
