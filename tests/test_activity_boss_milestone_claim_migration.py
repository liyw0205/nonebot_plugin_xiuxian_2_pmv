import json
import tempfile
import unittest
from pathlib import Path

from nonebot_plugin_xiuxian_2.features.activity_reward.migrations import (
    apply_activity_boss_milestone_claim,
    apply_activity_boss_milestone_legacy_receipts,
)
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork
from tests.test_db_backend import db_backend


class ActivityBossMilestoneClaimMigrationTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.game = self.root / "game.db"
        legacy_dir = self.root / "activity"
        legacy_dir.mkdir()
        self.legacy = legacy_dir / "activity.db"
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
                "INSERT INTO activity_boss_reward_claim_operations VALUES(?,?,?,?)",
                (
                    "old-milestone",
                    json.dumps(["u", "a", [["m1", "里程碑一", "灵石x50"]]], ensure_ascii=True,
                               sort_keys=True, separators=(",", ":")),
                    json.dumps({"names": ["里程碑一"], "rank": 0}, ensure_ascii=True,
                               sort_keys=True, separators=(",", ":")),
                    "2026-02-03T04:05:06",
                ),
            )
            conn.execute(
                "INSERT INTO activity_boss_reward_claim_operations VALUES(?,?,?,?)",
                (
                    "old-rank",
                    json.dumps(["u", "a", 1, "1-1", "灵石x80"], ensure_ascii=True,
                               sort_keys=True, separators=(",", ":")),
                    json.dumps({"names": ["排行奖励"], "rank": 1}, ensure_ascii=True,
                               sort_keys=True, separators=(",", ":")),
                    "2026-02-03T04:05:07",
                ),
            )
            conn.execute("INSERT INTO activity_boss_milestone_claim VALUES('a','u','m1','old')")
            conn.execute("INSERT INTO activity_boss_milestone_claim VALUES('a','u','m2','old')")

    def tearDown(self):
        self.tmp.cleanup()

    def test_imports_only_milestone_receipts_and_claimed_markers_idempotently(self):
        with DatabaseUnitOfWork(self.game, immediate=True) as uow:
            apply_activity_boss_milestone_claim(uow)
            apply_activity_boss_milestone_legacy_receipts(uow)
        with DatabaseUnitOfWork(self.game, read_only=True) as uow:
            self.assertEqual(1, uow.query_one(
                "SELECT COUNT(*) AS count FROM activity_boss_milestone_claim_operations"
            )["count"])
            imported = uow.query_one(
                "SELECT status,result_status,created_at,request_json "
                "FROM activity_boss_milestone_claim_operations WHERE operation_id='old-milestone'"
            )
            self.assertEqual(("applied", "applied", "2026-02-03T04:05:06"),
                             (imported["status"], imported["result_status"], imported["created_at"]))
            self.assertTrue(json.loads(imported["request_json"])["legacy_import"])
            self.assertEqual("old-milestone", uow.query_one(
                "SELECT operation_id FROM activity_boss_milestone_claim_reservations "
                "WHERE activity_key='a' AND user_id='u' AND milestone_key='m1'"
            )["operation_id"])
            self.assertTrue(uow.query_one(
                "SELECT operation_id FROM activity_boss_milestone_claim_reservations "
                "WHERE activity_key='a' AND user_id='u' AND milestone_key='m2'"
            )["operation_id"].startswith("legacy-claimed:"))
        with DatabaseUnitOfWork(self.game, immediate=True) as uow:
            apply_activity_boss_milestone_legacy_receipts(uow)
        with DatabaseUnitOfWork(self.game, read_only=True) as uow:
            self.assertEqual(1, uow.query_one(
                "SELECT COUNT(*) AS count FROM activity_boss_milestone_claim_operations"
            )["count"])
            self.assertEqual(2, uow.query_one(
                "SELECT COUNT(*) AS count FROM activity_boss_milestone_claim_reservations"
            )["count"])

    def test_rejects_malformed_milestone_receipt(self):
        with db_backend.transaction(self.legacy) as conn:
            conn.execute(
                "UPDATE activity_boss_reward_claim_operations SET result_json='{}' "
                "WHERE operation_id='old-milestone'"
            )
        with DatabaseUnitOfWork(self.game, immediate=True) as uow:
            apply_activity_boss_milestone_claim(uow)
        with self.assertRaisesRegex(RuntimeError, "milestone result mismatch"):
            with DatabaseUnitOfWork(self.game, immediate=True) as uow:
                apply_activity_boss_milestone_legacy_receipts(uow)
        with DatabaseUnitOfWork(self.game, read_only=True) as uow:
            self.assertEqual(0, uow.query_one(
                "SELECT COUNT(*) AS count FROM activity_boss_milestone_claim_operations"
            )["count"])

    def test_milestone_backfill_does_not_take_ownership_of_pass_markers(self):
        with db_backend.transaction(self.legacy) as conn:
            conn.execute("CREATE TABLE activity_pass_reward_claim(activity_key TEXT,user_id TEXT,level INTEGER)")
            conn.execute("INSERT INTO activity_pass_reward_claim VALUES('a','u',1)")
        with DatabaseUnitOfWork(self.game, immediate=True) as uow:
            apply_activity_boss_milestone_claim(uow)
            apply_activity_boss_milestone_legacy_receipts(uow)
            self.assertEqual(1, uow.query_one(
                "SELECT COUNT(*) AS count FROM activity_boss_milestone_claim_operations"
            )["count"])

    def test_conflicting_reservation_rolls_back_imported_receipt(self):
        with DatabaseUnitOfWork(self.game, immediate=True) as uow:
            apply_activity_boss_milestone_claim(uow)
            uow.execute("INSERT INTO activity_boss_milestone_claim_reservations "
                        "(activity_key,user_id,milestone_key,operation_id) VALUES('a','u','m1','other')")
        with self.assertRaisesRegex(RuntimeError, "reservation conflict"):
            with DatabaseUnitOfWork(self.game, immediate=True) as uow:
                apply_activity_boss_milestone_legacy_receipts(uow)
        with DatabaseUnitOfWork(self.game, read_only=True) as uow:
            self.assertEqual(0, uow.query_one(
                "SELECT COUNT(*) AS count FROM activity_boss_milestone_claim_operations"
            )["count"])
            self.assertEqual("other", uow.query_one(
                "SELECT operation_id FROM activity_boss_milestone_claim_reservations"
            )["operation_id"])

    def test_null_names_fail_closed(self):
        with db_backend.transaction(self.legacy) as conn:
            conn.execute("UPDATE activity_boss_reward_claim_operations SET result_json=? WHERE operation_id='old-milestone'",
                         (json.dumps({"names": None, "rank": 0}),))
        with DatabaseUnitOfWork(self.game, immediate=True) as uow:
            apply_activity_boss_milestone_claim(uow)
        with self.assertRaisesRegex(RuntimeError, "milestone result mismatch"):
            with DatabaseUnitOfWork(self.game, immediate=True) as uow:
                apply_activity_boss_milestone_legacy_receipts(uow)


if __name__ == "__main__":
    unittest.main()
