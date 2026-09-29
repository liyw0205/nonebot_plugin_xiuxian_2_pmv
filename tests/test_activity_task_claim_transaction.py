import tempfile
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_activity.transaction_service import ActivityTaskClaimService
from nonebot_plugin_xiuxian_2.features.activity_reward.migrations import apply_activity_task_claim
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork
from nonebot_plugin_xiuxian_2.infrastructure.database import ReconcileService
from nonebot_plugin_xiuxian_2.features.activity_reward.task_claim_application import ActivityTaskClaimApplication
from nonebot_plugin_xiuxian_2.plugin import apply_platform_schema
from tests.test_db_backend import db_backend


class ActivityTaskClaimTransactionTests(unittest.TestCase):
    def test_activity_service_defers_task_claim_service_construction(self):
        from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_activity import service

        self.assertIsNone(service._activity_task_claim_application_instance)

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        root = Path(self.tmp.name)
        self.activity, self.game = root / "activity.db", root / "game.db"
        with db_backend.transaction(self.activity) as conn:
            conn.execute("CREATE TABLE activity_task_progress(activity_key TEXT,user_id TEXT,scope_type TEXT,scope_key TEXT,task_key TEXT,progress INTEGER,target INTEGER,claimed INTEGER,claim_time TEXT,update_time TEXT,PRIMARY KEY(activity_key,user_id,scope_type,scope_key,task_key))")
            conn.execute("CREATE TABLE activity_task_claim_log(id INTEGER PRIMARY KEY,activity_key TEXT,user_id TEXT,scope_type TEXT,scope_key TEXT,task_key TEXT,reward TEXT,create_time TEXT)")
            conn.execute("INSERT INTO activity_task_progress VALUES('a','u','daily','d','t',2,2,0,'','')")
        with db_backend.transaction(self.game) as conn:
            conn.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,stone INTEGER)")
            conn.execute("INSERT INTO user_xiuxian VALUES('u',10)")
            conn.execute("CREATE TABLE back(user_id TEXT,goods_id INTEGER,goods_name TEXT,goods_type TEXT,goods_num INTEGER,create_time TEXT,update_time TEXT,bind_num INTEGER,UNIQUE(user_id,goods_id))")
        with DatabaseUnitOfWork(self.game) as uow:
            apply_platform_schema(uow)
            apply_activity_task_claim(uow)
        self.service = ActivityTaskClaimService(self.activity, self.game)
        rewards = ({"type": "stone", "quantity": 50}, {"id": 101, "name": "任务令", "type": "道具", "quantity": 2})
        self.tasks = (("t", "daily", "d", 2, "灵石x50,任务令x2", rewards, "每日任务"),)

    def tearDown(self):
        self.tmp.cleanup()

    def test_atomic_claim_and_idempotency(self):
        result = self.service.claim("op", "u", "a", self.tasks, 100)
        self.assertEqual("applied", result.status)
        self.assertEqual("duplicate", self.service.claim("op", "u", "a", self.tasks, 100).status)
        self.assertEqual("duplicate", self.service.get_result("op", "u").status)
        self.assertEqual("operation_conflict", self.service.get_result("op", "other").status)
        with db_backend.connection(self.activity) as conn:
            self.assertEqual((1, 1), tuple(conn.execute("SELECT p.claimed,COUNT(l.id) FROM activity_task_progress p JOIN activity_task_claim_log l ON 1=1").fetchone()))
        with db_backend.connection(self.game) as conn:
            self.assertEqual(60, conn.execute("SELECT stone FROM user_xiuxian").fetchone()[0])
            self.assertEqual(2, conn.execute("SELECT goods_num FROM back").fetchone()[0])

    def test_separate_operation_cannot_grant_the_same_period_task_twice(self):
        self.assertEqual("applied", self.service.claim("first", "u", "a", self.tasks, 100).status)
        self.assertEqual("claim_in_progress", self.service.claim("second", "u", "a", self.tasks, 100).status)
        with db_backend.connection(self.game) as conn:
            self.assertEqual(60, conn.execute("SELECT stone FROM user_xiuxian").fetchone()[0])
            self.assertEqual(2, conn.execute("SELECT goods_num FROM back").fetchone()[0])

    def test_inventory_rejection_and_retry_after_legacy_projection_failure(self):
        self.assertEqual("inventory_full", self.service.claim("full", "u", "a", self.tasks, 1).status)
        with db_backend.transaction(self.activity) as conn:
            conn.execute("CREATE TRIGGER fail_task BEFORE UPDATE OF claimed ON activity_task_progress BEGIN SELECT RAISE(ABORT,'x'); END")
        with self.assertRaises(Exception):
            self.service.claim("rollback", "u", "a", self.tasks, 100)
        with db_backend.connection(self.activity) as conn:
            self.assertEqual(0, conn.execute("SELECT claimed FROM activity_task_progress").fetchone()[0])
        with db_backend.connection(self.game) as conn:
            self.assertEqual(60, conn.execute("SELECT stone FROM user_xiuxian").fetchone()[0])
            self.assertEqual(2, conn.execute("SELECT goods_num FROM back").fetchone()[0])
        with db_backend.transaction(self.activity) as conn:
            conn.execute("DROP TRIGGER fail_task")
        application = ActivityTaskClaimApplication(self.game, self.activity)
        with DatabaseUnitOfWork(self.game, immediate=True) as uow:
            report = ReconcileService().run(
                uow,
                operation_handlers={application.action: application.reconcile},
            )
        self.assertTrue(report.clean)
        self.assertEqual("duplicate", self.service.claim("rollback", "u", "a", self.tasks, 100).status)
        with db_backend.connection(self.game) as conn:
            self.assertEqual(60, conn.execute("SELECT stone FROM user_xiuxian").fetchone()[0])
            self.assertEqual(2, conn.execute("SELECT goods_num FROM back").fetchone()[0])

    def test_reconcile_after_legacy_projection_commit_does_not_repeat_assets_or_log(self):
        application = self.service._get_application()
        finalize = application.repository._finalize_legacy_state

        def finalize_then_fail(request):
            finalize(request)
            raise RuntimeError("lost acknowledgement")

        application.repository._finalize_legacy_state = finalize_then_fail
        with self.assertRaisesRegex(RuntimeError, "lost acknowledgement"):
            self.service.claim("after-projection", "u", "a", self.tasks, 100)
        with db_backend.connection(self.activity) as conn:
            self.assertEqual(1, conn.execute("SELECT claimed FROM activity_task_progress").fetchone()[0])
            self.assertEqual(1, conn.execute("SELECT COUNT(*) FROM activity_task_claim_log").fetchone()[0])
        application.repository._finalize_legacy_state = finalize
        with DatabaseUnitOfWork(self.game, immediate=True) as uow:
            report = ReconcileService().run(
                uow,
                operation_handlers={application.action: application.reconcile},
            )
        self.assertTrue(report.clean)
        with db_backend.connection(self.game) as conn:
            self.assertEqual(60, conn.execute("SELECT stone FROM user_xiuxian").fetchone()[0])
            self.assertEqual(2, conn.execute("SELECT goods_num FROM back").fetchone()[0])
            self.assertEqual(1, conn.execute(
                "SELECT COUNT(*) FROM activity_task_reward_claim_operations "
                "WHERE operation_id='after-projection' AND status='applied'"
            ).fetchone()[0])
        with db_backend.connection(self.activity) as conn:
            self.assertEqual(1, conn.execute("SELECT COUNT(*) FROM activity_task_claim_log").fetchone()[0])

    def test_started_ledger_and_feature_receipt_are_atomic_for_reconcile(self):
        application = self.service._get_application()
        claim = application.repository.claim

        class SimulatedCrash(BaseException):
            pass

        def crash_before_claim(*args, **kwargs):
            raise SimulatedCrash

        application.repository.claim = crash_before_claim
        try:
            with self.assertRaises(SimulatedCrash):
                application.claim("prepared", "u", "a", self.tasks, 100)
        finally:
            application.repository.claim = claim

        with db_backend.connection(self.game) as conn:
            self.assertEqual("started", conn.execute(
                "SELECT status FROM operation_ledger WHERE operation_id='prepared' "
                "AND action='activity_reward.tasks.claim'"
            ).fetchone()[0])
            self.assertEqual("started", conn.execute(
                "SELECT status FROM activity_task_reward_claim_operations WHERE operation_id='prepared'"
            ).fetchone()[0])

        with DatabaseUnitOfWork(self.game, immediate=True) as uow:
            report = ReconcileService().run(
                uow,
                operation_handlers={application.action: application.reconcile},
            )
        self.assertTrue(report.clean)
        with db_backend.connection(self.game) as conn:
            self.assertEqual("applied", conn.execute(
                "SELECT status FROM operation_ledger WHERE operation_id='prepared' "
                "AND action='activity_reward.tasks.claim'"
            ).fetchone()[0])
            self.assertEqual(60, conn.execute("SELECT stone FROM user_xiuxian").fetchone()[0])
            self.assertEqual(2, conn.execute("SELECT goods_num FROM back").fetchone()[0])


if __name__ == "__main__":
    unittest.main()
