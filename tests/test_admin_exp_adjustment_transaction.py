import tempfile
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_admin.exp_adjustment_service import (
    AdminExpAdjustmentService,
)
from tests.test_db_backend import db_backend


class AdminExpAdjustmentTransactionTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.database = Path(self.temp.name) / "game.db"
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,exp INTEGER,stone INTEGER)"
            )
            conn.execute("INSERT INTO user_xiuxian VALUES(%s,%s,%s)", ("u1", 100, 50))
        self.service = AdminExpAdjustmentService(self.database)

    def tearDown(self):
        self.temp.cleanup()

    def adjust(self, operation_id="exp-op", expected=100, delta=25):
        return self.service.adjust(
            operation_id, "admin-1", "u1", expected, delta, target_name="测试道友"
        )

    def test_adjustment_and_audit_are_atomic_and_replayable(self):
        result = self.adjust()
        self.assertEqual((result.status, result.final_exp, result.applied_delta), ("adjusted", 125, 25))
        duplicate = self.adjust()
        self.assertEqual((duplicate.status, duplicate.final_exp), ("duplicate", 125))
        replay_after_refresh = self.adjust(expected=125)
        self.assertEqual((replay_after_refresh.status, replay_after_refresh.final_exp), ("duplicate", 125))
        with db_backend.connection(self.database) as conn:
            self.assertEqual(conn.execute("SELECT exp FROM user_xiuxian").fetchone()[0], 125)
            audit = conn.execute(
                "SELECT exp_delta,trace_id FROM economy_log WHERE user_id=%s", ("u1",)
            ).fetchone()
            self.assertEqual((audit[0], audit[1]), (25, "exp-op"))
            self.assertEqual(
                conn.execute("SELECT COUNT(*) FROM admin_exp_adjustment_operations").fetchone()[0], 1
            )

    def test_snapshot_conflict_and_operation_conflict_do_not_mutate(self):
        self.assertEqual(self.adjust(expected=99).status, "state_changed")
        self.assertEqual(self.adjust().status, "adjusted")
        self.assertEqual(self.adjust(delta=30).status, "operation_conflict")
        with db_backend.connection(self.database) as conn:
            self.assertEqual(conn.execute("SELECT exp FROM user_xiuxian").fetchone()[0], 125)
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM economy_log").fetchone()[0], 1)

    def test_subtraction_is_clamped_at_zero_and_records_actual_delta(self):
        result = self.adjust(delta=-150)
        self.assertEqual((result.final_exp, result.applied_delta), (0, -100))
        with db_backend.connection(self.database) as conn:
            self.assertEqual(conn.execute("SELECT exp FROM user_xiuxian").fetchone()[0], 0)
            self.assertEqual(conn.execute("SELECT exp_delta FROM economy_log").fetchone()[0], -100)

    def test_operation_insert_failure_rolls_back_exp_and_audit(self):
        with db_backend.transaction(self.database) as conn:
            self.service._ensure_schema(conn)
            conn.execute(
                "CREATE TRIGGER reject_admin_exp BEFORE INSERT ON admin_exp_adjustment_operations "
                "BEGIN SELECT RAISE(ABORT,'reject admin exp'); END"
            )
        with self.assertRaisesRegex(Exception, "reject admin exp"):
            self.adjust()
        with db_backend.connection(self.database) as conn:
            self.assertEqual(conn.execute("SELECT exp FROM user_xiuxian").fetchone()[0], 100)
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM economy_log").fetchone()[0], 0)

    def test_real_entry_uses_service_without_legacy_exp_updates(self):
        path = "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_admin/__init__.py"
        with open(path, encoding="utf-8") as source_file:
            text = source_file.read()
        handler = text[text.index("async def adjust_exp_command_"):text.index("@zaohua_xiuxian.handle")]
        self.assertIn("admin_exp_adjustment_service.adjust(", handler)
        self.assertNotIn("sql_message.update_exp(", handler)
        self.assertNotIn("sql_message.update_j_exp(", handler)


if __name__ == "__main__":
    unittest.main()
