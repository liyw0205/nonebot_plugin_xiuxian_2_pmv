import tempfile
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_admin.stone_adjustment_service import (
    AdminStoneAdjustmentService,
)
from tests.test_db_backend import db_backend


class AdminStoneAdjustmentTransactionTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.database = Path(self.temp.name) / "game.db"
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,exp INTEGER,stone INTEGER)"
            )
            conn.execute("INSERT INTO user_xiuxian VALUES(%s,%s,%s)", ("u1", 100, 50))
        self.service = AdminStoneAdjustmentService(self.database)

    def tearDown(self):
        self.temp.cleanup()

    def adjust(self, operation_id="stone-op", expected=50, delta=25):
        return self.service.adjust(
            operation_id, "admin-1", "u1", expected, delta, target_name="测试道友"
        )

    def test_adjustment_and_economy_audit_are_atomic_and_replayable(self):
        result = self.adjust()
        self.assertEqual((result.status, result.final_stone, result.applied_delta), ("adjusted", 75, 25))
        duplicate = self.adjust()
        self.assertEqual((duplicate.status, duplicate.final_stone), ("duplicate", 75))
        replay_after_refresh = self.adjust(expected=75)
        self.assertEqual(
            (replay_after_refresh.status, replay_after_refresh.final_stone), ("duplicate", 75)
        )
        with db_backend.connection(self.database) as conn:
            self.assertEqual(conn.execute("SELECT stone FROM user_xiuxian").fetchone()[0], 75)
            audit = conn.execute(
                "SELECT source,action,stone_delta,trace_id FROM economy_log WHERE user_id=%s",
                ("u1",),
            ).fetchone()
            self.assertEqual(tuple(audit), ("admin", "admin_stone_add", 25, "stone-op"))
            self.assertEqual(
                conn.execute("SELECT COUNT(*) FROM admin_stone_adjustment_operations").fetchone()[0], 1
            )

    def test_snapshot_conflict_and_operation_conflict_do_not_mutate(self):
        self.assertEqual(self.adjust(expected=49).status, "state_changed")
        self.assertEqual(self.adjust().status, "adjusted")
        self.assertEqual(self.adjust(delta=30).status, "operation_conflict")
        with db_backend.connection(self.database) as conn:
            self.assertEqual(conn.execute("SELECT stone FROM user_xiuxian").fetchone()[0], 75)
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM economy_log").fetchone()[0], 1)

    def test_subtraction_is_clamped_at_zero_and_audits_actual_delta(self):
        result = self.adjust(delta=-80)
        self.assertEqual((result.final_stone, result.applied_delta), (0, -50))
        with db_backend.connection(self.database) as conn:
            self.assertEqual(conn.execute("SELECT stone FROM user_xiuxian").fetchone()[0], 0)
            audit = conn.execute("SELECT action,stone_delta FROM economy_log").fetchone()
            self.assertEqual(tuple(audit), ("admin_stone_cost", -50))

    def test_operation_insert_failure_rolls_back_stone_and_audit(self):
        with db_backend.transaction(self.database) as conn:
            self.service._ensure_schema(conn)
            conn.execute(
                "CREATE TRIGGER reject_admin_stone BEFORE INSERT ON admin_stone_adjustment_operations "
                "BEGIN SELECT RAISE(ABORT,'reject admin stone'); END"
            )
        with self.assertRaisesRegex(Exception, "reject admin stone"):
            self.adjust()
        with db_backend.connection(self.database) as conn:
            self.assertEqual(conn.execute("SELECT stone FROM user_xiuxian").fetchone()[0], 50)
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM economy_log").fetchone()[0], 0)

    def test_real_single_user_entry_uses_service_without_legacy_update(self):
        path = "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_admin/__init__.py"
        with open(path, encoding="utf-8") as source_file:
            text = source_file.read()
        handler = text[text.index("async def gm_command_"):text.index("# GM加思恋结晶")]
        single_user = handler[handler.index("else:  # 单人"):]
        self.assertIn("admin_stone_adjustment_service.adjust(", single_user)
        self.assertNotIn("sql_message.update_ls(", single_user)
        self.assertIn("sql_message.update_ls_all(amount)", handler)


if __name__ == "__main__":
    unittest.main()
