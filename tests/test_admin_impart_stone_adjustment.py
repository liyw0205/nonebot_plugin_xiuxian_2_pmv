from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_admin.impart_stone_adjustment_service import (
    AdminImpartStoneAdjustmentService,
)
from tests.test_db_backend import db_backend


class AdminImpartStoneAdjustmentTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        root = Path(self.temp.name)
        self.game_database = root / "game.db"
        self.impart_database = root / "impart.db"
        with db_backend.transaction(self.game_database) as conn:
            conn.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY)")
            conn.execute("INSERT INTO user_xiuxian VALUES('u')")
        with db_backend.transaction(self.impart_database) as conn:
            conn.execute("CREATE TABLE xiuxian_impart(user_id TEXT,stone_num INTEGER)")
            conn.execute("INSERT INTO xiuxian_impart VALUES('u',5)")
        self.service = AdminImpartStoneAdjustmentService(
            self.game_database, self.impart_database
        )

    def tearDown(self) -> None:
        self.temp.cleanup()

    def stone(self):
        with db_backend.connection(self.impart_database) as conn:
            row = conn.execute(
                "SELECT stone_num FROM xiuxian_impart WHERE user_id='u'"
            ).fetchone()
            return int(row[0]) if row else None

    def adjust(self, operation="adjust", **changes):
        values = dict(
            operation_id=operation,
            operator_id="admin",
            user_id="u",
            expected_stone=5,
            requested_delta=3,
            target_name="道友",
        )
        values.update(changes)
        return self.service.adjust(**values)

    def test_adjustment_is_atomic_idempotent_and_audited(self) -> None:
        first = self.adjust()
        duplicate = self.adjust(expected_stone=8, target_name="新道号")

        self.assertEqual((first.status, duplicate.status), ("adjusted", "duplicate"))
        self.assertEqual(
            (first.previous_stone, first.final_stone, first.applied_delta),
            (5, 8, 3),
        )
        self.assertEqual(self.stone(), 8)
        with db_backend.connection(self.game_database) as conn:
            log = conn.execute(
                "SELECT action,item_delta,trace_id FROM economy_log"
            ).fetchone()
            self.assertEqual(
                ("admin_impart_stone_add", "adjust"), (log[0], log[2])
            )
            self.assertEqual(3, json.loads(log[1])[0]["amount"])
            self.assertEqual(
                1,
                conn.execute(
                    "SELECT COUNT(*) FROM admin_impart_stone_operations"
                ).fetchone()[0],
            )

    def test_deduction_clamps_at_zero_and_records_actual_delta(self) -> None:
        result = self.adjust("deduct", requested_delta=-20)

        self.assertEqual(
            (result.status, result.final_stone, result.applied_delta),
            ("adjusted", 0, -5),
        )
        self.assertEqual(self.stone(), 0)
        with db_backend.connection(self.game_database) as conn:
            log = conn.execute(
                "SELECT action,item_delta FROM economy_log"
            ).fetchone()
            self.assertEqual("admin_impart_stone_cost", log[0])
            self.assertEqual(-5, json.loads(log[1])[0]["amount"])

    def test_missing_impart_row_is_created_for_existing_player(self) -> None:
        with db_backend.transaction(self.impart_database) as conn:
            conn.execute("DELETE FROM xiuxian_impart")

        result = self.adjust("create", expected_stone=None)

        self.assertEqual((result.previous_stone, result.final_stone), (0, 3))
        self.assertEqual(self.stone(), 3)

    def test_snapshot_payload_and_duplicate_rows_are_rechecked(self) -> None:
        self.assertEqual(
            "state_changed", self.adjust("stale", expected_stone=4).status
        )
        self.assertEqual("adjusted", self.adjust("conflict").status)
        self.assertEqual(
            "operation_conflict",
            self.adjust("conflict", requested_delta=1).status,
        )
        with db_backend.transaction(self.impart_database) as conn:
            conn.execute("UPDATE xiuxian_impart SET stone_num=5")
            conn.execute("INSERT INTO xiuxian_impart VALUES('u',5)")
        snapshot = self.service.snapshot("u")
        self.assertEqual(
            "invalid_state",
            self.adjust("duplicates", expected_stone=snapshot).status,
        )

    def test_operation_failure_rolls_back_impart_audit_and_operation(self) -> None:
        with db_backend.connection(self.game_database) as conn:
            conn.execute(
                "ATTACH DATABASE %s AS impart_data", (str(self.impart_database),)
            )
            self.service._ensure_schema(conn)
            conn.commit()
            conn.execute("DETACH DATABASE impart_data")
        with db_backend.transaction(self.game_database) as conn:
            conn.execute(
                "CREATE TRIGGER fail_admin_impart_operation BEFORE INSERT ON "
                "admin_impart_stone_operations BEGIN SELECT RAISE(ABORT,'failed'); END"
            )

        with self.assertRaises(db_backend.IntegrityError):
            self.adjust("failed")

        self.assertEqual(self.stone(), 5)
        with db_backend.connection(self.game_database) as conn:
            self.assertEqual(
                0, conn.execute("SELECT COUNT(*) FROM economy_log").fetchone()[0]
            )
            self.assertEqual(
                0,
                conn.execute(
                    "SELECT COUNT(*) FROM admin_impart_stone_operations"
                ).fetchone()[0],
            )

    def test_production_single_entry_uses_service(self) -> None:
        source = (
            Path(__file__).parents[1]
            / "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_admin/__init__.py"
        ).read_text(encoding="utf-8")
        start = source.index("async def ccll_command_")
        handler = source[start:source.index("@adjust_exp_command.handle", start)]
        self.assertIn("admin_impart_stone_adjustment_service.adjust(", handler)
        self.assertNotIn("xiuxian_impart.update_stone_num(", handler)
        self.assertIn("admin_impart_stone_batch_adjustment_service.adjust(", handler)


if __name__ == "__main__":
    unittest.main()
