from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_admin.transaction_service import (
    AdminPlayerStatusResetService,
)
from tests.test_db_backend import db_backend


class AdminPlayerStatusResetTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.database = Path(self.temp.name) / "game.db"
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "CREATE TABLE user_xiuxian("
                "user_id TEXT PRIMARY KEY,exp INTEGER,hp INTEGER,mp INTEGER,"
                "atk INTEGER,user_stamina INTEGER)"
            )
            conn.execute("INSERT INTO user_xiuxian VALUES('u',101,1,2,3,4)")
        self.service = AdminPlayerStatusResetService(self.database)

    def tearDown(self) -> None:
        self.temp.cleanup()

    def state(self):
        with db_backend.connection(self.database) as conn:
            return tuple(
                conn.execute(
                    "SELECT exp,hp,mp,atk,user_stamina FROM user_xiuxian "
                    "WHERE user_id='u'"
                ).fetchone()
            )

    def reset(self, operation="reset", **changes):
        values = dict(
            operation_id=operation,
            operator_id="admin",
            user_id="u",
            expected_state=(101, 1, 2, 3, 4),
            max_stamina=20,
            target_name="道友",
        )
        values.update(changes)
        return self.service.reset(**values)

    def test_reset_is_atomic_idempotent_and_recomputes_all_fields(self) -> None:
        first = self.reset()
        duplicate = self.reset(
            expected_state=(101, 50, 101, 10, 20), target_name="新道号"
        )

        self.assertEqual((first.status, duplicate.status), ("reset", "duplicate"))
        self.assertEqual(first.previous_state, (101, 1, 2, 3, 4))
        self.assertEqual(first.final_state, (101, 50, 101, 10, 20))
        self.assertEqual(self.state(), (101, 50, 101, 10, 20))
        with db_backend.connection(self.database) as conn:
            self.assertEqual(
                1,
                conn.execute(
                    "SELECT COUNT(*) FROM admin_player_status_reset_operations"
                ).fetchone()[0],
            )

    def test_snapshot_and_payload_conflicts_are_rejected(self) -> None:
        self.assertEqual(
            "state_changed",
            self.reset("stale", expected_state=(101, 9, 2, 3, 4)).status,
        )
        self.assertEqual("reset", self.reset("conflict").status)
        self.assertEqual(
            "operation_conflict",
            self.reset("conflict", max_stamina=30).status,
        )

    def test_missing_user_is_rejected_without_operation(self) -> None:
        result = self.reset(
            "missing", user_id="missing", expected_state=None
        )

        self.assertEqual(result.status, "user_missing")
        with db_backend.connection(self.database) as conn:
            if conn.table_exists("admin_player_status_reset_operations"):
                self.assertEqual(
                    0,
                    conn.execute(
                        "SELECT COUNT(*) FROM admin_player_status_reset_operations"
                    ).fetchone()[0],
                )

    def test_operation_failure_rolls_back_whole_status_reset(self) -> None:
        with db_backend.transaction(self.database) as conn:
            self.service._ensure_schema(conn)
            conn.execute(
                "CREATE TRIGGER fail_status_reset_operation BEFORE INSERT ON "
                "admin_player_status_reset_operations BEGIN SELECT RAISE(ABORT,'failed'); END"
            )

        with self.assertRaises(db_backend.IntegrityError):
            self.reset("failed")

        self.assertEqual(self.state(), (101, 1, 2, 3, 4))
        with db_backend.connection(self.database) as conn:
            self.assertEqual(
                0,
                conn.execute(
                    "SELECT COUNT(*) FROM admin_player_status_reset_operations"
                ).fetchone()[0],
            )

    def test_production_single_entry_uses_service(self) -> None:
        source = (
            Path(__file__).parents[1]
            / "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_admin/__init__.py"
        ).read_text(encoding="utf-8")
        start = source.index("async def restate_")
        handler = source[start:source.index("@set_xiuxian.handle", start)]
        single = handler[handler.index("if give_qq:"):]
        self.assertIn("admin_player_status_reset_service.reset(", single)
        self.assertNotIn("sql_message.restate(give_qq)", single)
        self.assertNotIn("sql_message.update_user_stamina(give_qq", single)


if __name__ == "__main__":
    unittest.main()
