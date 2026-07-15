from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_admin.transaction_service import (
    AdminImpartStoneBatchAdjustmentService,
)
from tests.test_db_backend import db_backend


class AdminImpartStoneBatchAdjustmentTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        root = Path(self.temp.name)
        self.game_database = root / "game.db"
        self.impart_database = root / "impart.db"
        with db_backend.transaction(self.game_database) as conn:
            conn.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY)")
            conn.executemany(
                "INSERT INTO user_xiuxian VALUES(%s)",
                (("1",), ("2",), ("3",)),
            )
        with db_backend.transaction(self.impart_database) as conn:
            conn.execute("CREATE TABLE xiuxian_impart(user_id TEXT,stone_num INTEGER)")
            conn.executemany(
                "INSERT INTO xiuxian_impart VALUES(%s,%s)",
                (("1", 5), ("2", 1)),
            )
        self.service = AdminImpartStoneBatchAdjustmentService(
            self.game_database, self.impart_database
        )

    def tearDown(self) -> None:
        self.temp.cleanup()

    def stone(self, user_id):
        with db_backend.connection(self.impart_database) as conn:
            row = conn.execute(
                "SELECT stone_num FROM xiuxian_impart WHERE user_id=%s",
                (str(user_id),),
            ).fetchone()
            return int(row[0]) if row else None

    def adjust(
        self,
        operation="batch",
        user_ids=("1", "2"),
        requested_delta=2,
        **changes,
    ):
        values = dict(
            operation_id=operation,
            operator_id="admin",
            user_ids=user_ids,
            requested_delta=requested_delta,
            chunk_size=100,
        )
        values.update(changes)
        return self.service.adjust(**values)

    def test_batch_resumes_with_frozen_recipient_snapshot(self) -> None:
        first = self.adjust(
            "resume", user_ids=("2", "1", "2"), chunk_size=1
        )
        self.assertEqual(
            (first.status, first.total, first.completed, first.applied_delta),
            ("applied", 2, 1, 2),
        )
        self.assertEqual("resume", self.service.find_running("admin", 2))

        resumed = self.adjust("resume", user_ids=("1", "2", "3"))
        duplicate = self.adjust("resume", user_ids=("1", "2"))

        self.assertEqual(
            (
                resumed.completed,
                resumed.applied_delta,
                resumed.affected_users,
                resumed.skipped_users,
            ),
            (2, 4, 2, 0),
        )
        self.assertEqual(duplicate.status, "duplicate")
        self.assertEqual((self.stone("1"), self.stone("2"), self.stone("3")), (7, 3, None))
        with db_backend.connection(self.game_database) as conn:
            self.assertEqual(
                2,
                conn.execute(
                    "SELECT COUNT(*) FROM economy_log "
                    "WHERE action='admin_impart_stone_add'"
                ).fetchone()[0],
            )

    def test_deduction_clamps_each_player_and_sums_actual_delta(self) -> None:
        result = self.adjust(
            "deduct", user_ids=("1", "2", "3"), requested_delta=-3
        )

        self.assertEqual(
            (
                result.completed,
                result.applied_delta,
                result.affected_users,
                result.skipped_users,
            ),
            (3, -4, 2, 1),
        )
        self.assertEqual((self.stone("1"), self.stone("2"), self.stone("3")), (2, 0, 0))

    def test_request_conflict_keeps_original_plan(self) -> None:
        self.assertEqual("applied", self.adjust("conflict", chunk_size=1).status)

        conflict = self.adjust("conflict", requested_delta=3)

        self.assertEqual((conflict.status, conflict.completed), ("operation_conflict", 1))
        self.assertIsNone(self.service.find_running("other-admin", 2))

    def test_progress_failure_replays_child_without_second_adjustment(self) -> None:
        with db_backend.transaction(self.game_database) as conn:
            self.service._ensure_schema(conn)
            conn.execute(
                "CREATE TRIGGER reject_impart_batch_progress BEFORE INSERT ON "
                "admin_impart_stone_batch_progress BEGIN SELECT RAISE(ABORT,'failed'); END"
            )

        with self.assertRaises(db_backend.IntegrityError):
            self.adjust("progress-failure", user_ids=("1",))
        self.assertEqual(self.stone("1"), 7)
        with db_backend.connection(self.game_database) as conn:
            self.assertEqual(
                0,
                conn.execute(
                    "SELECT COUNT(*) FROM admin_impart_stone_batch_progress"
                ).fetchone()[0],
            )
        with db_backend.transaction(self.game_database) as conn:
            conn.execute("DROP TRIGGER reject_impart_batch_progress")

        resumed = self.adjust("progress-failure", user_ids=("1",))

        self.assertEqual((resumed.completed, resumed.applied_delta), (1, 2))
        self.assertEqual(self.stone("1"), 7)
        with db_backend.connection(self.game_database) as conn:
            self.assertEqual(
                1,
                conn.execute(
                    "SELECT COUNT(*) FROM economy_log "
                    "WHERE action='admin_impart_stone_add'"
                ).fetchone()[0],
            )

    def test_production_all_entry_uses_resumable_batch(self) -> None:
        source = (
            Path(__file__).parents[1]
            / "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_admin/__init__.py"
        ).read_text(encoding="utf-8")
        start = source.index("async def ccll_command_")
        handler = source[start:source.index("@adjust_exp_command.handle", start)]
        self.assertIn("admin_impart_stone_batch_adjustment_service.find_running(", handler)
        self.assertIn("admin_impart_stone_batch_adjustment_service.adjust(", handler)
        self.assertNotIn("update_impart_stone_all(", handler)


if __name__ == "__main__":
    unittest.main()
