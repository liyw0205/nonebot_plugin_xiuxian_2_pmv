from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_admin.player_status_batch_reset_service import (
    AdminPlayerStatusBatchResetService,
)
from tests.test_db_backend import db_backend


class AdminPlayerStatusBatchResetTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.database = Path(self.temp.name) / "game.db"
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "CREATE TABLE user_xiuxian("
                "user_id TEXT PRIMARY KEY,exp INTEGER,hp INTEGER,mp INTEGER,"
                "atk INTEGER,user_stamina INTEGER)"
            )
            conn.executemany(
                "INSERT INTO user_xiuxian VALUES(%s,%s,%s,%s,%s,%s)",
                (
                    ("1", 100, 1, 2, 3, 4),
                    ("2", 201, 5, 6, 7, 8),
                    ("3", 300, 9, 10, 11, 12),
                ),
            )
        self.service = AdminPlayerStatusBatchResetService(self.database)

    def tearDown(self) -> None:
        self.temp.cleanup()

    def state(self, user_id):
        with db_backend.connection(self.database) as conn:
            row = conn.execute(
                "SELECT exp,hp,mp,atk,user_stamina FROM user_xiuxian "
                "WHERE user_id=%s",
                (str(user_id),),
            ).fetchone()
            return tuple(row) if row else None

    def reset(self, operation="batch", user_ids=("1", "2"), **changes):
        values = dict(
            operation_id=operation,
            operator_id="admin",
            user_ids=user_ids,
            max_stamina=20,
            chunk_size=100,
        )
        values.update(changes)
        return self.service.reset(**values)

    def test_batch_resumes_with_frozen_player_snapshot(self) -> None:
        first = self.reset(
            "resume", user_ids=("2", "1", "2"), chunk_size=1
        )

        self.assertEqual(
            (first.status, first.total, first.completed, first.reset_users),
            ("applied", 2, 1, 1),
        )
        self.assertEqual("resume", self.service.find_running("admin", 20))
        self.assertEqual(self.state("1"), (100, 50, 100, 10, 20))
        self.assertEqual(self.state("2"), (201, 5, 6, 7, 8))

        resumed = self.reset("resume", user_ids=("1", "2", "3"))
        duplicate = self.reset("resume", user_ids=("1", "2"))

        self.assertEqual(
            (
                resumed.completed,
                resumed.reset_users,
                resumed.skipped_users,
                duplicate.status,
            ),
            (2, 2, 0, "duplicate"),
        )
        self.assertEqual(self.state("2"), (201, 100, 201, 20, 20))
        self.assertEqual(self.state("3"), (300, 9, 10, 11, 12))

    def test_request_conflict_keeps_original_plan(self) -> None:
        self.assertEqual("applied", self.reset("conflict", chunk_size=1).status)

        conflict = self.reset("conflict", max_stamina=30)

        self.assertEqual((conflict.status, conflict.completed), ("operation_conflict", 1))
        self.assertIsNone(self.service.find_running("other-admin", 20))

    def test_progress_failure_replays_child_without_second_reset(self) -> None:
        with db_backend.transaction(self.database) as conn:
            self.service._ensure_schema(conn)
            conn.execute(
                "CREATE TRIGGER reject_status_batch_progress BEFORE INSERT ON "
                "admin_player_status_batch_reset_progress "
                "BEGIN SELECT RAISE(ABORT,'failed'); END"
            )

        with self.assertRaises(db_backend.IntegrityError):
            self.reset("progress-failure", user_ids=("1",))
        self.assertEqual(self.state("1"), (100, 50, 100, 10, 20))
        with db_backend.connection(self.database) as conn:
            self.assertEqual(
                1,
                conn.execute(
                    "SELECT COUNT(*) FROM admin_player_status_reset_operations"
                ).fetchone()[0],
            )
            self.assertEqual(
                0,
                conn.execute(
                    "SELECT COUNT(*) FROM admin_player_status_batch_reset_progress"
                ).fetchone()[0],
            )
        with db_backend.transaction(self.database) as conn:
            conn.execute("DROP TRIGGER reject_status_batch_progress")

        resumed = self.reset("progress-failure", user_ids=("1",))

        self.assertEqual((resumed.completed, resumed.reset_users), (1, 1))
        self.assertEqual(self.state("1"), (100, 50, 100, 10, 20))
        with db_backend.connection(self.database) as conn:
            self.assertEqual(
                1,
                conn.execute(
                    "SELECT COUNT(*) FROM admin_player_status_reset_operations"
                ).fetchone()[0],
            )

    def test_missing_frozen_player_is_recorded_as_skipped(self) -> None:
        first = self.reset("missing", chunk_size=1)
        self.assertEqual(first.completed, 1)
        with db_backend.transaction(self.database) as conn:
            conn.execute("DELETE FROM user_xiuxian WHERE user_id='2'")

        resumed = self.reset("missing", user_ids=("1", "2", "3"))

        self.assertEqual(
            (resumed.completed, resumed.reset_users, resumed.skipped_users),
            (2, 1, 1),
        )
        self.assertEqual(self.state("3"), (300, 9, 10, 11, 12))

    def test_running_plan_finishes_when_current_player_query_is_empty(self) -> None:
        first = self.reset("empty-resume", chunk_size=1)
        self.assertEqual(first.completed, 1)
        with db_backend.transaction(self.database) as conn:
            conn.execute("DELETE FROM user_xiuxian")

        resumed = self.reset("empty-resume", user_ids=())

        self.assertEqual(
            (resumed.total, resumed.completed, resumed.reset_users, resumed.skipped_users),
            (2, 2, 1, 1),
        )
        with self.assertRaisesRegex(ValueError, "requires users"):
            self.reset("new-empty", user_ids=())

    def test_production_all_entry_uses_resumable_batch(self) -> None:
        source = (
            Path(__file__).parents[1]
            / "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_admin/__init__.py"
        ).read_text(encoding="utf-8")
        start = source.index("async def restate_")
        handler = source[start:source.index("@set_xiuxian.handle", start)]
        all_branch = handler[handler.index("if not args:"):handler.index("plain_args =")]
        self.assertIn("admin_player_status_batch_reset_service.find_running(", all_branch)
        self.assertIn("admin_player_status_batch_reset_service.reset(", all_branch)
        self.assertNotIn("sql_message.restate()", all_branch)
        self.assertNotIn("sql_message.update_all_users_stamina(", all_branch)


if __name__ == "__main__":
    unittest.main()
