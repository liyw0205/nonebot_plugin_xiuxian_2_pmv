from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_work.transaction_service import (
    WorkDailyRefreshResetService,
)
from tests.test_db_backend import db_backend


class WorkDailyRefreshResetTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.database = Path(self.temp.name) / "work.db"
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,work_num INTEGER)"
            )
            conn.executemany(
                "INSERT INTO user_xiuxian VALUES(%s,%s)",
                (("u1", 0), ("u2", 2), ("u3", 5)),
            )
        self.service = WorkDailyRefreshResetService(self.database)

    def tearDown(self):
        self.temp.cleanup()

    def reset(self, business_date="2026-07-14", count=5, chunk_size=500):
        return self.service.reset(
            business_date,
            count,
            chunk_size=chunk_size,
            updated_at=f"{business_date} 08:00:30",
        )

    def users(self):
        with db_backend.connection(self.database) as conn:
            return {
                str(row[0]): int(row[1])
                for row in conn.execute(
                    "SELECT user_id,work_num FROM user_xiuxian ORDER BY user_id"
                ).fetchall()
            }

    def targets(self, business_date="2026-07-14"):
        with db_backend.connection(self.database) as conn:
            return [
                (str(row[0]), str(row[1]), row[2], row[3])
                for row in conn.execute(
                    "SELECT user_id,status,previous_count,final_count "
                    "FROM work_daily_refresh_reset_targets WHERE business_date=%s "
                    "ORDER BY user_id",
                    (business_date,),
                ).fetchall()
            ]

    def test_freezes_players_and_resets_in_chunks(self):
        first = self.reset(chunk_size=2)
        self.assertEqual(
            (first.status, first.task_status, first.total, first.completed, first.changed),
            ("applied", "running", 3, 2, 2),
        )
        self.assertEqual(self.users(), {"u1": 5, "u2": 5, "u3": 5})

        with db_backend.transaction(self.database) as conn:
            conn.execute("DELETE FROM user_xiuxian WHERE user_id='u3'")
            conn.execute("INSERT INTO user_xiuxian VALUES('u4',0)")
        completed = self.reset(chunk_size=2)
        duplicate = self.reset(chunk_size=2)
        self.assertEqual(
            (completed.task_status, completed.total, completed.completed, completed.changed),
            ("completed", 3, 3, 2),
        )
        self.assertEqual(duplicate.status, "duplicate")
        self.assertEqual(self.users(), {"u1": 5, "u2": 5, "u4": 0})
        self.assertEqual(
            self.targets(),
            [
                ("u1", "applied", 0, 5),
                ("u2", "applied", 2, 5),
                ("u3", "skipped", None, None),
            ],
        )

    def test_same_business_date_rejects_changed_reset_count(self):
        applied = self.reset()
        conflict = self.reset(count=6)
        self.assertEqual((applied.task_status, conflict.status), ("completed", "operation_conflict"))
        self.assertEqual(self.users(), {"u1": 5, "u2": 5, "u3": 5})

    def test_failed_chunk_rolls_back_and_next_run_resumes_pending_targets(self):
        first = self.reset(chunk_size=1)
        self.assertEqual((first.completed, first.changed), (1, 1))
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "CREATE TRIGGER fail_second_reset BEFORE UPDATE OF status "
                "ON work_daily_refresh_reset_targets "
                "WHEN NEW.user_id='u2' AND NEW.status='applied' "
                "BEGIN SELECT RAISE(ABORT,'failed'); END"
            )
        with self.assertRaises(db_backend.IntegrityError):
            self.reset(chunk_size=1)
        self.assertEqual(self.users(), {"u1": 5, "u2": 2, "u3": 5})
        self.assertEqual(
            self.targets(),
            [
                ("u1", "applied", 0, 5),
                ("u2", "pending", None, None),
                ("u3", "pending", None, None),
            ],
        )

        with db_backend.transaction(self.database) as conn:
            conn.execute("DROP TRIGGER fail_second_reset")
        resumed = self.reset(chunk_size=10)
        self.assertEqual(
            (resumed.status, resumed.task_status, resumed.completed, resumed.changed),
            ("applied", "completed", 3, 2),
        )
        self.assertEqual(self.users(), {"u1": 5, "u2": 5, "u3": 5})

    def test_empty_player_set_is_completed_and_idempotent(self):
        with db_backend.transaction(self.database) as conn:
            conn.execute("DELETE FROM user_xiuxian")
        first = self.reset()
        duplicate = self.reset()
        self.assertEqual(
            (first.status, first.task_status, first.total, duplicate.status),
            ("applied", "completed", 0, "duplicate"),
        )

    def test_duplicate_historical_user_rows_are_one_reset_target(self):
        with db_backend.transaction(self.database) as conn:
            conn.execute("DROP TABLE user_xiuxian")
            conn.execute("CREATE TABLE user_xiuxian(user_id TEXT,work_num INTEGER)")
            conn.executemany(
                "INSERT INTO user_xiuxian VALUES(%s,%s)",
                (("duplicate", 0), ("duplicate", 2), ("normal", 5)),
            )

        result = self.reset()

        self.assertEqual(
            (result.status, result.task_status, result.total, result.changed),
            ("applied", "completed", 2, 1),
        )
        with db_backend.connection(self.database) as conn:
            self.assertEqual(
                [
                    tuple(row)
                    for row in conn.execute(
                        "SELECT work_num FROM user_xiuxian "
                        "WHERE user_id='duplicate' ORDER BY rowid"
                    ).fetchall()
                ],
                [(5,), (5,)],
            )
        self.assertEqual(
            self.targets(),
            [
                ("duplicate", "applied", 0, 5),
                ("normal", "applied", 5, 5),
            ],
        )

    def test_production_reset_entry_uses_batch_service(self):
        source = (
            Path(__file__).parents[1]
            / "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_work/__init__.py"
        ).read_text(encoding="utf-8")
        handler = source.split("async def resetrefreshnum", 1)[1].split(
            "async def delayed_reminder", 1
        )[0]
        self.assertIn("work_daily_refresh_reset_service.reset(", handler)
        self.assertIn("return result", handler)
        self.assertIn("await asyncio.sleep(0)", handler)
        self.assertNotIn("sql_message.reset_work_num(", handler)


if __name__ == "__main__":
    unittest.main()
