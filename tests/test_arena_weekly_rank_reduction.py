from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_arena.transaction_service import (
    ArenaWeeklyRankReductionService,
)
from tests.test_db_backend import db_backend


class ArenaWeeklyRankReductionTests(unittest.TestCase):
    business_week = "2026-W29"

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.database = Path(self.temp.name) / "player.db"
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "CREATE TABLE arena(user_id TEXT PRIMARY KEY,score INTEGER,rank TEXT,"
                "win_streak INTEGER)"
            )
            conn.executemany(
                "INSERT INTO arena VALUES(%s,%s,%s,%s)",
                (
                    ("u1", 1100, "青铜", 2),
                    ("u2", 2000, "黄金", 3),
                    ("u3", 3300, "王者", 5),
                    ("u4", 1000, "青铜", 0),
                ),
            )
        self.service = ArenaWeeklyRankReductionService(self.database)

    def tearDown(self):
        self.temp.cleanup()

    def reduce(self, week=None, steps=2, chunk_size=500):
        return self.service.reduce(
            week or self.business_week,
            steps,
            chunk_size=chunk_size,
            updated_at="2026-07-17 20:00:00",
        )

    def users(self):
        with db_backend.connection(self.database) as conn:
            return {
                str(row[0]): (int(row[1]), str(row[2]), int(row[3]))
                for row in conn.execute(
                    "SELECT user_id,score,rank,win_streak FROM arena ORDER BY user_id"
                ).fetchall()
            }

    def targets(self):
        with db_backend.connection(self.database) as conn:
            return [
                tuple(row)
                for row in conn.execute(
                    "SELECT user_id,previous_score,previous_rank,previous_win_streak,"
                    "target_score,target_rank,status,error_text "
                    "FROM arena_weekly_rank_reduction_targets WHERE business_week=%s "
                    "ORDER BY ordinal",
                    (self.business_week,),
                ).fetchall()
            ]

    def test_freezes_targets_and_resumes_without_overwriting_new_state(self):
        first = self.reduce(chunk_size=2)
        self.assertEqual(
            (first.status, first.task_status, first.total, first.completed, first.changed),
            ("applied", "running", 4, 2, 2),
        )
        self.assertEqual(self.users()["u1"], (1000, "青铜", 0))
        self.assertEqual(self.users()["u2"], (1000, "青铜", 0))

        with db_backend.transaction(self.database) as conn:
            conn.execute("UPDATE arena SET score=3400 WHERE user_id='u3'")
            conn.execute("INSERT INTO arena VALUES('u5',2800,'钻石',4)")
        completed = self.reduce(chunk_size=10)
        duplicate = self.reduce()

        self.assertEqual(
            (
                completed.task_status,
                completed.total,
                completed.completed,
                completed.changed,
                completed.skipped,
                completed.conflicted,
                duplicate.status,
            ),
            ("completed", 4, 4, 2, 1, 1, "duplicate"),
        )
        self.assertEqual(self.users()["u3"], (3400, "王者", 5))
        self.assertEqual(self.users()["u4"], (1000, "青铜", 0))
        self.assertEqual(self.users()["u5"], (2800, "钻石", 4))
        self.assertEqual(
            self.targets(),
            [
                ("u1", 1100, "青铜", 2, 1000, "青铜", "applied", ""),
                ("u2", 2000, "黄金", 3, 1000, "青铜", "applied", ""),
                ("u3", 3300, "王者", 5, 2300, "铂金", "conflict", "state_changed"),
                ("u4", 1000, "青铜", 0, 1000, "青铜", "applied", ""),
            ],
        )

    def test_business_week_is_idempotent_and_parameters_must_match(self):
        applied = self.reduce(week="2026-07-17")
        duplicate = self.reduce(week="2026-W29")
        conflict = self.reduce(week="2026-W29", steps=1)

        self.assertEqual("2026-W29", applied.business_week)
        self.assertEqual("duplicate", duplicate.status)
        self.assertEqual("operation_conflict", conflict.status)

        next_week = self.reduce(week="2026-W30")
        self.assertEqual(("applied", "completed", 4), (
            next_week.status,
            next_week.task_status,
            next_week.total,
        ))

    def test_failed_chunk_rolls_back_and_records_error_before_resume(self):
        first = self.reduce(chunk_size=1)
        self.assertEqual((first.completed, first.changed), (1, 1))
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "CREATE TRIGGER fail_second_rank BEFORE UPDATE OF status ON "
                "arena_weekly_rank_reduction_targets WHEN NEW.user_id='u2' "
                "AND NEW.status='applied' BEGIN SELECT RAISE(ABORT,'failed rank'); END"
            )

        with self.assertRaises(db_backend.IntegrityError):
            self.reduce(chunk_size=1)

        self.assertEqual(self.users()["u2"], (2000, "黄金", 3))
        with db_backend.connection(self.database) as conn:
            target_status = conn.execute(
                "SELECT status FROM arena_weekly_rank_reduction_targets "
                "WHERE business_week=%s AND user_id='u2'",
                (self.business_week,),
            ).fetchone()[0]
            last_error = conn.execute(
                "SELECT last_error FROM arena_weekly_rank_reduction_operations "
                "WHERE business_week=%s",
                (self.business_week,),
            ).fetchone()[0]
        self.assertEqual("pending", target_status)
        self.assertIn("failed rank", last_error)

        with db_backend.transaction(self.database) as conn:
            conn.execute("DROP TRIGGER fail_second_rank")
        resumed = self.reduce(chunk_size=10)
        self.assertEqual(
            ("applied", "completed", 4, 3, ""),
            (
                resumed.status,
                resumed.task_status,
                resumed.completed,
                resumed.changed,
                resumed.last_error,
            ),
        )

    def test_missing_frozen_user_is_skipped(self):
        first = self.reduce(chunk_size=1)
        self.assertEqual(first.completed, 1)
        with db_backend.transaction(self.database) as conn:
            conn.execute("DELETE FROM arena WHERE user_id='u2'")

        completed = self.reduce(chunk_size=10)

        self.assertEqual((4, 2, 1, 0), (
            completed.completed,
            completed.changed,
            completed.skipped,
            completed.conflicted,
        ))
        self.assertEqual("skipped", self.targets()[1][6])
        self.assertEqual("user_missing", self.targets()[1][7])

    def test_empty_arena_completes_and_replays(self):
        with db_backend.transaction(self.database) as conn:
            conn.execute("DELETE FROM arena")

        first = self.reduce()
        duplicate = self.reduce()

        self.assertEqual(
            ("applied", "completed", 0, "duplicate"),
            (first.status, first.task_status, first.total, duplicate.status),
        )

    def test_scheduler_entry_uses_resumable_weekly_service(self):
        root = Path(__file__).resolve().parents[1] / "nonebot_plugin_xiuxian_2/xiuxian"
        arena_source = (root / "xiuxian_arena/__init__.py").read_text(encoding="utf-8")
        start = arena_source.index("async def reduce_arena_rank")
        handler = arena_source[start:arena_source.index("async def use_arena_challenge_ticket", start)]
        self.assertIn("arena_weekly_rank_reduction_service.reduce(", handler)
        self.assertIn("await asyncio.sleep(0)", handler)
        self.assertNotIn("arena_limit.reduce_all_users_rank(", handler)

        limit_source = (root / "xiuxian_arena/arena_limit.py").read_text(encoding="utf-8")
        self.assertNotIn("def reduce_all_users_rank(", limit_source)
        scheduler_source = (root / "xiuxian_scheduler/__init__.py").read_text(
            encoding="utf-8"
        )
        self.assertIn(
            'await _run_job("竞技场每周降段", reduce_arena_rank, 2)',
            scheduler_source,
        )


if __name__ == "__main__":
    unittest.main()
