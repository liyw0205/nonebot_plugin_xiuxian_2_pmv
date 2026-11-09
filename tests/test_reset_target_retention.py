"""Retention for the daily-reset target ledgers: only dead detail may expire.

`work_daily_refresh_reset_targets` and `world_boss_daily_limit_reset_targets` hold
one row per player per business date.  A date whose parent operation row is
`completed` answers `duplicate` from that parent row (totals, changed, skipped)
*before* any target row is read, by both the feature repositories and the legacy
compatibility services.  The detail of such an old, finished run is what expires.

These tests pin the guards -- pending / unknown-status / non-ISO-date / missing
parent / running parent rows all stay -- and compare the retry response of the
real feature *and* legacy paths byte-for-byte before and after retention.
"""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.compatibility.legacy_work_daily_refresh_reset import (
    WorkDailyRefreshResetService,
)
from nonebot_plugin_xiuxian_2.features.boss.migrations import (
    apply_boss_player_schema,
)
from nonebot_plugin_xiuxian_2.features.boss.world_boss_repository import (
    WorldBossDailyLimitResetSqlRepository,
)
from nonebot_plugin_xiuxian_2.features.maintenance.reset_target_retention import (
    DEFAULT_RETENTION_DAYS,
    cutoff_date,
    prune_reset_targets,
)
from nonebot_plugin_xiuxian_2.features.work.maintenance_repository import (
    WorkDailyRefreshResetRepository,
)
from nonebot_plugin_xiuxian_2.features.work.migrations import apply_work_daily_refresh_reset
from nonebot_plugin_xiuxian_2.infrastructure.clock import SystemClock
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_boss.transaction_service import (
    WorldBossDailyLimitResetService,
)

OLD = "2026-01-05"          # far outside any window
RECENT = "2026-11-01"
TODAY = "2026-11-09"
UNKNOWN_DATE = "2026/01/04"  # non-ISO: never eligible


class _Base(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        root = Path(self.tmp.name)
        self.game = root / "xiuxian.db"
        self.player = root / "player.db"
        self.state = root / "maintenance" / "reset_target_retention.json"
        with DatabaseUnitOfWork(self.game) as uow:
            apply_work_daily_refresh_reset(uow)
            uow.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY, work_num INTEGER DEFAULT 0)")
        with DatabaseUnitOfWork(self.player) as uow:
            apply_boss_player_schema(uow)
            uow.execute(
                "CREATE TABLE IF NOT EXISTS boss(user_id TEXT PRIMARY KEY,"
                "boss_integral INTEGER DEFAULT 0,boss_stone INTEGER DEFAULT 0,"
                "boss_battle_count INTEGER DEFAULT 0)"
            )

    def tearDown(self) -> None:
        self.tmp.cleanup()

    # ---------------------------------------------------------------- fixtures
    def seed_work(self, business_date: str, *, status="completed", target_status="applied",
                  users=("u1", "u2"), reset_count=3, total=2, completed=2, changed=1) -> None:
        with DatabaseUnitOfWork(self.game) as uow:
            uow.execute(
                "INSERT OR REPLACE INTO work_daily_refresh_reset_operations"
                "(business_date,reset_count,total,completed,changed,status,created_at,updated_at)"
                " VALUES(?,?,?,?,?,?,?,?)",
                (business_date, reset_count, total, completed, changed, status,
                 "2026-01-05 00:00:00", "2026-01-05 00:00:00"),
            )
            for index, user in enumerate(users):
                uow.execute(
                    "INSERT OR REPLACE INTO work_daily_refresh_reset_targets"
                    "(business_date,user_id,status,previous_count,final_count,updated_at)"
                    " VALUES(?,?,?,?,?,?)",
                    (business_date, user, target_status, index, reset_count, "2026-01-05 00:00:00"),
                )
            for user in users:
                uow.execute("INSERT OR IGNORE INTO user_xiuxian(user_id,work_num) VALUES(?,?)",
                            (user, reset_count))

    def seed_boss(self, business_date: str, *, status="completed", target_status="applied",
                  users=("u1", "u2"), total=2, completed=2, changed=1, skipped=0) -> None:
        with DatabaseUnitOfWork(self.player) as uow:
            uow.execute(
                "INSERT OR REPLACE INTO world_boss_daily_limit_reset_operations"
                "(business_date,total,completed,changed,skipped,status,created_at,updated_at)"
                " VALUES(?,?,?,?,?,?,?,?)",
                (business_date, total, completed, changed, skipped, status,
                 "2026-01-05 00:00:00", "2026-01-05 00:00:00"),
            )
            for user in users:
                uow.execute(
                    "INSERT OR REPLACE INTO world_boss_daily_limit_reset_targets"
                    "(business_date,user_id,status,previous_integral,previous_stone,"
                    "previous_battle_count,updated_at) VALUES(?,?,?,?,?,?,?)",
                    (business_date, user, target_status, 5, 6, 7, "2026-01-05 00:00:00"),
                )
            for user in users:
                uow.execute(
                    "INSERT OR IGNORE INTO boss(user_id,boss_integral,boss_stone,boss_battle_count)"
                    " VALUES(?,0,0,0)", (user,))

    def count(self, db: Path, table: str, business_date: str | None = None) -> int:
        with DatabaseUnitOfWork(db, read_only=True) as uow:
            if business_date is None:
                return int(uow.query_one('SELECT COUNT(*) AS n FROM "%s"' % table)["n"])
            return int(uow.query_one(
                'SELECT COUNT(*) AS n FROM "%s" WHERE business_date=?' % table, (business_date,))["n"])

    def run_retention(self, **kwargs):
        return prune_reset_targets(
            self.game, self.player, state_path=self.state, today=TODAY, logger=None, **kwargs
        )


class RetentionGuardTests(_Base):
    def test_cutoff_is_conservative_and_iso(self) -> None:
        self.assertEqual(cutoff_date(TODAY, 30), "2026-10-10")
        self.assertEqual(DEFAULT_RETENTION_DAYS, 30)

    def test_old_terminal_detail_expires_but_parent_and_player_state_survive(self) -> None:
        self.seed_work(OLD)
        self.seed_boss(OLD)
        report = self.run_retention()
        self.assertEqual(report.deleted, 4)  # 2 work + 2 boss targets
        self.assertEqual(self.count(self.game, "work_daily_refresh_reset_targets", OLD), 0)
        self.assertEqual(self.count(self.player, "world_boss_daily_limit_reset_targets", OLD), 0)
        # Parent markers and their aggregates are untouched.
        with DatabaseUnitOfWork(self.game, read_only=True) as uow:
            op = uow.query_one(
                "SELECT reset_count,total,completed,changed,status FROM "
                "work_daily_refresh_reset_operations WHERE business_date=?", (OLD,))
        self.assertEqual((int(op["reset_count"]), int(op["total"]), int(op["completed"]),
                          int(op["changed"]), str(op["status"])), (3, 2, 2, 1, "completed"))
        with DatabaseUnitOfWork(self.player, read_only=True) as uow:
            op = uow.query_one(
                "SELECT total,completed,changed,skipped,status FROM "
                "world_boss_daily_limit_reset_operations WHERE business_date=?", (OLD,))
        self.assertEqual((int(op["total"]), int(op["completed"]), int(op["changed"]),
                          int(op["skipped"]), str(op["status"])), (2, 2, 1, 0, "completed"))
        # Player state untouched.
        with DatabaseUnitOfWork(self.game, read_only=True) as uow:
            rows = uow.query_all("SELECT user_id,work_num FROM user_xiuxian ORDER BY user_id")
        self.assertEqual([(r["user_id"], int(r["work_num"])) for r in rows], [("u1", 3), ("u2", 3)])

    def test_pending_unknown_status_and_unknown_date_rows_stay(self) -> None:
        self.seed_work(OLD, target_status="pending")
        self.seed_boss(OLD, target_status="pending")
        # A status this build does not know must never be treated as terminal.
        self.seed_work("2026-01-06", users=("u3",))
        with DatabaseUnitOfWork(self.game) as uow:
            uow.execute("UPDATE work_daily_refresh_reset_targets SET status='mystery' WHERE business_date='2026-01-06'")
        # Non-ISO business dates cannot be ordered against the cutoff.
        self.seed_work(UNKNOWN_DATE, users=("u4",))

        self.run_retention()

        self.assertEqual(self.count(self.game, "work_daily_refresh_reset_targets", OLD), 2)
        self.assertEqual(self.count(self.player, "world_boss_daily_limit_reset_targets", OLD), 2)
        self.assertEqual(self.count(self.game, "work_daily_refresh_reset_targets", "2026-01-06"), 1)
        self.assertEqual(self.count(self.game, "work_daily_refresh_reset_targets", UNKNOWN_DATE), 1)

    def test_running_parent_rows_stay_for_resume(self) -> None:
        self.seed_work(OLD, status="running", target_status="applied", completed=1)
        self.seed_boss(OLD, status="running", target_status="applied", completed=1)
        self.run_retention()
        self.assertEqual(self.count(self.game, "work_daily_refresh_reset_targets", OLD), 2)
        self.assertEqual(self.count(self.player, "world_boss_daily_limit_reset_targets", OLD), 2)

    def test_rows_without_a_parent_marker_stay(self) -> None:
        with DatabaseUnitOfWork(self.game) as uow:
            uow.execute(
                "INSERT INTO work_daily_refresh_reset_targets"
                "(business_date,user_id,status,updated_at) VALUES('2026-01-07','legacy','applied','x')")
        self.run_retention()
        self.assertEqual(self.count(self.game, "work_daily_refresh_reset_targets", "2026-01-07"), 1)

    def test_recent_rows_within_the_window_stay(self) -> None:
        self.seed_work(RECENT)
        self.seed_boss(RECENT)
        report = self.run_retention()
        self.assertEqual(report.deleted, 0)
        self.assertEqual(self.count(self.game, "work_daily_refresh_reset_targets", RECENT), 2)
        self.assertEqual(self.count(self.player, "world_boss_daily_limit_reset_targets", RECENT), 2)

    def test_missing_tables_are_skipped_without_creating_schema(self) -> None:
        empty = Path(self.tmp.name) / "empty.db"
        empty.write_bytes(b"")
        report = prune_reset_targets(empty, empty, state_path=self.state, today=TODAY)
        self.assertEqual(report.deleted, 0)
        self.assertTrue(all(task.skipped for task in report.tasks.values()))
        with DatabaseUnitOfWork(empty, read_only=True) as uow:
            tables = {row["name"] for row in uow.query_all("SELECT name FROM sqlite_master WHERE type='table'")}
        self.assertEqual(tables, set())


class RetryEqualityTests(_Base):
    def _work_duplicates(self):
        feature = WorkDailyRefreshResetRepository(self.game, clock=SystemClock())
        legacy = WorkDailyRefreshResetService(self.game)
        return feature.reset(OLD, 3), legacy.reset(OLD, 3)

    def _boss_duplicates(self):
        feature = WorldBossDailyLimitResetSqlRepository(self.player)
        legacy = WorldBossDailyLimitResetService(self.player)
        return feature.reset(OLD), legacy.reset(OLD)

    @staticmethod
    def _shape(result):
        return (result.status, result.business_date, result.task_status, result.total,
                result.completed, result.changed)

    def test_feature_and_legacy_retry_are_identical_after_retention(self) -> None:
        self.seed_work(OLD)
        self.seed_boss(OLD)
        work_before = [self._shape(r) for r in self._work_duplicates()]
        boss_before = [self._shape(r) for r in self._boss_duplicates()]
        self.assertTrue(all(item[0] == "duplicate" for item in work_before))
        self.assertTrue(all(item[0] == "duplicate" for item in boss_before))

        report = self.run_retention()
        self.assertEqual(report.deleted, 4)

        work_after = [self._shape(r) for r in self._work_duplicates()]
        boss_after = [self._shape(r) for r in self._boss_duplicates()]
        self.assertEqual(work_after, work_before,
                         "an old completed date must still look processed with the same totals")
        self.assertEqual(boss_after, boss_before)
        # The date still reports as processed, not "unprocessed".
        self.assertEqual(work_after[0][2], "completed")
        self.assertEqual(work_after[0][3], 2)

    def test_running_date_still_resumes_after_retention_ran(self) -> None:
        # An old running date keeps its pending detail, so a retry can finish it.
        self.seed_work(OLD, status="running", target_status="pending", completed=0)
        with DatabaseUnitOfWork(self.game) as uow:
            uow.execute("UPDATE user_xiuxian SET work_num=9")
        self.run_retention()
        self.assertEqual(self.count(self.game, "work_daily_refresh_reset_targets", OLD), 2)
        result = WorkDailyRefreshResetRepository(self.game, clock=SystemClock()).reset(OLD, 3)
        self.assertEqual(result.status, "applied")
        with DatabaseUnitOfWork(self.game, read_only=True) as uow:
            values = [int(r["work_num"]) for r in uow.query_all("SELECT work_num FROM user_xiuxian")]
        self.assertEqual(values, [3, 3])


class BoundedRunTests(_Base):
    def test_interrupted_run_resumes_and_second_boot_is_a_noop(self) -> None:
        for index in range(4):
            self.seed_work("2026-01-%02d" % (index + 1), users=("u1", "u2", "u3"))
        interrupted = self.run_retention(budget_seconds=0.0, clock=lambda: 0.0)
        self.assertEqual(interrupted.deleted, 0)
        self.assertTrue(any(task.candidates_remaining for task in interrupted.tasks.values()))

        completed = self.run_retention(batch_size=2)
        self.assertEqual(completed.deleted, 12)
        again = self.run_retention(batch_size=2)
        self.assertEqual(again.deleted, 0)
        self.assertTrue(all(task.skipped or task.deleted == 0 for task in again.tasks.values()))

    def test_batches_are_bounded_per_transaction(self) -> None:
        for index in range(3):
            self.seed_work("2026-01-%02d" % (index + 1), users=("u1", "u2"))
        report = self.run_retention(batch_size=1)
        self.assertEqual(report.deleted, 6)
        self.assertGreaterEqual(report.tasks["work_reset_targets"].batches, 6)


if __name__ == "__main__":
    unittest.main()
