from __future__ import annotations

import sqlite3
import tempfile
import unittest
from datetime import datetime
from pathlib import Path

from ....infrastructure.database import DatabaseUnitOfWork
from ....plugin import build_migrations, migrations_for_database
from ..application import SectApplication
from ..migrations import apply_sect_task_claim_operations, apply_sect_task_state
from ..task_state_repository import SectTaskStateSqlRepository


class _Clock:
    def now(self):
        return datetime(2026, 7, 11, 8, 9, 10)


class _Random:
    def choice(self, values):
        return values[-1]


class SectTaskStateRepositoryTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.database = Path(self.temp.name) / "sect.db"
        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            apply_sect_task_state(uow)
            apply_sect_task_claim_operations(uow)
            uow.execute(
                "CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,sect_id INTEGER,sect_task INTEGER)"
            )
            uow.execute("CREATE TABLE sects(sect_id INTEGER PRIMARY KEY)")
            uow.execute("INSERT INTO user_xiuxian VALUES('u',7,0)")
            uow.execute("INSERT INTO sects VALUES(7)")
        self.repository = SectTaskStateSqlRepository(self.database)

    def tearDown(self):
        self.temp.cleanup()

    def test_migration_is_idempotent_and_keeps_existing_state(self):
        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            uow.execute(
                "INSERT INTO sect_task_state "
                "(user_id,sect_id,task_key,task_data,period,accepted_at,updated_at) "
                "VALUES('u',1,'trial','{}','2026-07-11','a','a')"
            )
            uow.execute(
                "INSERT INTO sect_task_claim_operations "
                "(operation_id,user_id,sect_id,period,task_key,task_data) "
                "VALUES('claim-op','u',1,'2026-07-11','trial','{}')"
            )
            apply_sect_task_state(uow)
            apply_sect_task_claim_operations(uow)
        with sqlite3.connect(self.database) as connection:
            self.assertEqual(
                ("trial",),
                connection.execute(
                    "SELECT task_key FROM sect_task_state WHERE user_id='u'"
                ).fetchone(),
            )
            index = connection.execute(
                "SELECT 1 FROM sqlite_master WHERE type='index' "
                "AND name='idx_sect_task_state_sect_period'"
            ).fetchone()
            receipt_table = connection.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' "
                "AND name='sect_task_claim_operations'"
            ).fetchone()
            receipt = connection.execute(
                "SELECT user_id,sect_id,period,task_key,task_data "
                "FROM sect_task_claim_operations WHERE operation_id='claim-op'"
            ).fetchone()
        self.assertIsNotNone(index)
        self.assertIsNotNone(receipt_table)
        self.assertEqual(("u", 1, "2026-07-11", "trial", "{}"), receipt)

    def test_application_accepts_reads_completes_and_clears_current_task(self):
        application = SectApplication(
            self.database, clock=_Clock(), random_source=_Random()
        )
        config = {"first": {"type": 1}, "chosen": {"type": 2, "cost": 5}}
        accepted = application.accept_task("u", "7", config)
        self.assertEqual(
            ("chosen", {"type": 2, "cost": 5}, 7, "2026-07-11", "accepted"),
            (
                accepted["任务名称"],
                accepted["任务内容"],
                accepted["sect_id"],
                accepted["period"],
                accepted["status"],
            ),
        )
        self.assertEqual(accepted, application.get_active_task("u"))

        application.complete_task("u")
        self.assertIsNone(application.get_active_task("u"))
        with DatabaseUnitOfWork(self.database, read_only=True) as uow:
            completed = uow.query_one(
                "SELECT status,progress,target,completed_at FROM sect_task_state "
                "WHERE user_id='u' AND period='2026-07-11'"
            )
        self.assertEqual("completed", completed["status"])
        self.assertEqual(completed["target"], completed["progress"])
        self.assertIsNotNone(completed["completed_at"])

        application.accept_task("u", 8, config)
        application.clear_task("u")
        with DatabaseUnitOfWork(self.database, read_only=True) as uow:
            self.assertIsNone(
                uow.query_one(
                    "SELECT 1 AS present FROM sect_task_state WHERE user_id='u'"
                )
            )

    def test_bad_json_is_tolerated_and_completed_rows_are_not_active(self):
        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            uow.execute(
                "INSERT INTO sect_task_state "
                "(user_id,sect_id,task_key,task_data,period,status,accepted_at,updated_at) "
                "VALUES('u',1,'trial','not-json','2026-07-11','accepted','a','b')"
            )
        task = self.repository.get_active_task("u", "2026-07-11")
        self.assertEqual({}, task["任务内容"])
        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            uow.execute(
                "UPDATE sect_task_state SET status='completed' WHERE user_id='u'"
            )
        self.assertIsNone(self.repository.get_active_task("u", "2026-07-11"))

    def test_missing_schema_fails_without_request_time_ddl(self):
        database = Path(self.temp.name) / "unmigrated.db"
        with sqlite3.connect(database):
            pass
        repository = SectTaskStateSqlRepository(database)
        with self.assertRaisesRegex(RuntimeError, "run migrations first"):
            repository.get_active_task("u", "2026-07-11")
        with sqlite3.connect(database) as connection:
            tables = connection.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        self.assertEqual([], tables)

    def test_missing_claim_receipt_schema_fails_without_request_time_ddl(self):
        database = Path(self.temp.name) / "missing-receipts.db"
        with DatabaseUnitOfWork(database, immediate=True) as uow:
            apply_sect_task_state(uow)
        repository = SectTaskStateSqlRepository(database)
        with self.assertRaisesRegex(RuntimeError, "run migrations first"):
            repository.claim_task(
                "claim-op", "u", 7, "trial", {}, "2026-07-11", 3, "now"
            )
        with sqlite3.connect(database) as connection:
            tables = {
                row[0]
                for row in connection.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                )
            }
        self.assertEqual({"sect_task_state"}, tables)

    def test_claim_and_refresh_are_atomic_and_idempotent(self):
        application = SectApplication(
            self.database, clock=_Clock(), random_source=_Random()
        )
        config = {"first": {"type": 1}, "chosen": {"type": 2, "cost": 5}}
        claimed = application.claim_task("claim-op", "u", 7, config, 3)
        duplicate = application.claim_task("claim-op", "u", 7, config, 3)
        self.assertTrue(claimed.applied)
        self.assertEqual("duplicate", duplicate.status)
        self.assertEqual(("chosen", {"type": 2, "cost": 5}), (claimed.task_key, claimed.task_data))

        current = application.get_active_task("u")
        refreshed = application.refresh_task("refresh-op", "u", 7, current, config, 3)
        replay = application.refresh_task("refresh-op", "u", 7, current, config, 3)
        active = application.get_active_task("u")
        self.assertTrue(refreshed.applied)
        self.assertEqual("duplicate", replay.status)
        self.assertEqual(("chosen", {"type": 2, "cost": 5}), (refreshed.task_key, refreshed.task_data))
        self.assertEqual((refreshed.task_key, refreshed.task_data), (active["任务名称"], active["任务内容"]))

    def test_claim_enforces_membership_limit_and_existing_task(self):
        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            uow.execute(
                "INSERT INTO sect_task_state "
                "(user_id,sect_id,task_key,task_data,period,accepted_at,updated_at) "
                "VALUES('u',7,'existing','{}','2026-07-11','a','a')"
            )
        data = {"type": 1}
        existing = self.repository.claim_task(
            "existing-op", "u", 7, "trial", data, "2026-07-11", 3, "now"
        )
        self.assertEqual("task_exists", existing["status"])
        wrong_sect = self.repository.claim_task(
            "wrong-sect", "u", 8, "trial", data, "2026-07-11", 3, "now"
        )
        self.assertEqual("sect_changed", wrong_sect["status"])
        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            uow.execute("UPDATE user_xiuxian SET sect_task=3 WHERE user_id='u'")
        limited = self.repository.claim_task(
            "limited", "u", 7, "trial", data, "2026-07-11", 3, "now",
            replace_existing=True,
        )
        self.assertEqual("daily_limit", limited["status"])

    def test_refresh_rejects_stale_state_and_rolls_back_receipt_failure(self):
        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            uow.execute(
                "INSERT INTO sect_task_state "
                "(user_id,sect_id,task_key,task_data,period,accepted_at,updated_at) "
                "VALUES('u',7,'old','{\"type\": 1}','2026-07-11','a','a')"
            )
        stale = self.repository.refresh_task(
            "stale", "u", 7, "2026-07-11", "wrong", {}, "new", {}, 3, "now"
        )
        self.assertEqual("state_changed", stale["status"])
        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            uow.execute(
                "CREATE TRIGGER fail_task_claim BEFORE INSERT ON sect_task_claim_operations "
                "BEGIN SELECT RAISE(ABORT,'receipt failed'); END"
            )
        with self.assertRaises(sqlite3.IntegrityError):
            self.repository.refresh_task(
                "fail", "u", 7, "2026-07-11", "old", {"type": 1}, "new", {}, 3, "now"
            )
        with DatabaseUnitOfWork(self.database, read_only=True) as uow:
            row = uow.query_one(
                "SELECT task_key FROM sect_task_state WHERE user_id='u' AND period='2026-07-11'"
            )
            receipt = uow.query_one(
                "SELECT 1 AS present FROM sect_task_claim_operations WHERE operation_id='fail'"
            )
        self.assertEqual("old", row["task_key"])
        self.assertIsNone(receipt)

    def test_migration_is_game_database_only(self):
        migrations = build_migrations()
        game = {item.version for item in migrations_for_database(migrations, "game_db")}
        player = {item.version for item in migrations_for_database(migrations, "player_db")}
        self.assertIn("sect.016", game)
        self.assertNotIn("sect.016", player)
        self.assertIn("sect.017", game)
        self.assertNotIn("sect.017", player)


if __name__ == "__main__":
    unittest.main()
