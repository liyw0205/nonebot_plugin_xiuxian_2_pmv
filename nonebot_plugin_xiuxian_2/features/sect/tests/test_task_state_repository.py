from __future__ import annotations

import sqlite3
import tempfile
import unittest
from datetime import datetime
from pathlib import Path

from ....infrastructure.database import DatabaseUnitOfWork
from ....plugin import build_migrations, migrations_for_database
from ..application import SectApplication
from ..migrations import apply_sect_task_state
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
            apply_sect_task_state(uow)
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
        self.assertIsNotNone(index)

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

    def test_migration_is_game_database_only(self):
        migrations = build_migrations()
        game = {item.version for item in migrations_for_database(migrations, "game_db")}
        player = {item.version for item in migrations_for_database(migrations, "player_db")}
        self.assertIn("sect.016", game)
        self.assertNotIn("sect.016", player)


if __name__ == "__main__":
    unittest.main()
