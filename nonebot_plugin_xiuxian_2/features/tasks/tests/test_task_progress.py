from __future__ import annotations

import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from ....infrastructure.database import DatabaseUnitOfWork
from ....plugin import build_migrations, migrations_for_database
from ..application import TaskProgressApplication
from ..migrations import apply_task_progress


class TaskProgressApplicationTest(unittest.TestCase):
    def test_progress_schema_migration_runs_only_on_player_database(self) -> None:
        migrations = build_migrations()
        game_versions = {
            item.version for item in migrations_for_database(migrations, "game_db")
        }
        player_versions = {
            item.version for item in migrations_for_database(migrations, "player_db")
        }
        self.assertNotIn("tasks.001", game_versions)
        self.assertIn("tasks.001", player_versions)

    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.database = Path(self.temp.name) / "player.db"
        self.application = TaskProgressApplication(self.database)

    def tearDown(self) -> None:
        self.temp.cleanup()

    def migrate(self) -> None:
        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            apply_task_progress(uow)

    def test_request_requires_startup_schema_migration(self) -> None:
        with self.assertRaises(sqlite3.OperationalError):
            self.application.record("op-1", "u", (("sign_in", 1),), {}, ())

        with sqlite3.connect(self.database) as conn:
            tables = {
                row[0]
                for row in conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                )
            }
        self.assertNotIn("xiuxian_tasks", tables)
        self.assertNotIn("task_progress_event_operations", tables)

    def test_migration_preserves_existing_progress_and_application_replays(self) -> None:
        with sqlite3.connect(self.database) as conn:
            conn.execute(
                "CREATE TABLE xiuxian_tasks(user_id TEXT PRIMARY KEY,daily_progress TEXT)"
            )
            conn.execute(
                "INSERT INTO xiuxian_tasks(user_id,daily_progress) VALUES(?,?)",
                ("u", json.dumps({"existing": 2})),
            )

        self.migrate()
        with sqlite3.connect(self.database) as conn:
            migrated_value = conn.execute(
                "SELECT daily_progress FROM xiuxian_tasks WHERE user_id='u'"
            ).fetchone()[0]
        self.assertEqual(json.loads(migrated_value), {"existing": 2})

        task = {
            "key": "daily_sign",
            "cycle": "daily",
            "name": "今日问道",
            "target": 1,
            "amount": 1,
        }
        periods = {"daily": "2026-09-27"}
        first = self.application.record(
            "op-1", "u", (("sign_in", 1),), periods, (task,)
        )
        replay = self.application.record(
            "op-1", "u", (("sign_in", 1),), {"daily": "2099-01-01"}, (task,)
        )

        self.assertEqual((first.status, replay.status), ("applied", "duplicate"))
        self.assertEqual(first.completed, replay.completed, ("今日问道",))
        states = self.application.get_states("u", periods)
        self.assertEqual(states["daily"][0], {"daily_sign": 1})

        with sqlite3.connect(self.database) as conn:
            preserved = conn.execute(
                "SELECT daily_progress FROM xiuxian_tasks WHERE user_id='u'"
            ).fetchone()[0]
        self.assertEqual(json.loads(preserved), {"daily_sign": 1})


if __name__ == "__main__":
    unittest.main()
