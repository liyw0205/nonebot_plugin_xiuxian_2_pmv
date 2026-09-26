from __future__ import annotations

import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from ....infrastructure.database import DatabaseUnitOfWork
from ....plugin import build_migrations, migrations_for_database
from ..application import TaskProgressApplication
from ..migrations import (
    apply_task_claim,
    apply_task_claim_player,
    apply_task_claim_recovery,
    apply_task_progress,
)


class TaskProgressApplicationTest(unittest.TestCase):
    def test_task_schema_migrations_are_routed_to_their_owning_databases(self) -> None:
        migrations = build_migrations()
        game_versions = {
            item.version for item in migrations_for_database(migrations, "game_db")
        }
        player_versions = {
            item.version for item in migrations_for_database(migrations, "player_db")
        }
        self.assertNotIn("tasks.001", game_versions)
        self.assertIn("tasks.001", player_versions)
        self.assertIn("tasks.002", game_versions)
        self.assertNotIn("tasks.002", player_versions)
        self.assertIn("tasks.003", game_versions)
        self.assertNotIn("tasks.003", player_versions)
        self.assertNotIn("tasks.004", game_versions)
        self.assertIn("tasks.004", player_versions)

    def test_claim_migration_extends_existing_economy_log_without_losing_rows(self) -> None:
        database = Path(self.temp.name) / "game.db"
        with sqlite3.connect(database) as conn:
            conn.execute(
                "CREATE TABLE economy_log("
                "id INTEGER PRIMARY KEY AUTOINCREMENT,user_id TEXT,"
                "source TEXT NOT NULL,action TEXT NOT NULL,"
                "stone_delta INTEGER NOT NULL DEFAULT 0,"
                "item_delta TEXT NOT NULL DEFAULT '[]',detail TEXT NOT NULL DEFAULT '{}',"
                "trace_id TEXT,created_at TEXT NOT NULL)"
            )
            conn.execute(
                "INSERT INTO economy_log(user_id,source,action,created_at) "
                "VALUES('u','legacy','keep','2026-09-27')"
            )

        with DatabaseUnitOfWork(database, immediate=True) as uow:
            apply_task_claim(uow)

        with sqlite3.connect(database) as conn:
            columns = {
                row[1] for row in conn.execute("PRAGMA table_info(economy_log)")
            }
            row = conn.execute(
                "SELECT user_id,source,action FROM economy_log WHERE id=1"
            ).fetchone()
            operation_table = conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' "
                "AND name='task_reward_claim_operations'"
            ).fetchone()
        self.assertIn("exp_delta", columns)
        self.assertIn("sect_contribution_delta", columns)
        self.assertEqual(row, ("u", "legacy", "keep"))
        self.assertIsNotNone(operation_table)

    def test_recovery_migrations_preserve_legacy_rows_and_are_idempotent(self) -> None:
        game_database = Path(self.temp.name) / "legacy-game.db"
        with sqlite3.connect(game_database) as conn:
            conn.execute(
                "CREATE TABLE task_reward_claim_operations("
                "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,result_json TEXT NOT NULL,"
                "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
            )
            conn.execute(
                "INSERT INTO task_reward_claim_operations(operation_id,payload,result_json) "
                "VALUES('legacy','[\"u\",[\"daily\"]]','[]')"
            )

        with DatabaseUnitOfWork(game_database, immediate=True) as uow:
            apply_task_claim_recovery(uow)
            apply_task_claim_recovery(uow)
        with DatabaseUnitOfWork(Path(self.temp.name) / "legacy-player.db", immediate=True) as uow:
            apply_task_claim_player(uow)
            apply_task_claim_player(uow)

        with sqlite3.connect(game_database) as conn:
            row = conn.execute(
                "SELECT payload,result_json,status,result_status,request_json "
                "FROM task_reward_claim_operations WHERE operation_id='legacy'"
            ).fetchone()
        self.assertEqual(row, ('["u",["daily"]]', "[]", "applied", "applied", "{}"))

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
