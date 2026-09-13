from __future__ import annotations

import tempfile
import unittest
from datetime import datetime
from pathlib import Path

from ..application import BegApplication
from ..repository import BegRepository
from ....infrastructure.database import DatabaseUnitOfWork


class BegApplicationTest(unittest.TestCase):
    def test_execute_is_idempotent(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            app = BegApplication(f"{directory}/game.db")
            first = app.execute(operation_id="op-1", user_id="u")
            second = app.execute(operation_id="op-1", user_id="u")
            self.assertEqual(first.operation_id, second.operation_id)
            self.assertTrue(second.replayed)

    @staticmethod
    def _database(directory: str) -> Path:
        database = Path(directory) / "game.db"
        with DatabaseUnitOfWork(database) as uow:
            uow.execute(
                "CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,stone INTEGER,"
                "create_time TEXT,is_beg INTEGER,is_novice INTEGER,sect_id INTEGER,"
                "root_type TEXT,level TEXT)"
            )
            uow.execute(
                "INSERT INTO user_xiuxian VALUES (?,?,?,?,?,?,?,?)",
                ("u", 100, "2026-09-12 08:30:00", 0, 0, None, "天灵根", "练气境初期"),
            )
            uow.execute(
                "CREATE TABLE back(user_id TEXT,goods_id INTEGER,goods_name TEXT,"
                "goods_type TEXT,goods_num INTEGER,create_time TEXT,update_time TEXT,"
                "bind_num INTEGER,UNIQUE(user_id,goods_id))"
            )
        return database

    def test_daily_action_owns_ledger_and_replays_mutable_snapshots(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database = self._database(directory)
            app = BegApplication(database)
            request = {
                "action": "daily_settle",
                "expected_create_time": datetime(2026, 9, 12, 8, 30),
                "expected_stone": 100,
                "expected_sect_id": None,
                "expected_root_type": "天灵根",
                "expected_level": "练气境初期",
                "settled_at": datetime(2026, 9, 13),
                "max_age_days": 7,
                "eligible_levels": ("练气境初期", "练气境中期"),
                "stone_reward": 25,
            }
            first = app.execute(operation_id="daily-op", user_id="u", payload=request)
            replay = app.execute(
                operation_id="daily-op",
                user_id="u",
                payload={**request, "expected_stone": 999, "stone_reward": 1},
            )
            self.assertEqual((first.status, first.data["stone"]), ("applied", 125))
            self.assertTrue(replay.replayed)
            with DatabaseUnitOfWork(database) as uow:
                self.assertEqual(
                    uow.query_one("SELECT status FROM operation_ledger WHERE operation_id = ?", ("daily-op",))["status"],
                    "applied",
                )

    def test_rejection_and_repository_failure_keep_assets_consistent(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database = self._database(directory)
            app = BegApplication(database)
            request = {
                "action": "daily_settle",
                "expected_create_time": "2026-09-12 08:30:00",
                "expected_stone": 99,
                "expected_sect_id": None,
                "expected_root_type": "天灵根",
                "expected_level": "练气境初期",
                "settled_at": "2026-09-13 00:00:00",
                "max_age_days": 7,
                "eligible_levels": ("练气境初期",),
                "stone_reward": 25,
            }
            rejected = app.execute(operation_id="reject-op", user_id="u", payload=request)
            self.assertEqual((rejected.status, rejected.code), ("rejected", "state_changed"))

            def fail(checkpoint: str) -> None:
                if checkpoint == "after_user_update":
                    raise RuntimeError("injected failure")

            failing = BegApplication(database, repository=BegRepository(failure_hook=fail))
            request["expected_stone"] = 100
            with self.assertRaisesRegex(RuntimeError, "injected failure"):
                failing.execute(operation_id="failed-op", user_id="u", payload=request)
            with DatabaseUnitOfWork(database) as uow:
                user = uow.query_one("SELECT stone,is_beg FROM user_xiuxian WHERE user_id = ?", ("u",))
                ledger = uow.query_one(
                    "SELECT status FROM operation_ledger WHERE operation_id = ? AND action = ?",
                    ("failed-op", "beg.daily_settle"),
                )
            self.assertEqual((user["stone"], user["is_beg"], ledger["status"]), (100, 0, "failed"))

    def test_unsupported_action_finishes_its_own_ledger_row(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database = self._database(directory)
            outcome = BegApplication(database).execute(
                operation_id="unsupported-op",
                user_id="u",
                payload={"action": "future_action"},
            )
            self.assertEqual(outcome.data["status"], "unsupported")
            with DatabaseUnitOfWork(database) as uow:
                row = uow.query_one(
                    "SELECT status FROM operation_ledger WHERE operation_id = ? AND action = ?",
                    ("unsupported-op", "beg.future_action"),
                )
            self.assertEqual(row["status"], "applied")


if __name__ == "__main__":
    unittest.main()
