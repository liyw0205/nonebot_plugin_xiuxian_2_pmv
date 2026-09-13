from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from ....core.errors import OperationConflictError
from ....infrastructure.database import DatabaseUnitOfWork
from ..application import DailyFortuneApplication


class DailyFortuneServiceTests(unittest.TestCase):
    def test_claim_is_idempotent(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            app = DailyFortuneApplication(str(Path(directory) / "game.db"))
            first = app.claim(user_id="u", operation_id="op", date="2026-09-12")
            replay = app.claim(user_id="u", operation_id="op", date="2026-09-12")
            self.assertEqual(first.status, "applied")
            self.assertEqual(replay.status, "replayed")

    def test_same_day_and_rejected_replay_do_not_mutate_twice(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            app = DailyFortuneApplication(str(Path(directory) / "game.db"))
            first = app.claim(user_id="u", operation_id="first", date="2026-09-12")
            rejected = app.claim(user_id="u", operation_id="second", date="2026-09-12")
            replay = app.claim(user_id="u", operation_id="second", date="2026-09-12")
            self.assertEqual(first.status, "applied")
            self.assertEqual((rejected.status, replay.status), ("rejected", "rejected"))
            with DatabaseUnitOfWork(Path(directory) / "game.db") as uow:
                self.assertEqual(uow.query_one("SELECT COUNT(*) AS count FROM daily_fortune_claims")["count"], 1)

    def test_operation_payload_conflict_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            app = DailyFortuneApplication(str(Path(directory) / "game.db"))
            app.claim(user_id="u", operation_id="same", date="2026-09-12")
            with self.assertRaises(OperationConflictError):
                app.claim(user_id="other", operation_id="same", date="2026-09-12")

    def test_database_failure_rolls_back_and_is_retryable(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "game.db"
            app = DailyFortuneApplication(str(database))
            with DatabaseUnitOfWork(database) as uow:
                uow.execute("CREATE TABLE daily_fortune_claims (user_id TEXT, fortune_date TEXT, score INTEGER, title TEXT, message TEXT, operation_id TEXT, created_at TEXT, PRIMARY KEY(user_id, fortune_date))")
                uow.execute("CREATE TRIGGER fail_fortune BEFORE INSERT ON daily_fortune_claims BEGIN SELECT RAISE(ABORT, 'blocked'); END")
            with self.assertRaisesRegex(Exception, "blocked"):
                app.claim(user_id="u", operation_id="retry", date="2026-09-12")
            with DatabaseUnitOfWork(database) as uow:
                self.assertEqual(uow.query_one("SELECT COUNT(*) AS count FROM daily_fortune_claims")["count"], 0)
                self.assertEqual(uow.query_one("SELECT status FROM operation_ledger WHERE operation_id='retry'")["status"], "failed")
                uow.execute("DROP TRIGGER fail_fortune")
            result = app.claim(user_id="u", operation_id="retry", date="2026-09-12")
            self.assertEqual(result.status, "applied")


if __name__ == "__main__":
    unittest.main()
