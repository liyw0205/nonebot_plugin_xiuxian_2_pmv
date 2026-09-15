import sqlite3
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from nonebot_plugin_xiuxian_2.features.sign_in.tasks import SignInTaskRepository
from nonebot_plugin_xiuxian_2.features.sign_in.migrations import apply_sign_in_tasks
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork


class SignInTaskRepositoryTests(unittest.TestCase):
    def test_daily_and_weekly_completion_are_idempotent(self):
        with tempfile.TemporaryDirectory() as d:
            db = Path(d) / "game.db"
            with DatabaseUnitOfWork(db) as uow:
                apply_sign_in_tasks(uow)
            repo = SignInTaskRepository(db)
            now = datetime(2026, 9, 14, tzinfo=timezone.utc)
            self.assertEqual(repo.record(user_id="u1", operation_id="s1", occurred_at=now), ["今日问道"])
            self.assertEqual(repo.record(user_id="u1", operation_id="s1", occurred_at=now), [])
            result = []
            for i in range(2, 7):
                result = repo.record(user_id="u1", operation_id=f"s{i}", occurred_at=now)
            self.assertEqual(result, ["七日勤修"])
            with sqlite3.connect(db) as c:
                self.assertEqual(c.execute("SELECT daily_progress,weekly_progress FROM sign_in_task_projection").fetchone(), (1, 6))

    def test_invalid_amount_is_rejected_by_effects(self):
        from nonebot_plugin_xiuxian_2.features.sign_in.task_effects import ApplicationSignInTaskEffects

        effects = ApplicationSignInTaskEffects.__new__(ApplicationSignInTaskEffects)
        with self.assertRaises(ValueError):
            effects.record(user_id="u1", operation_id="s1", amount=2)


if __name__ == "__main__":
    unittest.main()
