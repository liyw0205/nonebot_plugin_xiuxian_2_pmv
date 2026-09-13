from __future__ import annotations

import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import Mock

from nonebot_plugin_xiuxian_2.compatibility.sign_in_effects import LegacySignInEffects
from nonebot_plugin_xiuxian_2.features.sign_in.application import SignInApplication
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork


class FixedRandom:
    @staticmethod
    def randint(lower: int, upper: int) -> int:
        return lower


class FixedClock:
    def now(self) -> datetime:
        return datetime(2026, 9, 13, tzinfo=timezone.utc)


class LegacySignInEffectsTests(unittest.TestCase):
    def test_new_application_can_route_legacy_side_effects_after_commit(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "sign.db"
            with DatabaseUnitOfWork(database) as uow:
                uow.execute("CREATE TABLE user_xiuxian (user_id TEXT, user_name TEXT, is_sign INTEGER, stone INTEGER)")
                uow.execute("INSERT INTO user_xiuxian VALUES (?, ?, ?, ?)", ("u1", "甲", 0, 0))
            lottery = Mock()
            task_progress = Mock()
            statistics = Mock()
            logger = Mock()
            effects = LegacySignInEffects(
                database,
                lottery_service=lottery,
                clock=FixedClock(),
                task_progress=task_progress,
                statistics=statistics,
                logger=logger,
            )
            app = SignInApplication(database, random_source=FixedRandom(), clock=FixedClock(), effects=effects)

            result = app.claim(user_id="u1", operation_id="sign-1", lower_limit=10, upper_limit=20)

            self.assertTrue(result.ok)
            lottery.settle.assert_called_once()
            statistics.assert_called_once_with("u1", "修仙签到")
            task_progress.assert_called_once_with("u1", "sign_in", operation_id="task-progress:sign-1")
            logger.assert_called_once()

    def test_replay_does_not_repeat_non_idempotent_legacy_side_effects(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "sign.db"
            with DatabaseUnitOfWork(database) as uow:
                uow.execute("CREATE TABLE user_xiuxian (user_id TEXT, user_name TEXT, is_sign INTEGER, stone INTEGER)")
                uow.execute("INSERT INTO user_xiuxian VALUES (?, ?, ?, ?)", ("u1", "甲", 0, 0))
            lottery = Mock()
            statistics = Mock()
            task_progress = Mock()
            effects = LegacySignInEffects(database, lottery_service=lottery, clock=FixedClock(), statistics=statistics, task_progress=task_progress)
            app = SignInApplication(database, random_source=FixedRandom(), clock=FixedClock(), effects=effects)

            app.claim(user_id="u1", operation_id="sign-1", lower_limit=10, upper_limit=20)
            app.claim(user_id="u1", operation_id="sign-1", lower_limit=10, upper_limit=20)

            self.assertEqual(statistics.call_count, 1)
            self.assertEqual(task_progress.call_count, 1)


if __name__ == "__main__":
    unittest.main()
