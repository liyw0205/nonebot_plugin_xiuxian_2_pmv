import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from nonebot_plugin_xiuxian_2.features.sign_in.application import SignInApplication
from nonebot_plugin_xiuxian_2.features.sign_in.application_effects import SignInApplicationEffects
from nonebot_plugin_xiuxian_2.features.sign_in.lottery_application import LotteryApplication
from nonebot_plugin_xiuxian_2.features.sign_in.lottery_repository import LotteryRepository
from nonebot_plugin_xiuxian_2.features.sign_in.repository import SignInRepository
from nonebot_plugin_xiuxian_2.features.sign_in.statistics import SignInStatisticsRepository
from nonebot_plugin_xiuxian_2.features.sign_in.task_effects import ApplicationSignInTaskEffects
from nonebot_plugin_xiuxian_2.features.sign_in.tasks import SignInTaskRepository
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork


class FixedClock:
    def now(self):
        return datetime(2026, 9, 14, tzinfo=timezone.utc)


class FixedRandom:
    def randint(self, lower, upper):
        return 1666


class SignInVerticalSliceTests(unittest.TestCase):
    def test_claim_replay_is_idempotent_across_all_effects(self):
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "game.db"
            with DatabaseUnitOfWork(database) as uow:
                uow.execute("CREATE TABLE user_xiuxian (user_id TEXT PRIMARY KEY, user_name TEXT, stone INTEGER, is_sign INTEGER)")
                uow.execute("INSERT INTO user_xiuxian VALUES (?, ?, ?, ?)", ("u1", "道友", 100, 0))
                LotteryRepository.ensure_schema(uow)
                SignInRepository().ensure_schema(uow)
                SignInStatisticsRepository.ensure_schema(uow)
                SignInTaskRepository.ensure_schema(uow)

            clock = FixedClock()
            effects = SignInApplicationEffects(
                database,
                lottery=LotteryApplication(str(database), clock=clock, random_source=FixedRandom()),
                clock=clock,
                statistics=SignInStatisticsRepository(str(database)),
                tasks=ApplicationSignInTaskEffects(SignInTaskRepository(database), clock),
            )
            application = SignInApplication(
                database,
                clock=clock,
                random_source=FixedRandom(),
                lower_limit=100,
                upper_limit=100,
                effects=effects,
            )

            first = application.claim(user_id="u1", operation_id="sign-1")
            replay = application.claim(user_id="u1", operation_id="sign-1")

            self.assertEqual(first.status, "applied")
            self.assertTrue(replay.replayed)
            with DatabaseUnitOfWork(database) as uow:
                user = uow.query_one("SELECT stone,is_sign FROM user_xiuxian WHERE user_id=?", ("u1",))
                lottery = uow.query_one("SELECT COUNT(*) AS count, MAX(prize_amount) AS prize FROM lottery_settlement_operations", ())
                task_events = uow.query_one("SELECT COUNT(*) AS count FROM sign_in_task_events", ())
            self.assertIsNotNone(user)
            self.assertIsNotNone(lottery)
            self.assertIsNotNone(task_events)
            assert user is not None and lottery is not None and task_events is not None
            self.assertEqual((int(user["stone"]), int(user["is_sign"])), (101766, 1))
            self.assertEqual(int(lottery["count"]), 1)
            self.assertEqual(int(lottery["prize"]), 100000)
            self.assertEqual(int(task_events["count"]), 1)
            self.assertEqual(SignInStatisticsRepository(str(database)).value(user_id="u1", event_key="修仙签到"), 1)


if __name__ == "__main__":
    unittest.main()
