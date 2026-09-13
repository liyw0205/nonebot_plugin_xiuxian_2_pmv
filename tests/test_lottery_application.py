from __future__ import annotations

import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from nonebot_plugin_xiuxian_2.features.sign_in.lottery_application import LotteryApplication
from nonebot_plugin_xiuxian_2.features.sign_in.lottery_repository import LotteryRepository
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork


class Clock:
    def now(self) -> datetime:
        return datetime(2026, 9, 13, tzinfo=timezone.utc)


class Random:
    def randint(self, lower: int, upper: int) -> int:
        return 1666


class LotteryApplicationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.database = Path(self.temp.name) / "game.db"
        with DatabaseUnitOfWork(self.database) as uow:
            uow.execute("CREATE TABLE user_xiuxian (user_id TEXT PRIMARY KEY, user_name TEXT, stone INTEGER)")
            uow.execute("INSERT INTO user_xiuxian VALUES (?, ?, ?)", ("u1", "甲", 100))
            LotteryRepository.ensure_schema(uow)
        self.app = LotteryApplication(str(self.database), clock=Clock(), random_source=Random())

    def tearDown(self) -> None:
        self.temp.cleanup()

    def test_settle_is_atomic_and_idempotent(self) -> None:
        first = self.app.settle(operation_id="lottery-1", user_id="u1", user_name="甲", business_date="2026-09-13", deposit=1000)
        replay = self.app.settle(operation_id="lottery-1", user_id="u1", user_name="甲", business_date="2026-09-13", deposit=1000)
        conflict = self.app.settle(operation_id="lottery-1", user_id="u1", user_name="甲", business_date="2026-09-13", deposit=2000)

        self.assertEqual(first.status, "settled")
        self.assertEqual(replay.status, "duplicate")
        self.assertEqual(conflict.status, "operation_conflict")
        self.assertEqual(first.prize_tier, "first")
        with DatabaseUnitOfWork(self.database) as uow:
            row = uow.query_one("SELECT stone FROM user_xiuxian WHERE user_id=?", ("u1",))
            self.assertIsNotNone(row)
            self.assertEqual(int(row["stone"]), 200)  # type: ignore[index]

    def test_same_day_second_user_attempt_is_rejected_without_wallet_change(self) -> None:
        first = self.app.settle(operation_id="lottery-1", user_id="u1", user_name="甲", business_date="2026-09-13", deposit=1000)
        second = self.app.settle(operation_id="lottery-2", user_id="u1", user_name="甲", business_date="2026-09-13", deposit=1000)
        self.assertEqual(first.status, "settled")
        self.assertEqual(second.status, "already_participated")

    def test_missing_lottery_schema_is_not_silently_initialized(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            app = LotteryApplication(str(Path(directory) / "empty.db"), clock=Clock(), random_source=Random())
            with self.assertRaisesRegex(RuntimeError, "schema migration"):
                app.settle(operation_id="lottery-1", user_id="u1", user_name="甲", business_date="2026-09-13")


if __name__ == "__main__":
    unittest.main()
