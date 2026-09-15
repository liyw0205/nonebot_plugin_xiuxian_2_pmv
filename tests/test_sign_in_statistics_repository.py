from __future__ import annotations

import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from nonebot_plugin_xiuxian_2.features.sign_in.statistics import SignInStatisticsRepository
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork


class SignInStatisticsRepositoryTests(unittest.TestCase):
    def test_record_is_idempotent_and_projects_count(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            repository = SignInStatisticsRepository(str(Path(directory) / "game.db"))
            with DatabaseUnitOfWork(repository.database) as uow:
                repository.ensure_schema(uow)
            occurred_at = datetime(2026, 9, 13, tzinfo=timezone.utc)
            self.assertTrue(repository.record(user_id="u1", operation_id="statistics:1", event_key="修仙签到", occurred_at=occurred_at))
            self.assertFalse(repository.record(user_id="u1", operation_id="statistics:1", event_key="修仙签到", occurred_at=occurred_at))
            self.assertTrue(repository.record(user_id="u1", operation_id="statistics:2", event_key="修仙签到", occurred_at=occurred_at))
            self.assertEqual(repository.value(user_id="u1", event_key="修仙签到"), 2)

    def test_missing_schema_is_not_created_at_request_time(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            repository = SignInStatisticsRepository(str(Path(directory) / "empty.db"))
            occurred_at = datetime(2026, 9, 13, tzinfo=timezone.utc)
            with self.assertRaises(Exception):
                repository.record(user_id="u1", operation_id="statistics:1", event_key="修仙签到", occurred_at=occurred_at)


if __name__ == "__main__":
    unittest.main()
