from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from nonebot_plugin_xiuxian_2.features.activity_reward.application import ActivityRewardApplication
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork


class _Repository:
    def __init__(self, ok=True):
        self.ok = ok
        self.calls = 0

    def claim_all(self, operation_id, user_id):
        self.calls += 1
        return self.ok, "奖励已领取" if self.ok else "暂无可领取奖励"


class ActivityRewardApplicationTests(unittest.TestCase):
    def test_success_and_replay(self):
        with tempfile.TemporaryDirectory() as directory:
            repo = _Repository()
            app = ActivityRewardApplication(Path(directory) / "game.db", repository=repo)
            first = app.claim_all(operation_id="activity-1", user_id="u-1")
            replay = app.claim_all(operation_id="activity-1", user_id="u-1")
            self.assertTrue(first.ok)
            self.assertTrue(replay.replayed)
            self.assertEqual(repo.calls, 1)
            with DatabaseUnitOfWork(Path(directory) / "game.db") as uow:
                row = uow.query_one("SELECT status FROM operation_ledger WHERE operation_id=?", ("activity-1",))
            self.assertEqual(row["status"], "applied")

    def test_rejection_is_idempotent(self):
        with tempfile.TemporaryDirectory() as directory:
            repo = _Repository(False)
            app = ActivityRewardApplication(Path(directory) / "game.db", repository=repo)
            result = app.claim_all(operation_id="activity-2", user_id="u-1")
            replay = app.claim_all(operation_id="activity-2", user_id="u-1")
            self.assertFalse(result.ok)
            self.assertEqual(result.code, "not_claimable")
            self.assertEqual(replay.code, "not_claimable")
            self.assertEqual(repo.calls, 1)


if __name__ == "__main__":
    unittest.main()
