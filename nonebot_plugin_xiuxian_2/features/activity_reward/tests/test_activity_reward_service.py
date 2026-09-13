from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from ..application import ActivityRewardApplication


class _FakeRepository:
    def claim_all(self, operation_id: str, user_id: str):
        return True, "ok"


class ActivityRewardServiceTests(unittest.TestCase):
    def test_application_returns_structured_result(self):
        with tempfile.TemporaryDirectory() as directory:
            result = ActivityRewardApplication(
                Path(directory) / "game.db", repository=_FakeRepository()
            ).claim_all(operation_id="activity-feature-1", user_id="u")
        self.assertTrue(result.ok)
        self.assertEqual(result.data["user_id"], "u")


if __name__ == "__main__":
    unittest.main()
