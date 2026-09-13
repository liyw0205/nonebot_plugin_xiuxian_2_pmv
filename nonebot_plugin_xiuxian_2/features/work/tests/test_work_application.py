from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from ..application import WorkClaimApplication, WorkSettlementApplication


class _Repository:
    def __init__(self, status="applied"):
        self.status = status
        self.calls = 0

    def claim(self, operation_id, user_id, expected_count, expected_offer, task_index, started_at):
        self.calls += 1
        return {"status": self.status, "task_name": "采药", "started_at": started_at, "remaining_count": expected_count}

    def settle(self, *args, **kwargs):
        self.calls += 1
        return {
            "status": self.status,
            "exp": 120,
            "item_awarded": True,
            "success_kind": "ok",
            "item_msg": "一品:灵草",
            "scheduled_time": "采药",
        }


class WorkClaimApplicationTests(unittest.TestCase):
    def test_claim_replays_without_repeating_repository(self):
        with tempfile.TemporaryDirectory() as directory:
            repository = _Repository()
            app = WorkClaimApplication(Path(directory) / "game.db", repository=repository)
            kwargs = {
                "operation_id": "work-1", "user_id": "u", "expected_count": 3,
                "expected_offer": {"tasks": {"采药": {"time": 5}}}, "task_index": 1,
                "started_at": "2026-09-12 10:00:00",
            }
            first = app.claim(**kwargs)
            second = app.claim(**kwargs)
            self.assertTrue(first.ok)
            self.assertTrue(second.replayed)
            self.assertEqual(repository.calls, 1)

    def test_settlement_replays_without_repeating_repository(self):
        with tempfile.TemporaryDirectory() as directory:
            repository = _Repository()
            app = WorkSettlementApplication(Path(directory) / "game.db", repository=repository)
            kwargs = {
                "operation_id": "work-settle-1", "user_id": "u",
                "expected_work": {"create_time": "2026-09-12 10:00:00", "scheduled_time": "采药"},
                "exp_gain": 120, "item": {"id": 1, "name": "灵草", "type": "药材"},
                "max_exp": 999, "max_goods_num": 99, "success_kind": "ok", "item_msg": "一品:灵草",
            }
            first = app.settle(**kwargs)
            second = app.settle(**kwargs)
            self.assertTrue(first.ok)
            self.assertTrue(second.replayed)
            self.assertEqual(first.data["exp"], 120)
            self.assertEqual(repository.calls, 1)

    def test_state_rejection_is_idempotent(self):
        with tempfile.TemporaryDirectory() as directory:
            repository = _Repository("state_changed")
            app = WorkClaimApplication(Path(directory) / "game.db", repository=repository)
            kwargs = {
                "operation_id": "work-2", "user_id": "u", "expected_count": 0,
                "expected_offer": {"tasks": {}}, "task_index": 1, "started_at": "2026-09-12 10:00:00",
            }
            first = app.claim(**kwargs)
            second = app.claim(**kwargs)
            self.assertFalse(first.ok)
            self.assertEqual(second.code, "state_changed")
            self.assertEqual(repository.calls, 1)


if __name__ == "__main__":
    unittest.main()
