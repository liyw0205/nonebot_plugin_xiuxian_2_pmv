from __future__ import annotations

import tempfile
import unittest
from datetime import datetime
from pathlib import Path

from ....core.errors import OperationConflictError
from ..application import TiantiSettlementApplication


class _Repository:
    def __init__(self, status="settled"):
        self.status = status
        self.calls = 0

    def settle(self, operation_id, user_id, settled_at, *, sect_fairyland_level=0):
        self.calls += 1
        return {"status": self.status, "detail": {"real_gain": 10, "new_hp": 20}}


class _FlakyRepository(_Repository):
    def __init__(self):
        super().__init__()
        self.failed = True

    def settle(self, operation_id, user_id, settled_at, *, sect_fairyland_level=0):
        self.calls += 1
        if self.failed:
            self.failed = False
            raise RuntimeError("temporary player database failure")
        return {"status": "settled", "detail": {"real_gain": 5, "new_hp": 15}}


class TiantiSettlementApplicationTests(unittest.TestCase):
    def test_replay_is_idempotent_and_audited(self):
        with tempfile.TemporaryDirectory() as directory:
            repository = _Repository()
            app = TiantiSettlementApplication(Path(directory) / "player.db", repository=repository)
            kwargs = {
                "operation_id": "tianti-1", "user_id": "u",
                "settled_at": datetime(2026, 9, 12, 10, 0), "sect_fairyland_level": 1,
            }
            first = app.settle(**kwargs)
            second = app.settle(**kwargs)
            self.assertTrue(first.ok)
            self.assertTrue(second.replayed)
            self.assertEqual(repository.calls, 1)

    def test_rejection_is_stable(self):
        with tempfile.TemporaryDirectory() as directory:
            repository = _Repository("state_changed")
            app = TiantiSettlementApplication(Path(directory) / "player.db", repository=repository)
            kwargs = {"operation_id": "tianti-2", "user_id": "u", "settled_at": datetime(2026, 9, 12, 10, 0)}
            first = app.settle(**kwargs)
            second = app.settle(**kwargs)
            self.assertFalse(first.ok)
            self.assertEqual(first.code, "state_changed")
            self.assertEqual(second.code, "state_changed")

    def test_operation_payload_conflict_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            app = TiantiSettlementApplication(Path(directory) / "player.db", repository=_Repository())
            app.settle(operation_id="tianti-3", user_id="u", settled_at=datetime(2026, 9, 12, 10, 0))
            with self.assertRaises(OperationConflictError):
                app.settle(operation_id="tianti-3", user_id="other", settled_at=datetime(2026, 9, 12, 10, 0))

    def test_repository_failure_is_recorded_and_retryable(self):
        with tempfile.TemporaryDirectory() as directory:
            repository = _FlakyRepository()
            app = TiantiSettlementApplication(Path(directory) / "player.db", repository=repository)
            kwargs = {"operation_id": "tianti-4", "user_id": "u", "settled_at": datetime(2026, 9, 12, 10, 0)}
            with self.assertRaisesRegex(RuntimeError, "temporary player database failure"):
                app.settle(**kwargs)
            retry = app.settle(**kwargs)
            self.assertTrue(retry.ok)
            self.assertEqual(repository.calls, 2)


if __name__ == "__main__":
    unittest.main()
