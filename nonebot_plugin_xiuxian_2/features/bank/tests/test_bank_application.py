from __future__ import annotations

import tempfile
import unittest

from ....infrastructure.database import DatabaseUnitOfWork
from ....plugin import apply_platform_schema
from ..application import BankApplication


class _Repository:
    def deposit(self, *args):
        return {"status": "applied", "deposited": args[2]}


class BankApplicationTest(unittest.TestCase):
    def test_replays_same_operation(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            with DatabaseUnitOfWork(f"{directory}/game.db") as uow:
                apply_platform_schema(uow)
            app = BankApplication(f"{directory}/game.db", f"{directory}/player.db", repository=_Repository())
            request = {
                "operation_id": "op-1",
                "user_id": "u",
                "amount": 1,
                "expected_saved_stone": 0,
                "expected_saved_at": "",
                "bank_level": "初级",
                "interest": 0,
                "settled_at": "2026-01-01T00:00:00+00:00",
                "save_limit": 100,
            }
            first = app.deposit(**request)
            second = app.deposit(**request)
            self.assertEqual(first.operation_id, second.operation_id)
            self.assertTrue(second.replayed)


if __name__ == "__main__":
    unittest.main()
