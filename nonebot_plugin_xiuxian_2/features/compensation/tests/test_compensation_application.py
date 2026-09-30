from __future__ import annotations

import tempfile
import unittest

from ..application import CompensationApplication
from ....infrastructure.database import DatabaseUnitOfWork, OperationLedger


class CompensationApplicationTest(unittest.TestCase):
    def test_execute_is_idempotent(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database = f"{directory}/game.db"
            with DatabaseUnitOfWork(database) as uow:
                OperationLedger().ensure_schema(uow)
            app = CompensationApplication(database)
            first = app.execute(operation_id="op-1", user_id="u")
            second = app.execute(operation_id="op-1", user_id="u")
            self.assertEqual(first.operation_id, second.operation_id)
            self.assertTrue(second.replayed)


if __name__ == "__main__":
    unittest.main()
