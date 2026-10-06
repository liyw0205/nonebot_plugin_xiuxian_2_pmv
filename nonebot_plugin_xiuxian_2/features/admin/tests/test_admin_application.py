from __future__ import annotations

import tempfile
import unittest
from unittest.mock import Mock

from ..application import AdminApplication
from ..repository import AdminRepository
from ....infrastructure.database import DatabaseUnitOfWork, OperationLedger


class AdminApplicationTest(unittest.TestCase):
    def test_execute_is_idempotent(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database = f"{directory}/game.db"
            with DatabaseUnitOfWork(database, immediate=True) as uow:
                OperationLedger().ensure_schema(uow)
            repository = Mock(spec=AdminRepository)
            repository.execute.return_value = {"status": "applied"}
            app = AdminApplication(database, repository=repository)
            first = app.execute(operation_id="op-1", user_id="u")
            second = app.execute(operation_id="op-1", user_id="u")
            self.assertTrue(first.ok)
            self.assertEqual(first.operation_id, second.operation_id)
            self.assertTrue(second.replayed)
            repository.execute.assert_called_once_with("op-1", "u", "execute", {"user_id": "u"})


if __name__ == "__main__":
    unittest.main()
