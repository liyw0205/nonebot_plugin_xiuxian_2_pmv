from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from ..application import EntertainmentApplication
from ....infrastructure.database import DatabaseUnitOfWork
from ....plugin import apply_platform_schema


class EntertainmentApplicationTest(unittest.TestCase):
    def test_execute_is_idempotent(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "game.db"
            with DatabaseUnitOfWork(database) as uow:
                apply_platform_schema(uow)
            app = EntertainmentApplication(database)
            first = app.execute(operation_id="op-1", user_id="u")
            second = app.execute(operation_id="op-1", user_id="u")
            self.assertEqual(first.operation_id, second.operation_id)
            self.assertTrue(second.replayed)


if __name__ == "__main__":
    unittest.main()
