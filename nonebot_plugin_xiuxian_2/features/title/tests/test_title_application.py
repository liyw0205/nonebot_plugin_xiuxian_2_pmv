from __future__ import annotations

import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from ..application import TitleApplication
from ..migrations import apply_title_schema
from ....infrastructure.database import DatabaseUnitOfWork, OperationLedger


class TitleApplicationTest(unittest.TestCase):
    def test_state_read_uses_migrated_title_projection(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database = f"{directory}/player.db"
            with DatabaseUnitOfWork(database, immediate=True) as uow:
                apply_title_schema(uow)
                uow.execute(
                    "INSERT INTO title(user_id,unlocked,equipped) VALUES(?,?,?)",
                    ("u", json.dumps(["1", "2"]), "2"),
                )

            app = TitleApplication(database)
            self.assertEqual(
                app.get_state("u"),
                {"unlocked": '["1", "2"]', "equipped": "2"},
            )
            self.assertIsNone(app.get_state("missing"))

    def test_state_read_does_not_create_missing_schema(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database = f"{directory}/player.db"
            app = TitleApplication(database)
            with self.assertRaises(sqlite3.OperationalError):
                app.get_state("u")
            self.assertFalse(Path(database).exists())

            with sqlite3.connect(database) as conn:
                tables = {
                    row[0]
                    for row in conn.execute(
                        "SELECT name FROM sqlite_master WHERE type='table'"
                    )
                }
            self.assertEqual(tables, set())

    def test_execute_is_idempotent(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database = f"{directory}/player.db"
            with DatabaseUnitOfWork(database, immediate=True) as uow:
                OperationLedger().ensure_schema(uow)
                apply_title_schema(uow)
            app = TitleApplication(database)
            first = app.execute(operation_id="op-1", user_id="u")
            second = app.execute(operation_id="op-1", user_id="u")
            self.assertEqual(first.operation_id, second.operation_id)
            self.assertTrue(second.replayed)


if __name__ == "__main__":
    unittest.main()
