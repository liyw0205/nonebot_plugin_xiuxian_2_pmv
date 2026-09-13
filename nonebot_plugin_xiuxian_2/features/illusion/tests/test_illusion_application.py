from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from ..application import IllusionApplication
from ....infrastructure.database import DatabaseUnitOfWork


class IllusionApplicationTest(unittest.TestCase):
    def _database(self, directory: str) -> Path:
        database = Path(directory) / "game.db"
        with DatabaseUnitOfWork(database) as uow:
            uow.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY, stone INTEGER, exp INTEGER)")
            uow.execute("INSERT INTO user_xiuxian VALUES ('u', 0, 0)")
            uow.execute("CREATE TABLE back(user_id TEXT, goods_id INTEGER, goods_name TEXT, goods_type TEXT, goods_num INTEGER, create_time TEXT, update_time TEXT, bind_num INTEGER, UNIQUE(user_id, goods_id))")
        return database

    @staticmethod
    def _request(**overrides):
        request = {
            "action": "choose",
            "period": "2026-09-12",
            "question_index": 0,
            "choice_index": 0,
            "selected_option": "option",
            "stone": 1,
            "exp": 2,
            "item": None,
            "max_goods_num": 99,
        }
        request.update(overrides)
        return request

    def test_execute_is_idempotent(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database = self._database(directory)
            app = IllusionApplication(database)
            request = self._request()
            first = app.execute(operation_id="op-1", user_id="u", payload=request)
            second = app.execute(operation_id="op-1", user_id="u", payload=request)
            self.assertEqual(first.operation_id, second.operation_id)
            self.assertTrue(second.replayed)
            self.assertEqual(first.data["status"], "applied")

    def test_rejected_choice_does_not_change_assets(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database = self._database(directory)
            app = IllusionApplication(database)
            app.execute(operation_id="op-1", user_id="u", payload=self._request())
            rejected = app.execute(
                operation_id="op-2",
                user_id="u",
                payload=self._request(choice_index=1, selected_option="other"),
            )
            self.assertEqual((rejected.status, rejected.code), ("rejected", "already_chosen"))
            with DatabaseUnitOfWork(database) as uow:
                row = uow.query_one("SELECT stone, exp FROM user_xiuxian WHERE user_id = 'u'")
            self.assertEqual((row["stone"], row["exp"]), (1, 2))

    def test_operation_conflict_is_not_replayed(self) -> None:
        from ....core.errors import OperationConflictError

        with tempfile.TemporaryDirectory() as directory:
            database = self._database(directory)
            app = IllusionApplication(database)
            app.execute(operation_id="op-1", user_id="u", payload=self._request())
            with self.assertRaises(OperationConflictError):
                app.execute(operation_id="op-1", user_id="u", payload=self._request(stone=9))


if __name__ == "__main__":
    unittest.main()
