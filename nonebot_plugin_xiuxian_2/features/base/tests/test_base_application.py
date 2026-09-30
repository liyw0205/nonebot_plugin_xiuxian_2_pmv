import tempfile
import unittest
from pathlib import Path

from ..application import BaseApplication
from ....infrastructure.database import DatabaseUnitOfWork, OperationLedger


class Repo:
    def invoke(self, action, *args, **kwargs): return {"status": "rejected", "message": "state changed"}


class BaseApplicationTest(unittest.TestCase):
    def test_rejection_is_non_success(self):
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "game.db"
            with DatabaseUnitOfWork(database, immediate=True) as uow:
                OperationLedger().ensure_schema(uow)
            app = BaseApplication(database, Path(directory) / "player.db", repository=Repo())
            result = app.breakthrough(operation_id="base-1", user_id="u")
            self.assertFalse(result.ok)
            self.assertEqual(result.code, "rejected")

    def test_stone_theft_uses_feature_repository_by_default(self):
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "game.db"
            with DatabaseUnitOfWork(database, immediate=True) as uow:
                uow.execute(
                    "CREATE TABLE user_xiuxian("
                    "user_id TEXT PRIMARY KEY,stone INTEGER NOT NULL,user_stamina INTEGER NOT NULL)"
                )
                uow.execute("INSERT INTO user_xiuxian VALUES('thief',20,10)")
                uow.execute("INSERT INTO user_xiuxian VALUES('victim',10,10)")
                from ..migrations import apply_base_stone_contest_operations

                apply_base_stone_contest_operations(uow)
            app = BaseApplication(database, Path(directory) / "player.db")
            result = app.settle_stone_theft(
                operation_id="app-theft", thief_id="thief", victim_id="victim",
                outcome="success", requested_amount=5, penalty_amount=1,
            )
            self.assertEqual(result.status, "settled")
            replay = app.get_stone_theft_result("app-theft", "thief", "victim")
            self.assertEqual(replay.status, "duplicate")


if __name__ == "__main__": unittest.main()
