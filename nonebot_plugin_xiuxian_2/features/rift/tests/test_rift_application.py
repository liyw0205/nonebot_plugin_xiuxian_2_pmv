import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

from ....infrastructure.database import DatabaseUnitOfWork
from ....plugin import apply_platform_schema
from ..application import RiftApplication


class Repo:
    def invoke(self, action, *args, **kwargs): return {"status": "applied", "action": action}

    def replay(self, *args, **kwargs):
        return None

    def settle(self, *args, **kwargs):
        return SimpleNamespace(status="applied", explore_count=4, message="fixed")


class RiftApplicationTest(unittest.TestCase):
    def test_enter_is_idempotent(self):
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "game.db"
            with DatabaseUnitOfWork(database) as uow:
                apply_platform_schema(uow)
            app = RiftApplication(database, Path(directory) / "player.db", repository=Repo())
            self.assertTrue(app.enter(operation_id="rift-1", user_id="u").ok)
            self.assertTrue(app.enter(operation_id="rift-1", user_id="u").replayed)

    def test_demon_token_action_uses_feature_repository_without_operation_ledger(self):
        with tempfile.TemporaryDirectory() as directory:
            repository = Repo()
            app = RiftApplication(
                Path(directory) / "game.db",
                Path(directory) / "player.db",
                repository=repository,
                demon_token_repository=repository,
            )
            result = app.settle_demon_token_battle(
                operation_id="boss-1",
                user_id="u",
                item_id=20018,
                expected_rift={"name": "boss"},
                expected_user={"stone": 1},
                expected_explore_count=3,
                outcome={"message": "fixed"},
                max_goods_num=1000,
            )
            self.assertEqual((result.status, result.explore_count, result.message), ("applied", 4, "fixed"))
            self.assertIsNone(app.replay_demon_token_battle(operation_id="boss-2"))


if __name__ == "__main__": unittest.main()
