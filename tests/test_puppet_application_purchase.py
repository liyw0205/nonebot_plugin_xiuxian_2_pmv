import tempfile
import unittest
from pathlib import Path

from nonebot_plugin_xiuxian_2.features.puppet.application import PuppetApplication
from nonebot_plugin_xiuxian_2.features.puppet.purchase_repository import PuppetPurchaseSqlRepository
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork
from nonebot_plugin_xiuxian_2.plugin import apply_platform_schema
from tests.test_db_backend import db_backend


def make_databases(root: Path):
    game = root / "game.db"
    player = root / "player.db"
    with db_backend.transaction(game) as conn:
        conn.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY, stone INTEGER, blessed_spot_flag INTEGER)")
        conn.execute("INSERT INTO user_xiuxian VALUES('u',100,1)")
    with DatabaseUnitOfWork(game) as uow:
        apply_platform_schema(uow)
    with db_backend.transaction(player) as conn:
        conn.execute('CREATE TABLE mix_elixir_info(user_id TEXT PRIMARY KEY, "灵田傀儡" TEXT)')
        conn.execute("INSERT INTO mix_elixir_info VALUES('u','0')")
    return game, player


class PuppetApplicationPurchaseTests(unittest.TestCase):
    def test_default_purchase_uses_feature_repository(self):
        with tempfile.TemporaryDirectory() as temp:
            game, player = make_databases(Path(temp))
            app = PuppetApplication(game, player)
            outcome = app.purchase(operation_id="puppet-default", user_id="u", stone_cost=50)
            replay = app.purchase(operation_id="puppet-default", user_id="u", stone_cost=50)
            self.assertEqual((outcome.status, replay.status), ("applied", "replayed"))


class PuppetPurchaseRepositoryTests(unittest.TestCase):
    def test_repository_handles_success_and_duplicate(self):
        with tempfile.TemporaryDirectory() as temp:
            game, player = make_databases(Path(temp))
            repository = PuppetPurchaseSqlRepository(game, player)
            first = repository.purchase("p1", "u", 50)
            duplicate = repository.purchase("p1", "u", 50)
            self.assertEqual((first.status, duplicate.status), ("purchased", "duplicate"))

    def test_application_upgrade_uses_feature_repository(self):
        with tempfile.TemporaryDirectory() as temp:
            game, player = make_databases(Path(temp))
            app = PuppetApplication(game, player)
            app.purchase(operation_id="p0", user_id="u", stone_cost=10)
            outcome = app.upgrade(operation_id="p1", user_id="u", upgrade_costs={1: 20, 2: 30}, max_level=3)
            replay = app.upgrade(operation_id="p1", user_id="u", upgrade_costs={1: 20, 2: 30}, max_level=3)
            self.assertEqual((outcome.status, replay.status), ("applied", "replayed"))
