import tempfile
import unittest
from pathlib import Path
from ..repository import DungeonSessionSqlRepository
from tests.test_db_backend import db_backend

class DungeonExploreSettlementApplicationTests(unittest.TestCase):
    def test_repository_settle_does_not_import_legacy_dungeon_service(self):
        with tempfile.TemporaryDirectory() as temp:
            game, player = Path(temp)/'game.db', Path(temp)/'player.db'
            repo = DungeonSessionSqlRepository(game, player)
            self.assertEqual(repo.__class__.__module__, 'nonebot_plugin_xiuxian_2.features.dungeon.repository')
            self.assertFalse('transaction_service' in repo.settle.__qualname__)
