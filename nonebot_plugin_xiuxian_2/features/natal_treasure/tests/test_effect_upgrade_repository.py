import tempfile
import unittest
from pathlib import Path

from ..effect_upgrade_repository import NatalEffectUpgradeSqlRepository
from ....infrastructure.database import DatabaseUnitOfWork


class NatalEffectUpgradeSqlRepositoryTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        root = Path(self.temp.name)
        self.game = root / "game.db"
        self.player = root / "player.db"
        with DatabaseUnitOfWork(self.game) as uow:
            uow.execute("CREATE TABLE back(user_id TEXT, goods_id INTEGER, goods_num INTEGER, PRIMARY KEY(user_id,goods_id))")
            uow.execute("INSERT INTO back VALUES('u',20009,2)")
        with DatabaseUnitOfWork(self.player) as uow:
            uow.execute("CREATE TABLE natal_treasure(user_id TEXT PRIMARY KEY,form INTEGER,effect1_type INTEGER,effect1_level INTEGER,effect2_type INTEGER,effect2_level INTEGER,effect3_type INTEGER,effect3_level INTEGER)")
            uow.execute("INSERT INTO natal_treasure VALUES('u',1,11,1,12,1,13,3)")

    def tearDown(self):
        self.temp.cleanup()

    def call(self, operation_id, cost=1):
        return NatalEffectUpgradeSqlRepository(self.game, self.player).upgrade(operation_id, 'u', 20009, cost, 3, 5, 7)

    def test_upgrade_and_replay(self):
        first = self.call('op')
        replay = self.call('op')
        self.assertEqual((first['status'], replay['status'], first['level']), ('upgraded', 'duplicate', 2))

    def test_item_missing_and_rollback(self):
        with DatabaseUnitOfWork(self.game) as uow:
            uow.execute("UPDATE back SET goods_num=0 WHERE user_id='u'")
        self.assertEqual(self.call('missing')['status'], 'item_insufficient')
