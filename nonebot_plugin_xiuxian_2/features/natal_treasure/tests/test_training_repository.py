import tempfile
import unittest
from pathlib import Path

from ..training_repository import NatalTrainingSqlRepository
from ....infrastructure.database import DatabaseUnitOfWork


class NatalTrainingSqlRepositoryTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        root = Path(self.temp.name)
        self.game = root / "game.db"
        self.player = root / "player.db"
        with DatabaseUnitOfWork(self.game) as uow:
            uow.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY, stone INTEGER NOT NULL)")
            uow.execute("INSERT INTO user_xiuxian VALUES('u',10000000)")
        with DatabaseUnitOfWork(self.player) as uow:
            uow.execute("CREATE TABLE natal_treasure(user_id TEXT PRIMARY KEY, form INTEGER, level INTEGER, exp INTEGER, max_exp INTEGER)")
            uow.execute("INSERT INTO natal_treasure VALUES('u',1,0,0,100)")

    def tearDown(self):
        self.temp.cleanup()

    def call(self, operation_id, amount=5):
        return NatalTrainingSqlRepository(self.game, self.player).train(operation_id, 'u', amount, base_cost=1000000, growth_rate=0.5, max_level=10, max_exp_base=100, max_exp_growth=50)

    def test_train_and_replay(self):
        first = self.call('op')
        replay = self.call('op')
        self.assertEqual((first['status'], replay['status'], first['stone_cost']), ('trained', 'duplicate', 5000000))

    def test_insufficient_stone(self):
        with DatabaseUnitOfWork(self.game) as uow:
            uow.execute("UPDATE user_xiuxian SET stone=0 WHERE user_id='u'")
        self.assertEqual(self.call('short', 5)['status'], 'stone_insufficient')
