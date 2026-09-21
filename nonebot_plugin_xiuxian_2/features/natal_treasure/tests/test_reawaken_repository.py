import tempfile
import unittest
from pathlib import Path

from ..reawaken_repository import NatalReawakenSqlRepository
from ....infrastructure.database import DatabaseUnitOfWork


class NatalReawakenSqlRepositoryTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        root = Path(self.temp.name)
        self.game = root / "game.db"
        self.player = root / "player.db"
        with DatabaseUnitOfWork(self.game) as uow:
            uow.execute("CREATE TABLE back(user_id TEXT, goods_id INTEGER, goods_name TEXT, goods_type TEXT, goods_num INTEGER, bind_num INTEGER, PRIMARY KEY(user_id,goods_id))")
            uow.execute("INSERT INTO back VALUES('u',20009,'神秘经书','神物',2,0)")
        with DatabaseUnitOfWork(self.player) as uow:
            uow.execute("CREATE TABLE natal_treasure(user_id TEXT PRIMARY KEY, form INTEGER, name TEXT, level INTEGER, exp INTEGER, max_exp INTEGER, effect1_type INTEGER, effect1_base_value REAL, effect1_level INTEGER, effect2_type INTEGER, effect2_base_value REAL, effect2_level INTEGER, effect3_type INTEGER, effect3_base_value REAL, effect3_level INTEGER, fate_revive_count INTEGER, immortal_revive_count INTEGER, invincible_gain_count INTEGER, nirvana_revive_count INTEGER, soul_return_revive_count INTEGER, charge_status INTEGER, soul_summon_count TEXT, enlightenment_count TEXT)")
            uow.execute("INSERT INTO natal_treasure VALUES('u',1,'旧法宝',5,50,600,1,0.1,1,2,0.2,3,3,0.3,2,1,2,3,4,5,1,'{}','{}')")
        self.configs = {1: (0.1, 0.2), 2: (0.3, 0.4)}
        self.names = {1: ('一号',), 2: ('二号',)}

    def tearDown(self):
        self.temp.cleanup()

    def call(self, operation_id, cost=1, max_goods=1000):
        return NatalReawakenSqlRepository(self.game, self.player).reawaken(operation_id, 'u', 20009, '神秘经书', '神物', cost, 3, max_goods, self.configs, self.names, {2}, 7)

    def test_reawaken_and_replay(self):
        first = self.call('op')
        replay = self.call('op')
        self.assertEqual((first['status'], replay['status']), ('reawakened', 'duplicate'))
        with DatabaseUnitOfWork(self.game) as uow:
            self.assertEqual(uow.query_one("SELECT goods_num FROM back WHERE user_id='u' AND goods_id=20009")["goods_num"], 4)

    def test_item_and_trigger_rollback(self):
        with DatabaseUnitOfWork(self.player) as uow:
            uow.execute("UPDATE natal_treasure SET effect2_level=1,effect3_level=1 WHERE user_id='u'")
        with DatabaseUnitOfWork(self.game) as uow:
            uow.execute("UPDATE back SET goods_num=0 WHERE user_id='u' AND goods_id=20009")
        self.assertEqual(self.call('missing', cost=1)['status'], 'item_insufficient')
        with DatabaseUnitOfWork(self.game) as uow:
            uow.execute("UPDATE back SET goods_num=2 WHERE user_id='u' AND goods_id=20009")
        with DatabaseUnitOfWork(self.game) as uow:
            uow.execute("CREATE TRIGGER fail_reawaken BEFORE INSERT ON natal_reawaken_operations BEGIN SELECT RAISE(ABORT, 'failed'); END")
        with self.assertRaises(Exception):
            self.call('rollback')
