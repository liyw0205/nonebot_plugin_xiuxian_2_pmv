import tempfile
import unittest
from datetime import datetime
from pathlib import Path

from ..repository import DungeonPurchaseSqlRepository
from tests.test_db_backend import db_backend


class Clock:
    def now(self): return datetime(2026, 9, 15, 12)


class DungeonPurchaseRepositoryTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.db=Path(self.temp.name)/'game.db'
        with db_backend.transaction(self.db) as c:
            c.execute('CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,stone INTEGER)');c.execute("INSERT INTO user_xiuxian VALUES('u',100)")
            c.execute('CREATE TABLE back(user_id TEXT,goods_id INTEGER,goods_name TEXT,goods_type TEXT,goods_num INTEGER,create_time TEXT,update_time TEXT,bind_num INTEGER,UNIQUE(user_id,goods_id))')
            c.execute("CREATE TABLE dungeon_purchase_operations(operation_id TEXT PRIMARY KEY,payload TEXT,result_status TEXT,quantity INTEGER,cost INTEGER,stone INTEGER,inventory INTEGER,response TEXT)")
        self.repo=DungeonPurchaseSqlRepository(self.db,self.db,clock=Clock())
    def tearDown(self):self.temp.cleanup()
    def buy(self,op='x',stone=100):return self.repo.purchase(op,'u',1,'item','type',2,10,stone,99,1)
    def test_success_and_replay(self):
        self.assertEqual('applied',self.buy()['status']);self.assertEqual('duplicate',self.buy()['status'])
        with db_backend.connection(self.db) as c:self.assertEqual((80,2),tuple(c.execute('SELECT stone,(SELECT goods_num FROM back) FROM user_xiuxian').fetchone()))
    def test_stale_wallet_rejected(self):self.assertEqual('state_changed',self.buy('stale',99)['status'])
    def test_operation_failure_rolls_back(self):
        with db_backend.transaction(self.db) as c:c.execute("CREATE TRIGGER fail_dp BEFORE INSERT ON dungeon_purchase_operations BEGIN SELECT RAISE(ABORT,'failed'); END")
        with self.assertRaises(Exception):self.buy('fail')
        with db_backend.connection(self.db) as c:self.assertEqual(100,c.execute('SELECT stone FROM user_xiuxian').fetchone()[0])


if __name__=='__main__':unittest.main()
