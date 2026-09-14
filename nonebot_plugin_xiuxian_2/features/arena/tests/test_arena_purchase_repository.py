import json
import tempfile
import unittest
from datetime import date
from pathlib import Path

from ..repository import ArenaChallengePurchaseSqlRepository
from tests.test_db_backend import db_backend


class ArenaPurchaseSqlRepositoryTests(unittest.TestCase):
    def setUp(self):
        self.t=tempfile.TemporaryDirectory(); root=Path(self.t.name); self.game=root/'g.db'; self.player=root/'p.db'
        with db_backend.transaction(self.game) as c:
            c.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY)"); c.execute("INSERT INTO user_xiuxian VALUES('u')")
            c.execute("CREATE TABLE back(user_id TEXT,goods_id INTEGER,goods_name TEXT,goods_type TEXT,goods_num INTEGER,create_time TEXT,update_time TEXT,bind_num INTEGER,UNIQUE(user_id,goods_id))")
            c.execute("CREATE TABLE arena_purchase_operations(operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,quantity INTEGER,cost INTEGER,honor_points INTEGER,purchased INTEGER,inventory INTEGER)")
        with db_backend.transaction(self.player) as c:
            c.execute("CREATE TABLE arena(user_id TEXT PRIMARY KEY,honor_points INTEGER,weekly_purchases TEXT)")
            c.execute("INSERT INTO arena VALUES('u',100,?)",(json.dumps({'_last_reset':'2026-09-15','1':1}),))
        self.r=ArenaChallengePurchaseSqlRepository(self.game,self.player)
    def tearDown(self):self.t.cleanup()
    def buy(self,op='x',**kw):
        v=dict(item_id=1,item_name='item',item_type='type',quantity=2,unit_cost=10,weekly_limit=5,expected_honor=100,expected_weekly_purchases={'_last_reset':'2026-09-15','1':1},max_goods_num=99,bind_flag=1,today=date(2026,9,15));v.update(kw);return self.r.purchase(op,'u',**v)
    def state(self):
        with db_backend.connection(self.game) as c:item=c.execute("SELECT goods_num,bind_num FROM back").fetchone()
        with db_backend.connection(self.player) as c:a=c.execute("SELECT honor_points,weekly_purchases FROM arena").fetchone()
        return (int(a[0]),json.loads(a[1]),tuple(item) if item else None)
    def test_success_duplicate_conflict(self):
        a=self.buy();b=self.buy();c=self.buy(quantity=1);self.assertEqual((a['status'],b['status'],c['status']),('applied','duplicate','state_changed'));self.assertEqual(self.state(),(80,{'_last_reset':'2026-09-15','1':3},(2,2)))
    def test_rejections_and_rollback(self):
        self.assertEqual('limit_reached',self.buy('limit',quantity=5)['status']);self.assertEqual('honor_insufficient',self.buy('poor',unit_cost=60)['status'])
        with db_backend.transaction(self.game) as c:c.execute("CREATE TRIGGER fail_ap BEFORE INSERT ON arena_purchase_operations BEGIN SELECT RAISE(ABORT,'failed'); END")
        with self.assertRaises(Exception):self.buy('fail')
        self.assertEqual(self.state(),(100,{'_last_reset':'2026-09-15','1':1},None))

if __name__=='__main__':unittest.main()
