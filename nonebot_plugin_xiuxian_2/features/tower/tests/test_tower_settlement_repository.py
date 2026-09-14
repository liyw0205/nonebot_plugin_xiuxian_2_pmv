import json
import tempfile
import unittest
from pathlib import Path

from ..repository import TowerPurchaseSqlRepository
from tests.test_db_backend import db_backend

class TowerSettlementRepositoryTests(unittest.TestCase):
    def setUp(self):
        self.t=tempfile.TemporaryDirectory();r=Path(self.t.name);self.g=r/'g';self.p=r/'p'
        with db_backend.transaction(self.g) as c:
            c.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,stone INTEGER,exp INTEGER,hp INTEGER,mp INTEGER,user_stamina INTEGER)");c.execute("INSERT INTO user_xiuxian VALUES('u',10,100,50,80,12)");c.execute("CREATE TABLE back(user_id TEXT,goods_id INTEGER,goods_name TEXT,goods_type TEXT,goods_num INTEGER,create_time TEXT,update_time TEXT,bind_num INTEGER,UNIQUE(user_id,goods_id))");c.execute("CREATE TABLE tower_settlement_operations(operation_id TEXT PRIMARY KEY,payload TEXT,result_json TEXT)")
        with db_backend.transaction(self.p) as c:c.execute("CREATE TABLE tower(user_id TEXT PRIMARY KEY,current_floor INTEGER,max_floor INTEGER,score INTEGER,weekly_purchases TEXT)");c.execute("INSERT INTO tower VALUES('u',9,9,50,'{}')")
        self.rp=TowerPurchaseSqlRepository(self.g,self.p);self.expected={'current_floor':9,'max_floor':9,'score':50};self.player={'hp':50,'mp':80,'user_stamina':12}
    def tearDown(self):self.t.cleanup()
    def settle(self,op='x',**kw):
        v=dict(floor=10,score=8,stone=20,exp=30,items=({'id':1,'name':'item','type':'type','amount':1},),max_goods_num=99,expected_player=self.player,final_hp=17,final_mp=41,stamina_cost=3,challenge_succeeded=True);v.update(kw);return self.rp.settle(op,'u',self.expected,v['floor'],v['score'],v['stone'],v['exp'],v['items'],v['max_goods_num'],expected_player=v['expected_player'],final_hp=v['final_hp'],final_mp=v['final_mp'],stamina_cost=v['stamina_cost'],challenge_succeeded=v['challenge_succeeded'])
    def test_success_duplicate_and_failure_rollback(self):
        a=self.settle();b=self.settle();self.assertEqual(('applied','duplicate'),(a['status'],b['status']));self.assertEqual((10,10,58),tuple(self._tower()))
        with db_backend.transaction(self.g) as c:c.execute("CREATE TRIGGER fail_t BEFORE INSERT ON tower_settlement_operations BEGIN SELECT RAISE(ABORT,'failed'); END")
        with self.assertRaises(Exception):self.settle('fail',expected={'current_floor':10,'max_floor':10,'score':58},expected_player={'hp':17,'mp':41,'user_stamina':9},final_hp=20,final_mp=20)
    def _tower(self):
        with db_backend.connection(self.p) as c:return c.execute("SELECT current_floor,max_floor,score FROM tower").fetchone()
if __name__=='__main__':unittest.main()
