import json
import tempfile
import unittest
from datetime import date
from pathlib import Path
from ..repository import BossPurchaseSqlRepository
from tests.test_db_backend import db_backend
class BossPurchaseRepositoryTests(unittest.TestCase):
 def setUp(self):
  self.t=tempfile.TemporaryDirectory();r=Path(self.t.name);self.g=r/'g';self.p=r/'p'
  with db_backend.transaction(self.g) as c:c.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY)");c.execute("INSERT INTO user_xiuxian VALUES('u')");c.execute("CREATE TABLE back(user_id TEXT,goods_id INTEGER,goods_name TEXT,goods_type TEXT,goods_num INTEGER,create_time TEXT,update_time TEXT,bind_num INTEGER,UNIQUE(user_id,goods_id))");c.execute("CREATE TABLE boss_purchase_operations(operation_id TEXT PRIMARY KEY,payload TEXT,quantity INTEGER,cost INTEGER,integral INTEGER,purchased INTEGER,inventory INTEGER)")
  with db_backend.transaction(self.p) as c:c.execute("CREATE TABLE boss_limit(user_id TEXT PRIMARY KEY,integral INTEGER)");c.execute("INSERT INTO boss_limit VALUES('u',100)");c.execute("CREATE TABLE boss(user_id TEXT PRIMARY KEY,weekly_purchases TEXT)");c.execute("INSERT INTO boss VALUES('u',?)",(json.dumps({'_last_reset':'2026-09-15','1':1}),))
  self.r=BossPurchaseSqlRepository(self.g,self.p)
 def tearDown(self):self.t.cleanup()
 def buy(self,op='x',**kw):
  v=dict(item_id=1,item_name='item',item_type='type',quantity=2,unit_cost=10,weekly_limit=5,expected_integral=100,expected_weekly_purchases={'_last_reset':'2026-09-15','1':1},max_goods_num=99,today=date(2026,9,15));v.update(kw);return self.r.purchase(op,'u',**v)
 def test_success_duplicate_conflict_and_rollback(self):
  a=self.buy();b=self.buy();c=self.buy(quantity=1);self.assertEqual((a['status'],b['status'],c['status']),('applied','duplicate','state_changed'))
  with db_backend.connection(self.p) as db:self.assertEqual(80,db.execute("SELECT integral FROM boss_limit").fetchone()[0])
if __name__=='__main__':unittest.main()
