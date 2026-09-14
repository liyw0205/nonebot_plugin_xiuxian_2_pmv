import tempfile
import unittest
from pathlib import Path
from ..repository import SectRenameSqlRepository
from tests.test_db_backend import db_backend
class SectShopRepositoryTests(unittest.TestCase):
 def setUp(self):
  self.t=tempfile.TemporaryDirectory();self.db=Path(self.t.name)/'s.db'
  with db_backend.transaction(self.db) as c:
   c.execute('CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,sect_id INTEGER,sect_contribution INTEGER)');c.execute("INSERT INTO user_xiuxian VALUES('u',1,100)");c.execute('CREATE TABLE sects(sect_id INTEGER PRIMARY KEY,sect_materials INTEGER,closed INTEGER)');c.execute('INSERT INTO sects VALUES(1,100,0)');c.execute('CREATE TABLE back(user_id TEXT,goods_id INTEGER,goods_name TEXT,goods_type TEXT,goods_num INTEGER,create_time TEXT,update_time TEXT,bind_num INTEGER,UNIQUE(user_id,goods_id))');c.execute('CREATE TABLE sect_shop_weekly_purchases(user_id TEXT,week_key TEXT,item_id INTEGER,quantity INTEGER,PRIMARY KEY(user_id,week_key,item_id))');c.execute('CREATE TABLE sect_shop_purchase_operations(operation_id TEXT PRIMARY KEY,payload TEXT,quantity INTEGER,cost INTEGER,contribution INTEGER,materials INTEGER,purchased INTEGER)')
  self.r=SectRenameSqlRepository(self.db)
 def tearDown(self):self.t.cleanup()
 def test_success_duplicate_and_limit(self):
  a=self.r.purchase('x','u',1,9,'item','type',2,10,5,0,99,week_key='2026-W38');b=self.r.purchase('x','u',1,9,'item','type',2,10,5,0,99,week_key='2026-W38');c=self.r.purchase('y','u',1,9,'item','type',4,10,5,2,99,week_key='2026-W38');self.assertEqual(('applied','duplicate','limit_reached'),(a['status'],b['status'],c['status']))
if __name__=='__main__':unittest.main()
