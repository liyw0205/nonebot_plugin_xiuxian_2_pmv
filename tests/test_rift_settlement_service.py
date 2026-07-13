import json,tempfile,unittest
from pathlib import Path
import nonebot

nonebot.init()
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_rift.settlement_service import RiftSettlementService
from tests.test_db_backend import db_backend
class T(unittest.TestCase):
 def setUp(self):
  self.t=tempfile.TemporaryDirectory(); self.db=Path(self.t.name)/'g'; self.r={'name':'r'}
  with db_backend.transaction(self.db) as c:
   c.execute('CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,stone INTEGER,exp INTEGER,hp INTEGER,mp INTEGER)');c.execute("INSERT INTO user_xiuxian VALUES('u',1,2,3,4)")
   c.execute('CREATE TABLE user_cd(user_id TEXT,type INTEGER,create_time TEXT,scheduled_time TEXT)');c.execute("INSERT INTO user_cd VALUES('u',3,'x','x')")
   c.execute('CREATE TABLE rift_entries(user_id TEXT,rift_data TEXT,status TEXT)');c.execute("INSERT INTO rift_entries VALUES('u',%s,'active')",(json.dumps(self.r),));c.execute('CREATE TABLE back(user_id TEXT,goods_id INTEGER,goods_name TEXT,goods_type TEXT,goods_num INTEGER,create_time TEXT,update_time TEXT,bind_num INTEGER,UNIQUE(user_id,goods_id))')
  self.s=RiftSettlementService(self.db);self.u={'stone':1,'exp':2,'hp':3,'mp':4}
 def tearDown(self):self.t.cleanup()
 def test_idempotent(self):self.assertEqual(self.s.settle('o','u',self.r,self.u,{}).status,'applied');self.assertEqual(self.s.settle('o','u',self.r,self.u,{}).status,'duplicate')
 def test_snapshot(self):self.assertEqual(self.s.settle('o','u',{'name':'x'},self.u,{}).status,'state_changed')
 def test_rollback(self):
  with db_backend.transaction(self.db) as c:c.execute('CREATE TABLE rift_settlement_operations(operation_id TEXT PRIMARY KEY,payload TEXT,explore_count INTEGER,created_at TEXT)');c.execute("CREATE TRIGGER f BEFORE INSERT ON rift_settlement_operations BEGIN SELECT RAISE(ABORT,'x');END")
  with self.assertRaises(Exception):self.s.settle('o','u',self.r,self.u,{})
  with db_backend.connection(self.db) as c:self.assertEqual(c.execute('SELECT status FROM rift_entries').fetchone()[0],'active')
