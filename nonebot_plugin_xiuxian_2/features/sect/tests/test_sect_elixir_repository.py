import tempfile
import unittest
from pathlib import Path
from ..repository import SectRenameSqlRepository
from tests.test_db_backend import db_backend
class SectElixirRepositoryTests(unittest.TestCase):
 def setUp(self):
  self.t=tempfile.TemporaryDirectory();self.db=Path(self.t.name)/'s.db'
  with db_backend.transaction(self.db) as c:
   c.execute('CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,sect_id INTEGER,sect_position INTEGER,sect_contribution INTEGER,sect_elixir_get INTEGER)');c.execute("INSERT INTO user_xiuxian VALUES('u',1,1,100,0)");c.execute('CREATE TABLE sects(sect_id INTEGER PRIMARY KEY,elixir_room_level INTEGER,sect_materials INTEGER)');c.execute('INSERT INTO sects VALUES(1,1,100)');c.execute('CREATE TABLE back(user_id TEXT,goods_id INTEGER,goods_name TEXT,goods_type TEXT,goods_num INTEGER,create_time TEXT,update_time TEXT,bind_num INTEGER,UNIQUE(user_id,goods_id))');c.execute('CREATE TABLE sect_elixir_claim_operations(operation_id TEXT PRIMARY KEY,payload TEXT,rewards TEXT)')
  self.r=SectRenameSqlRepository(self.db)
 def tearDown(self):self.t.cleanup()
 def test_success_duplicate_and_claim_flag(self):
  rewards=[(9,'丹','丹药',1)];a=self.r.claim_elixir('x','u',1,10,20,rewards,99);b=self.r.claim_elixir('x','u',1,10,20,rewards,99);self.assertEqual(('applied','duplicate'),(a['status'],b['status']))
if __name__=='__main__':unittest.main()
