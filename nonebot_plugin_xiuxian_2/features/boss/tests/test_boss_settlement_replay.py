import json
import tempfile
import unittest
from pathlib import Path
from ..repository import BossPurchaseSqlRepository
from tests.test_db_backend import db_backend
class BossSettlementReplayTests(unittest.TestCase):
 def test_missing_and_duplicate(self):
  with tempfile.TemporaryDirectory() as d:
   g=Path(d)/'g';p=Path(d)/'p'
   with db_backend.transaction(g) as c:c.execute("CREATE TABLE world_boss_battle_operations(operation_id TEXT PRIMARY KEY,payload TEXT,boss_hp INTEGER,stamina INTEGER,battle_count INTEGER,stone INTEGER,exp INTEGER,integral INTEGER,activity_lines TEXT)");c.execute("INSERT INTO world_boss_battle_operations VALUES('x','[]',1,2,3,4,5,6,'[]')")
   r=BossPurchaseSqlRepository(g,p);self.assertIsNone(r.settlement_result('missing'));self.assertEqual('duplicate',r.settlement_result('x')['status'])
if __name__=='__main__':unittest.main()
