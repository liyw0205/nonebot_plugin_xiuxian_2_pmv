import json
import tempfile
import unittest
from pathlib import Path
from ..repository import ArenaChallengePurchaseSqlRepository
from tests.test_db_backend import db_backend
class ArenaSettlementReplayTests(unittest.TestCase):
 def test_replay_missing_success_conflict(self):
  with tempfile.TemporaryDirectory() as d:
   g=Path(d)/'g';p=Path(d)/'p'
   with db_backend.transaction(g) as c:c.execute('CREATE TABLE arena_challenge_settlement_operations(operation_id TEXT PRIMARY KEY,challenger_id TEXT,payload TEXT,result_json TEXT)')
   r=ArenaChallengePurchaseSqlRepository(g,p);self.assertIsNone(r.settlement_result('x','u'))
   payload='[]'; result=json.dumps({'outcome':'win','score_delta':20})
   with db_backend.transaction(g) as c:c.execute('INSERT INTO arena_challenge_settlement_operations VALUES(?,?,?,?)',('x','u',payload,result))
   replay=r.settlement_result('x','u');self.assertIsNotNone(replay);assert replay is not None
   self.assertEqual(('duplicate','win'),(replay['status'],replay['outcome']))
   conflict=r.settlement_result('x','v');self.assertIsNotNone(conflict);assert conflict is not None
   self.assertEqual('operation_conflict',conflict['status'])
if __name__=='__main__':unittest.main()
