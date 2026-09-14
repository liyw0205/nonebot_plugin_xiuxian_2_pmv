import tempfile
import unittest
from pathlib import Path
from ..repository import ArenaChallengePurchaseSqlRepository
from tests.test_db_backend import db_backend
class ArenaTicketSqlTests(unittest.TestCase):
 def setUp(self):
  self.t=tempfile.TemporaryDirectory();r=Path(self.t.name);self.g=r/'g';self.p=r/'p'
  with db_backend.transaction(self.g) as c:
   c.execute("CREATE TABLE back(user_id TEXT,goods_id INTEGER,goods_num INTEGER,bind_num INTEGER,UNIQUE(user_id,goods_id))");c.execute("INSERT INTO back VALUES('u',9,3,3)");c.execute("CREATE TABLE arena_challenge_ticket_operations(operation_id TEXT PRIMARY KEY,payload TEXT,used_tickets INTEGER,item_remaining INTEGER,challenges_used INTEGER,challenges_remaining INTEGER,challenge_cap INTEGER)")
  with db_backend.transaction(self.p) as c:c.execute("CREATE TABLE arena(user_id TEXT PRIMARY KEY,daily_challenges_used INTEGER,daily_extra_challenges INTEGER)");c.execute("INSERT INTO arena VALUES('u',2,1)")
  self.r=ArenaChallengePurchaseSqlRepository(self.g,self.p)
 def tearDown(self):self.t.cleanup()
 def test_success_replay_and_rollback(self):
  a=self.r.use_challenge_ticket('x','u',9,1,3,2,1,11);b=self.r.use_challenge_ticket('x','u',9,1,3,2,1,11);self.assertEqual((a['status'],b['status'],a['used_tickets']),('applied','duplicate',1))
  with db_backend.transaction(self.g) as c:c.execute("CREATE TRIGGER fail BEFORE INSERT ON arena_challenge_ticket_operations BEGIN SELECT RAISE(ABORT,'x'); END")
  with self.assertRaises(Exception):self.r.use_challenge_ticket('y','u',9,1,2,1,1,10)
  with db_backend.connection(self.g) as c:self.assertEqual(2,c.execute("SELECT goods_num FROM back").fetchone()[0])
if __name__=='__main__':unittest.main()
