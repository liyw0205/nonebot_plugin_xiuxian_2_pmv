import tempfile, unittest
from pathlib import Path
from ..repository import DungeonSessionSqlRepository
from tests.test_db_backend import db_backend


class DungeonSessionRepositoryTests(unittest.TestCase):
    def setUp(self):
        self.t=tempfile.TemporaryDirectory();self.db=Path(self.t.name)/'p.db'
        with db_backend.transaction(self.db) as c:
            c.execute('CREATE TABLE player_dungeon_status(user_id TEXT PRIMARY KEY,dungeon_id TEXT,dungeon_status TEXT,current_layer INTEGER,total_layers INTEGER,last_reset_date TEXT,reset_generation INTEGER,reset_operation_id TEXT)');c.execute("INSERT INTO player_dungeon_status VALUES('u','d1','not_started',2,5,'2026-09-15',3,'r3')");c.execute('CREATE TABLE dungeon_session_operations(operation_id TEXT PRIMARY KEY,payload TEXT,result_status TEXT,dungeon_status TEXT)')
        self.r=DungeonSessionSqlRepository(self.db,self.db);self.expected={'dungeon_id':'d1','dungeon_status':'not_started','current_layer':2,'total_layers':5,'last_reset_date':'2026-09-15','reset_generation':3,'reset_operation_id':'r3'};self.dungeon={'dungeon_id':'d1','date':'2026-09-15'}
    def tearDown(self):self.t.cleanup()
    def test_enter_exit_and_replay(self):
        self.assertEqual('applied',self.r.session_transition('enter','u',self.expected,self.dungeon,'enter')['status']);exploring=dict(self.expected,dungeon_status='exploring');self.assertEqual('applied',self.r.session_transition('exit','u',exploring,self.dungeon,'exit')['status']);self.assertEqual('duplicate',self.r.operation_session_result('exit','u','exit')['status'])
    def test_aba_snapshot_rejected(self):
        stale=dict(self.expected,reset_generation=2);self.assertEqual('state_changed',self.r.session_transition('aba','u',stale,self.dungeon,'enter')['status'])

if __name__=='__main__':unittest.main()
