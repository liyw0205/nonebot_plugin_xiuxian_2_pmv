import tempfile, unittest
from datetime import datetime
from pathlib import Path
from ..repository import MapMissionClaimSqlRepository
from tests.test_db_backend import db_backend
class Clock:
    def now(self): return datetime(2026,9,15,12)
class MissionRepositoryTests(unittest.TestCase):
    def setUp(self):
        self.t=tempfile.TemporaryDirectory();r=Path(self.t.name);self.g=r/'g';self.p=r/'p'
        with db_backend.transaction(self.g) as c:
            c.execute('CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,stone INTEGER)');c.execute("INSERT INTO user_xiuxian VALUES('u',10)");c.execute('CREATE TABLE back(user_id TEXT,goods_id INTEGER,goods_name TEXT,goods_type TEXT,goods_num INTEGER,create_time TEXT,update_time TEXT,bind_num INTEGER,UNIQUE(user_id,goods_id))');c.execute('CREATE TABLE map_mission_claim_operations(operation_id TEXT PRIMARY KEY,payload TEXT,stone INTEGER,rewards TEXT)')
        with db_backend.transaction(self.p) as c:
            c.execute('CREATE TABLE map_mission(user_id TEXT PRIMARY KEY,date TEXT,mission_type TEXT,target INTEGER,claimed INTEGER,settlement TEXT)');c.execute("INSERT INTO map_mission VALUES('u','2026-09-15','gather',5,0,'snap')");c.execute('CREATE TABLE map_daily_limit(user_id TEXT PRIMARY KEY,date TEXT,gather_count INTEGER)');c.execute("INSERT INTO map_daily_limit VALUES('u','2026-09-15',5)")
        self.r=MapMissionClaimSqlRepository(self.g,self.p,clock=Clock());self.m={'date':'2026-09-15','mission_type':'gather','target':5,'claimed':0,'settlement':'snap'};self.d={'date':'2026-09-15','gather_count':5}
    def tearDown(self):self.t.cleanup()
    def call(self,op='x',daily=None):return self.r.claim(op,'u',self.m,daily or self.d,'gather_count',7,[{'id':1,'name':'m','type':'m','amount':2}],99)
    def test_success_replay(self):self.assertEqual('applied',self.call()['status']);self.assertEqual('duplicate',self.call()['status'])
    def test_incomplete(self):
        with db_backend.transaction(self.p) as c:c.execute('UPDATE map_daily_limit SET gather_count=4')
        self.assertEqual('not_completed',self.call('i',{'date':'2026-09-15','gather_count':4})['status'])
    def test_stale(self):self.assertEqual('state_changed',self.r.claim('s','u',dict(self.m,settlement='bad'),self.d,'gather_count',0,[],99)['status'])
if __name__=='__main__':unittest.main()
