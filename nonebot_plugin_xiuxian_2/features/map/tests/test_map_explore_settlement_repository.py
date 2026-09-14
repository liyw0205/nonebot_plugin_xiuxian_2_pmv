import tempfile
import unittest
from datetime import datetime
from pathlib import Path

from ..repository import MapExploreSettlementSqlRepository
from tests.test_db_backend import db_backend


class Clock:
    def now(self): return datetime(2026, 9, 15, 12, 30)


class MapExploreSettlementRepositoryTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory(); root=Path(self.tmp.name); self.game=root/'g.db'; self.player=root/'p.db'
        with db_backend.transaction(self.game) as c:
            c.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,stone INTEGER)"); c.execute("INSERT INTO user_xiuxian VALUES('u',5)")
            c.execute("CREATE TABLE back(user_id TEXT,goods_id INTEGER,goods_name TEXT,goods_type TEXT,goods_num INTEGER,create_time TEXT,update_time TEXT,bind_num INTEGER,UNIQUE(user_id,goods_id))")
            c.execute("CREATE TABLE map_explore_settlement_operations(operation_id TEXT PRIMARY KEY,payload TEXT,stone INTEGER,rewards TEXT)")
        with db_backend.transaction(self.player) as c:
            c.execute("CREATE TABLE map_daily_limit(user_id TEXT PRIMARY KEY,date TEXT,explore_count INTEGER,resource_total_count INTEGER)"); c.execute("INSERT INTO map_daily_limit VALUES('u','2026-09-15',1,3)")
            c.execute("CREATE TABLE map_explore_status(user_id TEXT PRIMARY KEY,running INTEGER,node_type TEXT,node_name TEXT,start_time TEXT,duration_min INTEGER,settlement TEXT,max_duration_min INTEGER,interval_min INTEGER)"); c.execute("INSERT INTO map_explore_status VALUES('u',1,'遗迹','古迹','2026-09-15 12:00:00',20,'snap',120,20)")
        self.repo=MapExploreSettlementSqlRepository(self.game,self.player,clock=Clock())
        self.state={'running':1,'node_type':'遗迹','node_name':'古迹','start_time':'2026-09-15 12:00:00','duration_min':20,'settlement':'snap','max_duration_min':120,'interval_min':20}
        self.daily={'date':'2026-09-15','explore_count':1,'resource_total_count':3}
    def tearDown(self): self.tmp.cleanup()
    def call(self,op='x',cap=99): return self.repo.settle(op,'u',self.state,self.daily,5,7,[{'id':1,'name':'material','type':'material','amount':2}],cap)
    def test_success_and_replay(self):
        self.assertEqual('applied',self.call()['status']); self.assertEqual('duplicate',self.call()['status'])
        with db_backend.connection(self.game) as c: self.assertEqual(12,c.execute('SELECT stone FROM user_xiuxian').fetchone()[0])
        with db_backend.connection(self.player) as c: self.assertEqual(0,c.execute('SELECT running FROM map_explore_status').fetchone()[0])
    def test_inventory_rejection_preserves_state(self):
        self.assertEqual('inventory_full',self.call(cap=1)['status'])
        with db_backend.connection(self.game) as c: self.assertEqual(5,c.execute('SELECT stone FROM user_xiuxian').fetchone()[0])
    def test_stale_state_rejected(self):
        result = self.repo.settle('s', 'u', dict(self.state, node_name='旧'), self.daily, 5, 7, [], 99)
        self.assertEqual('state_changed', result['status'])


if __name__=='__main__': unittest.main()
