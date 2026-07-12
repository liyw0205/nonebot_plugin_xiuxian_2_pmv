from __future__ import annotations
import json, tempfile, unittest
from pathlib import Path
import nonebot
nonebot.init()
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_world_events.demon_claim_service import DemonClaimService
from tests.test_db_backend import db_backend

class DemonClaimServiceTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory(); root=Path(self.tmp.name); self.g=root/'g.db'; self.p=root/'p.db'; self.claimed={}
        with db_backend.transaction(self.g) as c:
            c.execute('CREATE TABLE user_xiuxian (user_id TEXT PRIMARY KEY, stone INTEGER, exp INTEGER)'); c.execute('INSERT INTO user_xiuxian VALUES (%s,%s,%s)',('u',10,20)); c.execute('CREATE TABLE back (user_id TEXT, goods_id INTEGER, goods_name TEXT, goods_type TEXT, goods_num INTEGER, create_time TEXT, update_time TEXT, bind_num INTEGER, UNIQUE(user_id,goods_id))')
        with db_backend.transaction(self.p) as c:
            c.execute('CREATE TABLE world_event_state (user_id TEXT PRIMARY KEY, event_id TEXT, claimed TEXT)'); c.execute('INSERT INTO world_event_state VALUES (%s,%s,%s)',('global','e1','{}'))
        self.s=DemonClaimService(self.g,self.p); self.items=[{'id':1,'name':'i','type':'t','amount':2}]
    def tearDown(self): self.tmp.cleanup()
    def claim(self,op='c',**kw): return self.s.claim(op,'global',kw.get('event','e1'),'u',kw.get('claimed',{}),kw.get('stone',5),kw.get('exp',6),kw.get('items',self.items),kw.get('max',99))
    def state(self):
        with db_backend.connection(self.g) as c: u=c.execute('SELECT stone,exp FROM user_xiuxian').fetchone(); i=c.execute('SELECT goods_num FROM back').fetchone()
        with db_backend.connection(self.p) as c: cl=json.loads(c.execute('SELECT claimed FROM world_event_state').fetchone()[0])
        return tuple(map(int,u)), int(i[0]) if i else 0, cl
    def test_success(self): self.assertEqual(self.claim().status,'applied'); self.assertEqual(self.state(),((15,26),2,{'u':True}))
    def test_rejections(self):
        self.assertEqual(self.claim('stale',event='e2').status,'state_changed'); self.assertEqual(self.state(),((10,20),0,{}))
        with db_backend.transaction(self.g) as c: c.execute('INSERT INTO back VALUES (%s,%s,%s,%s,%s,%s,%s,%s)',('u',1,'i','t',99,'','',99))
        self.assertEqual(self.claim('full').status,'inventory_full')
    def test_duplicate_and_conflict(self):
        self.assertEqual(self.claim('r').status,'applied'); self.assertEqual(self.claim('r').status,'duplicate'); self.assertEqual(self.claim('r',stone=9).status,'state_changed'); self.assertEqual(self.state(),((15,26),2,{'u':True}))
    def test_failure_rolls_back(self):
        with db_backend.transaction(self.g) as c: c.execute('CREATE TABLE demon_claim_operations (operation_id TEXT PRIMARY KEY,payload TEXT,created_at TIMESTAMP)'); c.execute("CREATE TRIGGER fail BEFORE INSERT ON demon_claim_operations BEGIN SELECT RAISE(ABORT,'x'); END")
        with self.assertRaises(db_backend.IntegrityError): self.claim('x')
        self.assertEqual(self.state(),((10,20),0,{}))
