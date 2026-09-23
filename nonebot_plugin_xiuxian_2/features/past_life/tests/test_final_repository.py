import tempfile
import unittest
from pathlib import Path
from ..final_repository import PastLifeFinalSettlementSqlRepository
from tests.test_db_backend import db_backend

class PastLifeFinalRepositoryTests(unittest.TestCase):
    def test_settle_replays_and_updates_wallet_and_history(self):
        with tempfile.TemporaryDirectory() as temp:
            game, player = Path(temp)/'game.db', Path(temp)/'player.db'
            with db_backend.transaction(game) as c:
                c.execute('CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,exp INTEGER,stone INTEGER)')
                c.execute("INSERT INTO user_xiuxian VALUES('u',100,50)")
                c.execute('CREATE TABLE back(user_id TEXT,goods_id INTEGER,goods_name TEXT,goods_type TEXT,goods_num INTEGER,create_time TEXT,update_time TEXT,UNIQUE(user_id,goods_id))')
            with db_backend.transaction(player) as c:
                c.execute('CREATE TABLE past_life(user_id TEXT PRIMARY KEY,state INTEGER,stage INTEGER,revision INTEGER,alloc TEXT,accumulated TEXT,talent TEXT,birth_scenario TEXT,total_score INTEGER,score_breakdown TEXT,event_indices TEXT,event_snapshots TEXT,early_death_rolls TEXT,history TEXT,last_run_time TEXT,total_runs INTEGER,best_ending TEXT,best_score INTEGER,endings_log TEXT,achievement_points INTEGER)')
                c.execute("INSERT INTO past_life VALUES('u',2,3,1,'{}','{}','','',0,'{}','[]','[]','{}','[]',NULL,0,'',0,'[]',0)")
            repo = PastLifeFinalSettlementSqlRepository(game, player, max_goods_num=99)
            expected = {'state':2,'stage':3,'revision':1,'alloc':{},'accumulated':{},'talent':'','birth_scenario':'','total_score':0,'score_breakdown':{},'event_indices':[],'event_snapshots':[],'early_death_rolls':{},'history':[],'last_run_time':None,'total_runs':0,'best_ending':'','best_score':0,'endings_log':[],'achievement_points':0}
            final = dict(expected)
            first = repo.settle('f','u',expected,final,'ending',88,10,20,3,{'id':7,'name':'item','type':'道具','num':2},'now',{'message':'done'})
            duplicate = repo.settle('f','u',expected,final,'ending',88,10,20,3,{'id':7,'name':'item','type':'道具','num':2},'now',{'message':'done'})
            self.assertEqual((first.status, duplicate.status), ('applied','duplicate'))
