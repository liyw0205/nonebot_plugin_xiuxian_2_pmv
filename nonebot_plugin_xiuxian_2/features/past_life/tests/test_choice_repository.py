import tempfile
import unittest
from pathlib import Path
from ..choice_repository import PastLifeChoiceSqlRepository
from tests.test_db_backend import db_backend

class PastLifeChoiceRepositoryTests(unittest.TestCase):
    def test_advance_replay_and_state_update(self):
        with tempfile.TemporaryDirectory() as temp:
            game,player=Path(temp)/'game.db',Path(temp)/'player.db'
            with db_backend.transaction(game) as c: c.execute('CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY)'); c.execute("INSERT INTO user_xiuxian VALUES('u')")
            with db_backend.transaction(player) as c: c.execute('CREATE TABLE past_life(user_id TEXT PRIMARY KEY,state INTEGER,stage INTEGER,revision INTEGER,alloc TEXT,accumulated TEXT,talent TEXT,birth_scenario TEXT,total_score INTEGER,score_breakdown TEXT,event_indices TEXT,event_snapshots TEXT,early_death_rolls TEXT,history TEXT,last_run_time TEXT,total_runs INTEGER,best_ending TEXT,best_score INTEGER,endings_log TEXT,achievement_points INTEGER)'); c.execute("INSERT INTO past_life VALUES('u',2,0,1,'{}','{}','t','b',0,'{}','[]','[]','{}','[]','',0,'',0,'[]',0)")
            repo=PastLifeChoiceSqlRepository(game,player); expected={'state':2,'stage':0,'revision':1,'alloc':{},'accumulated':{},'talent':'t','birth_scenario':'b','total_score':0,'score_breakdown':{},'event_indices':[],'event_snapshots':[],'early_death_rolls':{},'history':[],'last_run_time':'','total_runs':0,'best_ending':'','best_score':0,'endings_log':[],'achievement_points':0}; final=dict(expected); final.update({'stage':1,'revision':2}); first=repo.advance('c','u',1,expected_state=expected,final_state=final,response={'message':'ok','is_end':False,'ending':None}); dup=repo.advance('c','u',1,expected_state=expected,final_state=final,response={'message':'ok','is_end':False,'ending':None}); self.assertEqual((first.status,dup.status),('applied','duplicate'))
