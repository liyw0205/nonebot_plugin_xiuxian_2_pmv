import tempfile
import unittest
from pathlib import Path
from ..repository import DungeonSessionSqlRepository
from tests.test_db_backend import db_backend

class DungeonExploreSettlementRepositoryTests(unittest.TestCase):
    def test_settle_replays_prepared_operation(self):
        with tempfile.TemporaryDirectory() as temp:
            game, player = Path(temp)/'game.db', Path(temp)/'player.db'
            with db_backend.transaction(game) as c:
                c.execute('CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,hp INTEGER,mp INTEGER,stone INTEGER,exp INTEGER)')
                c.execute("INSERT INTO user_xiuxian VALUES('u',100,100,10,0)")
                c.execute('CREATE TABLE user_cd(user_id TEXT,type INTEGER)')
                c.execute("INSERT INTO user_cd VALUES('u',0)")
                c.execute('CREATE TABLE back(user_id TEXT,goods_id INTEGER,goods_name TEXT,goods_type TEXT,goods_num INTEGER,create_time TEXT,update_time TEXT,bind_num INTEGER,UNIQUE(user_id,goods_id))')
                c.execute('CREATE TABLE dungeon_explore_operations(operation_id TEXT PRIMARY KEY,request_identity TEXT,phase TEXT,prepared_json TEXT,result_status TEXT,result_json TEXT,current_layer INTEGER,dungeon_status TEXT,created_at TEXT,updated_at TEXT)')
            with db_backend.transaction(player) as c:
                c.execute('CREATE TABLE player_dungeon_status(user_id TEXT PRIMARY KEY,dungeon_id TEXT,dungeon_name TEXT,dungeon_status TEXT,current_layer INTEGER,total_layers INTEGER,last_reset_date TEXT,reset_generation INTEGER,reset_operation_id TEXT)')
                c.execute("INSERT INTO player_dungeon_status VALUES('u','d','D','exploring',1,2,'2026-07-15',1,'reset-1')")
            repo = DungeonSessionSqlRepository(game, player)
            repo.prepare('op','u',{'expected_status':{'dungeon_id':'d','dungeon_name':'D','dungeon_status':'exploring','current_layer':1,'total_layers':2,'last_reset_date':'2026-07-15','reset_generation':1,'reset_operation_id':'reset-1'},'team':None,'members':[{'user_id':'u','expected':{'hp':100,'mp':100,'stone':10,'exp':0,'cd_type':0},'final_hp':90,'final_mp':80,'stone_delta':3,'exp_delta':5,'items':[]}],'advance':True,'complete':False,'response':{'message':'ok'}})
            first = repo.settle('op','u',99)
            duplicate = repo.settle('op','u',99)
            self.assertEqual((first['status'], duplicate['status']), ('applied','duplicate'))
