import json
import tempfile
import unittest
from pathlib import Path

from ..repository import DungeonSessionSqlRepository
from tests.test_db_backend import db_backend


class DungeonExploreReplayRepositoryTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.game=Path(self.temp.name)/'g.db';self.player=Path(self.temp.name)/'p.db'
        with db_backend.transaction(self.game) as c:
            c.execute("CREATE TABLE dungeon_explore_operations(operation_id TEXT PRIMARY KEY,request_identity TEXT,phase TEXT,prepared_json TEXT,result_status TEXT,result_json TEXT,current_layer INTEGER,dungeon_status TEXT)")
        self.repo=DungeonSessionSqlRepository(self.game,self.player)
    def tearDown(self):self.temp.cleanup()
    def test_missing_completed_and_conflict(self):
        self.assertEqual('missing',self.repo.replay('missing','u')['status'])
        identity=json.dumps({'action':'explore','user_id':'u'},ensure_ascii=True,sort_keys=True)
        with db_backend.transaction(self.game) as c:c.execute("INSERT INTO dungeon_explore_operations VALUES(?,?,?,?,?,?,?,?)",('op',identity,'completed','{}','won','{"message":"ok"}',2,'completed'))
        replay=self.repo.replay('op','u');self.assertEqual(('duplicate','completed','ok'),(replay['status'],replay['phase'],replay['response']['message']))
        self.assertEqual('operation_conflict',self.repo.replay('op','other')['status'])


if __name__=='__main__':unittest.main()
