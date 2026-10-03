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
            c.execute("CREATE TABLE dungeon_explore_operations(operation_id TEXT PRIMARY KEY,request_identity TEXT,phase TEXT,prepared_json TEXT,intent_json TEXT DEFAULT '{}',result_status TEXT,result_json TEXT,current_layer INTEGER,dungeon_status TEXT,updated_at TEXT)")
        self.repo=DungeonSessionSqlRepository(self.game,self.player)
    def tearDown(self):self.temp.cleanup()
    def test_missing_completed_and_conflict(self):
        self.assertEqual('missing',self.repo.replay('missing','u')['status'])
        identity=json.dumps({'action':'explore','user_id':'u'},ensure_ascii=True,sort_keys=True)
        with db_backend.transaction(self.game) as c:c.execute("INSERT INTO dungeon_explore_operations(operation_id,request_identity,phase,prepared_json,intent_json,result_status,result_json,current_layer,dungeon_status,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?)",('op',identity,'completed','{}','{}','won','{"message":"ok"}',2,'completed',''))
        replay=self.repo.replay('op','u');self.assertEqual(('duplicate','completed','ok'),(replay['status'],replay['phase'],replay['response']['message']))
        self.assertEqual('operation_conflict',self.repo.replay('op','other')['status'])

    def test_prepare_and_rejection_are_idempotent(self):
        plan = {"user_id": "u", "response": {"message": "ready"}}
        self.assertEqual("intent_required", self.repo.prepare("prepared", "u", plan)["result_status"])
        intent = {"seed_version": "dungeon-explore-rng-v1", "seeds": {"encounter": "e", "battle": "b", "reward": "r"}}
        self.assertEqual("intent", self.repo.prepare_intent("prepared", "u", intent)["phase"])
        self.assertEqual("prepared", self.repo.prepare_resolution("prepared", "u", plan)["phase"])
        self.assertEqual("prepared", self.repo.prepare_resolution("prepared", "u", {"other": True})["phase"])
        response = {"message": "rejected"}
        first = self.repo.resolve_rejection("rejected", "u", "invalid", response, 99, current_layer=2, dungeon_status="exited")
        replay = self.repo.replay("rejected", "u")
        self.assertEqual(("completed", "duplicate", "rejected"), (first["phase"], replay["status"], replay["response"]["message"]))


if __name__=='__main__':unittest.main()
