import tempfile
import unittest
from pathlib import Path
from ..reward_claim_repository import CompensationRewardClaimSqlRepository
from tests.test_db_backend import db_backend

class CompensationRewardClaimRepositoryTests(unittest.TestCase):
    def test_claim_replay_version_and_inventory_are_atomic(self):
        with tempfile.TemporaryDirectory() as temp:
            db=Path(temp)/'game.db'
            with db_backend.transaction(db) as c:
                c.execute('CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,stone INTEGER)'); c.execute("INSERT INTO user_xiuxian VALUES('u',10)")
                c.execute('CREATE TABLE back(user_id TEXT,goods_id INTEGER,goods_name TEXT,goods_type TEXT,goods_num INTEGER,create_time TEXT,update_time TEXT,bind_num INTEGER,UNIQUE(user_id,goods_id))')
                c.execute('CREATE TABLE compensation_definitions(record_id TEXT PRIMARY KEY,version INTEGER)'); c.execute("INSERT INTO compensation_definitions VALUES('r',1)")
            repo=CompensationRewardClaimSqlRepository(db,max_goods_num=99)
            items=[{'type':'stone','id':'stone','name':'灵石','quantity':5}]
            first=repo.claim('op','补偿','r','u',items,expected_definition_version=1)
            duplicate=repo.claim('op','补偿','r','u',items,expected_definition_version=1)
            changed=repo.claim('op2','补偿','r','u',items,expected_definition_version=2)
            self.assertEqual((first.status,duplicate.status,changed.status),('claimed','duplicate','definition_changed'))
