import tempfile
import unittest
from pathlib import Path
from ..reward_claim_repository import CompensationRewardClaimSqlRepository
from tests.test_db_backend import db_backend

class CompensationRewardClaimLimitTests(unittest.TestCase):
    def test_limited_claim_honors_legacy_baseline(self):
        with tempfile.TemporaryDirectory() as temp:
            db=Path(temp)/'game.db'
            with db_backend.transaction(db) as c:
                c.execute('CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,stone INTEGER)'); c.execute("INSERT INTO user_xiuxian VALUES('u1',0)"); c.execute("CREATE TABLE back(user_id TEXT,goods_id INTEGER,goods_name TEXT,goods_type TEXT,goods_num INTEGER,create_time TEXT,update_time TEXT,bind_num INTEGER,UNIQUE(user_id,goods_id))")
            repo=CompensationRewardClaimSqlRepository(db,99); items=[]
            first=repo.claim('a','兑换码','CODE','u1',items,usage_limit=2,legacy_used_count=1)
            with db_backend.transaction(db) as c: c.execute("INSERT INTO user_xiuxian VALUES('u2',0)")
            second=repo.claim('b','兑换码','CODE','u2',items,usage_limit=2,legacy_used_count=1)
            self.assertEqual((first.status,first.used_count,second.status),('claimed',2,'exhausted'))
