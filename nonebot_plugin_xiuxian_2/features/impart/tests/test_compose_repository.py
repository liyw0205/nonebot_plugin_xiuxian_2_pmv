import tempfile
import unittest
from pathlib import Path

from ..compose_repository import ImpartCardComposeSqlRepository
from tests.test_db_backend import db_backend

class ImpartCardComposeRepositoryTests(unittest.TestCase):
    def test_compose_replay_and_state_guard(self):
        with tempfile.TemporaryDirectory() as temp:
            db=Path(temp)/'impart.db'
            with db_backend.transaction(db) as c:
                c.execute('CREATE TABLE impart_cards(user_id TEXT,card_name TEXT,quantity INTEGER,UNIQUE(user_id,card_name))')
                c.execute("INSERT INTO impart_cards VALUES('u','A',5)")
                c.execute("INSERT INTO impart_cards VALUES('u','B',1)")
                c.execute('CREATE TABLE xiuxian_impart(user_id TEXT PRIMARY KEY,impart_two_exp REAL DEFAULT 0,impart_exp_up REAL DEFAULT 0,impart_atk_per REAL DEFAULT 0,impart_hp_per REAL DEFAULT 0,impart_mp_per REAL DEFAULT 0,boss_atk REAL DEFAULT 0,impart_know_per REAL DEFAULT 0,impart_burst_per REAL DEFAULT 0,impart_mix_per REAL DEFAULT 0,impart_reap_per REAL DEFAULT 0)')
                c.execute("INSERT INTO xiuxian_impart(user_id) VALUES('u')")
            repo=ImpartCardComposeSqlRepository(db)
            first=repo.compose('c','u','A','B',5,1,5,{'A':{},'B':{}})
            duplicate=repo.compose('c','u','A','B',5,1,5,{'A':{},'B':{}})
            stale=repo.compose('s','u','A','B',5,1,5,{'A':{},'B':{}})
            self.assertEqual((first.status,duplicate.status,stale.status),('applied','duplicate','state_changed'))
