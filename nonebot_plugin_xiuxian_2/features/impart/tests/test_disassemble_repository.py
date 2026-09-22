import tempfile
import unittest
from pathlib import Path
from ..disassemble_repository import ImpartCardDisassembleSqlRepository
from tests.test_db_backend import db_backend

class ImpartCardDisassembleRepositoryTests(unittest.TestCase):
    def test_disassemble_replay_and_keep_one_card(self):
        with tempfile.TemporaryDirectory() as temp:
            db=Path(temp)/'impart.db'
            with db_backend.transaction(db) as c:
                c.execute('CREATE TABLE impart_cards(user_id TEXT,card_name TEXT,quantity INTEGER,UNIQUE(user_id,card_name))'); c.execute("INSERT INTO impart_cards VALUES('u','A',3)")
                c.execute('CREATE TABLE xiuxian_impart(user_id TEXT PRIMARY KEY,stone_num INTEGER,impart_two_exp REAL DEFAULT 0,impart_exp_up REAL DEFAULT 0,impart_atk_per REAL DEFAULT 0,impart_hp_per REAL DEFAULT 0,impart_mp_per REAL DEFAULT 0,boss_atk REAL DEFAULT 0,impart_know_per REAL DEFAULT 0,impart_burst_per REAL DEFAULT 0,impart_mix_per REAL DEFAULT 0,impart_reap_per REAL DEFAULT 0)'); c.execute("INSERT INTO xiuxian_impart(user_id,stone_num) VALUES('u',10)")
            repo=ImpartCardDisassembleSqlRepository(db)
            first=repo.disassemble('d','u','A',2,3,10,2,{'A':{}}); duplicate=repo.disassemble('d','u','A',2,3,10,2,{'A':{}}); missing=repo.disassemble('m','u','A',2,1,14,2,{'A':{}})
            self.assertEqual((first.status,duplicate.status,missing.status),('applied','duplicate','card_missing'))
