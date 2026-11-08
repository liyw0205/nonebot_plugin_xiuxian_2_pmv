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
                c.execute(
                    'CREATE TABLE impart_card_compose_operations('
                    'operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,'
                    'source_quantity INTEGER NOT NULL,target_quantity INTEGER NOT NULL)'
                )
                c.execute('CREATE TABLE xiuxian_impart(user_id TEXT PRIMARY KEY,impart_two_exp REAL DEFAULT 0,impart_exp_up REAL DEFAULT 0,impart_atk_per REAL DEFAULT 0,impart_hp_per REAL DEFAULT 0,impart_mp_per REAL DEFAULT 0,boss_atk REAL DEFAULT 0,impart_know_per REAL DEFAULT 0,impart_burst_per REAL DEFAULT 0,impart_mix_per REAL DEFAULT 0,impart_reap_per REAL DEFAULT 0)')
                c.execute("INSERT INTO xiuxian_impart(user_id) VALUES('u')")
            repo=ImpartCardComposeSqlRepository(db)
            first=repo.compose('c','u','A','B',5,1,5,{'A':{},'B':{}})
            duplicate=repo.compose('c','u','A','B',5,1,5,{'A':{},'B':{}})
            stale=repo.compose('s','u','A','B',5,1,5,{'A':{},'B':{}})
            self.assertEqual((first.status,duplicate.status,stale.status),('applied','duplicate','state_changed'))

    def test_missing_impart_row_refuses_before_touching_cards(self):
        """没有 xiuxian_impart 行时不得「给了卡却没有加成」：整笔拒绝且零写入。"""
        with tempfile.TemporaryDirectory() as temp:
            db = Path(temp) / 'impart.db'
            with db_backend.transaction(db) as c:
                c.execute('CREATE TABLE impart_cards(user_id TEXT,card_name TEXT,quantity INTEGER,UNIQUE(user_id,card_name))')
                c.execute("INSERT INTO impart_cards VALUES('u','A',5)")
                c.execute("INSERT INTO impart_cards VALUES('u','B',1)")
                c.execute(
                    'CREATE TABLE impart_card_compose_operations('
                    'operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,'
                    'source_quantity INTEGER NOT NULL,target_quantity INTEGER NOT NULL)'
                )
                c.execute('CREATE TABLE xiuxian_impart(user_id TEXT PRIMARY KEY,impart_two_exp REAL DEFAULT 0)')
            repo = ImpartCardComposeSqlRepository(db)
            result = repo.compose('c', 'u', 'A', 'B', 5, 1, 5, {'A': {}, 'B': {}})
            self.assertEqual('user_missing', result.status)
            self.assertFalse(result.succeeded)
            with db_backend.connection(db) as c:
                cards = dict(c.execute("SELECT card_name,quantity FROM impart_cards WHERE user_id='u'").fetchall())
                receipts = c.execute('SELECT COUNT(*) FROM impart_card_compose_operations').fetchone()[0]
            self.assertEqual({'A': 5, 'B': 1}, cards)
            self.assertEqual(0, receipts)

    def test_without_definitions_compose_is_unchanged(self):
        """未传 card_definitions 时不刷新加成，行为保持原样。"""
        with tempfile.TemporaryDirectory() as temp:
            db = Path(temp) / 'impart.db'
            with db_backend.transaction(db) as c:
                c.execute('CREATE TABLE impart_cards(user_id TEXT,card_name TEXT,quantity INTEGER,UNIQUE(user_id,card_name))')
                c.execute("INSERT INTO impart_cards VALUES('u','A',5)")
                c.execute("INSERT INTO impart_cards VALUES('u','B',1)")
                c.execute(
                    'CREATE TABLE impart_card_compose_operations('
                    'operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,'
                    'source_quantity INTEGER NOT NULL,target_quantity INTEGER NOT NULL)'
                )
            repo = ImpartCardComposeSqlRepository(db)
            result = repo.compose('c', 'u', 'A', 'B', 5, 1, 5, None)
            self.assertEqual('applied', result.status)
            self.assertEqual((0, 2), (result.source_quantity, result.target_quantity))
