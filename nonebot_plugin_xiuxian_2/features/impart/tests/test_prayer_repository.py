import tempfile
import unittest
from pathlib import Path

from ..prayer_repository import ImpartPrayerSqlRepository
from tests.test_db_backend import db_backend

class ImpartPrayerRepositoryTests(unittest.TestCase):
    def test_settle_replay_and_item_guard(self):
        with tempfile.TemporaryDirectory() as temp:
            game, impart = Path(temp)/'game.db', Path(temp)/'impart.db'
            with db_backend.transaction(game) as c:
                c.execute('CREATE TABLE back(user_id TEXT,goods_id INTEGER,goods_num INTEGER,bind_num INTEGER,UNIQUE(user_id,goods_id))'); c.execute("INSERT INTO back VALUES('u',9,2,2)")
            with db_backend.transaction(impart) as c:
                c.execute('CREATE TABLE xiuxian_impart(user_id TEXT PRIMARY KEY,impart_two_exp REAL DEFAULT 0,impart_exp_up REAL DEFAULT 0,impart_atk_per REAL DEFAULT 0,impart_hp_per REAL DEFAULT 0,impart_mp_per REAL DEFAULT 0,boss_atk REAL DEFAULT 0,impart_know_per REAL DEFAULT 0,impart_burst_per REAL DEFAULT 0,impart_mix_per REAL DEFAULT 0,impart_reap_per REAL DEFAULT 0)'); c.execute("INSERT INTO xiuxian_impart(user_id) VALUES('u')")
                c.execute('CREATE TABLE impart_cards(user_id TEXT,card_name TEXT,quantity INTEGER,UNIQUE(user_id,card_name))')
            repo=ImpartPrayerSqlRepository(game,impart)
            first=repo.settle('p','u',9,1,['A'],{'A':{}})
            duplicate=repo.settle('p','u',9,1,['A'],{'A':{}})
            missing=repo.settle('q','u',9,2,['A','A'],{'A':{}})
            self.assertEqual((first.status,duplicate.status,missing.status),('applied','duplicate','item_missing'))
