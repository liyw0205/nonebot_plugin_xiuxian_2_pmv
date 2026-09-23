import tempfile
import unittest
from pathlib import Path
from ..training_repository import ImpartTrainingSqlRepository
from tests.test_db_backend import db_backend

class ImpartTrainingRepositoryTests(unittest.TestCase):
    def test_training_replay_and_snapshot_guard(self):
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp); game,impart,player=root/'game.db',root/'impart.db',root/'player.db'
            with db_backend.transaction(game) as c: c.execute('CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,exp INTEGER,power INTEGER)'); c.execute("INSERT INTO user_xiuxian VALUES('u',10,1)")
            with db_backend.transaction(impart) as c: c.execute('CREATE TABLE xiuxian_impart(user_id TEXT PRIMARY KEY,exp_day INTEGER)'); c.execute("INSERT INTO xiuxian_impart VALUES('u',20)")
            with db_backend.transaction(player) as c: c.execute('CREATE TABLE impart_pk_daily(user_id TEXT PRIMARY KEY,exp_used INTEGER,exp_count INTEGER,exp_load INTEGER,exp_gain INTEGER)'); c.execute("INSERT INTO impart_pk_daily VALUES('u',0,0,0,0)")
            repo=ImpartTrainingSqlRepository(game,impart,player); first=repo.settle('t','u',expected_exp=10,expected_exp_day=20,expected_daily={'exp_used':0,'exp_count':0,'exp_load':0,'exp_gain':0},exp_cost=2,exp_gain=5,exp_load_gain=1,power=2); dup=repo.settle('t','u',expected_exp=10,expected_exp_day=20,expected_daily={'exp_used':0,'exp_count':0,'exp_load':0,'exp_gain':0},exp_cost=2,exp_gain=5,exp_load_gain=1,power=2); self.assertEqual((first.status,dup.status),('applied','duplicate'))
