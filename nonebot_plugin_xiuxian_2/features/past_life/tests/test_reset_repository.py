import tempfile
import unittest
from pathlib import Path
from ..reset_repository import PastLifeResetSqlRepository
from tests.test_db_backend import db_backend

class PastLifeResetRepositoryTests(unittest.TestCase):
    def test_reset_one_replay_and_clear_history(self):
        with tempfile.TemporaryDirectory() as temp:
            game,player=Path(temp)/'game.db',Path(temp)/'player.db'
            with db_backend.transaction(game) as c: c.execute('CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY)'); c.execute("INSERT INTO user_xiuxian VALUES('u')")
            with db_backend.transaction(player) as c: c.execute('CREATE TABLE past_life(user_id TEXT PRIMARY KEY,state INTEGER,stage INTEGER,revision INTEGER,total_runs INTEGER,best_ending TEXT,best_score INTEGER,endings_log TEXT,achievement_points INTEGER)'); c.execute("INSERT INTO past_life VALUES('u',2,3,5,2,'end',88,'[]',9)")
            repo=PastLifeResetSqlRepository(game,player); first=repo.reset_one('r','u',False); dup=repo.reset_one('r','u',False); self.assertEqual((first.status,dup.status),('applied','duplicate'))
