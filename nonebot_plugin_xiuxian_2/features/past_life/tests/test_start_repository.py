import tempfile
import unittest
from pathlib import Path
from ..start_repository import PastLifeStartSqlRepository
from tests.test_db_backend import db_backend

class PastLifeStartRepositoryTests(unittest.TestCase):
    def test_start_replay_and_state_creation(self):
        with tempfile.TemporaryDirectory() as temp:
            game,player=Path(temp)/'game.db',Path(temp)/'player.db'
            with db_backend.transaction(game) as c: c.execute('CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,username TEXT)'); c.execute("INSERT INTO user_xiuxian VALUES('u','User')")
            repo=PastLifeStartSqlRepository(game,player)
            kwargs={'expected_state':{},'alloc':{'悟性':3},'accumulated':{'悟性':3},'talent':'天赋','birth_scenario':'出生','event_indices':[0],'event_snapshots':[{'choices':[]}],'first_stage_message':'开始','choices_count':1,'refresh_slot_start':'2026-01-01 00:00:00'}
            first=repo.start('s','u',**kwargs); dup=repo.start('s','u',**kwargs); self.assertEqual((first.status,dup.status),('applied','duplicate'))
