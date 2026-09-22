import tempfile
import unittest
from pathlib import Path
from ..fertilize_repository import DongfuFertilizeSqlRepository
from tests.test_db_backend import db_backend

class DongfuFertilizeRepositoryTests(unittest.TestCase):
    def test_fertilize_replay_and_full_guard(self):
        with tempfile.TemporaryDirectory() as temp:
            game,player=Path(temp)/'game.db',Path(temp)/'player.db'; slots='[{"slot":1,"seed_id":1,"fertilizer":0}]'
            with db_backend.transaction(game) as c: c.execute('CREATE TABLE back(user_id TEXT,goods_id INTEGER,goods_num INTEGER)'); c.execute("INSERT INTO back VALUES('u',1,1)")
            with db_backend.transaction(player) as c: c.execute('CREATE TABLE dongfu_status(user_id TEXT PRIMARY KEY,built INTEGER,plant_slots TEXT)'); c.execute("INSERT INTO dongfu_status VALUES('u',1,?)",(slots,))
            repo=DongfuFertilizeSqlRepository(game,player); first=repo.fertilize('f','u',slots,1,1,3); dup=repo.fertilize('f','u',slots,1,1,3); self.assertEqual((first.status,dup.status),('fertilized','duplicate'))
