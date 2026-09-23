import json
import tempfile
import unittest
from pathlib import Path
from ..plant_repository import DongfuPlantSqlRepository
from tests.test_db_backend import db_backend

class DongfuPlantRepositoryTests(unittest.TestCase):
    def test_plant_replay_and_seed_guard(self):
        with tempfile.TemporaryDirectory() as temp:
            game,player=Path(temp)/'game.db',Path(temp)/'player.db'; slots=[{'slot':1,'seed_id':0,'seed_name':'','plant_start':'','plant_finish':'','fertilizer':0}]; expected=json.dumps(slots)
            with db_backend.transaction(game) as c: c.execute('CREATE TABLE back(user_id TEXT,goods_id INTEGER,goods_num INTEGER)'); c.execute("INSERT INTO back VALUES('u',1,1)")
            with db_backend.transaction(player) as c: c.execute('CREATE TABLE dongfu_status(user_id TEXT PRIMARY KEY,built INTEGER,plant_slots TEXT,planting INTEGER,plant_seed_id INTEGER,plant_start TEXT,plant_finish TEXT)'); c.execute("INSERT INTO dongfu_status VALUES('u',1,?,0,0,'','')",(expected,))
            repo=DongfuPlantSqlRepository(game,player); first=repo.plant('p','u',expected,1,1,'种子','2026-01-01','2026-01-02'); dup=repo.plant('p','u',expected,1,1,'种子','2026-01-01','2026-01-02'); self.assertEqual((first.status,dup.status),('planted','duplicate'))
