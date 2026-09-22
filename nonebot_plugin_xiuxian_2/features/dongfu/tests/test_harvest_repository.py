import json
import tempfile
import unittest
from pathlib import Path
from ..harvest_repository import DongfuHarvestSqlRepository
from tests.test_db_backend import db_backend

class DongfuHarvestRepositoryTests(unittest.TestCase):
    def test_harvest_replay_and_maturity_guard(self):
        with tempfile.TemporaryDirectory() as temp:
            game,player=Path(temp)/'game.db',Path(temp)/'player.db'; slots=[{'slot':1,'seed_id':1,'plant_finish':'2026-01-01 00:00:00'}]; expected=json.dumps(slots)
            with db_backend.transaction(game) as c: c.execute('CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY)'); c.execute("INSERT INTO user_xiuxian VALUES('u')"); c.execute('CREATE TABLE back(user_id TEXT,goods_id INTEGER,goods_name TEXT,goods_type TEXT,goods_num INTEGER,create_time TEXT,update_time TEXT,bind_num INTEGER,UNIQUE(user_id,goods_id))')
            with db_backend.transaction(player) as c: c.execute('CREATE TABLE dongfu_status(user_id TEXT PRIMARY KEY,built INTEGER,plant_slots TEXT,planting INTEGER,plant_seed_id INTEGER,plant_start TEXT,plant_finish TEXT,harvest_settlement TEXT)'); c.execute("INSERT INTO dongfu_status VALUES('u',1,?,?,?,?,?,?)",(expected,1,1,'','2025-12-31 00:00:00',''))
            repo=DongfuHarvestSqlRepository(game,player); items=[{'id':2,'name':'果','type':'特殊物品','amount':1}]; first=repo.harvest('h','u',slots,[1],items,99,'2026-01-02 00:00:00'); dup=repo.harvest('h','u',slots,[1],items,99,'2026-01-02 00:00:00'); self.assertEqual((first.status,dup.status),('harvested','duplicate'))
