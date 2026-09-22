import tempfile
import unittest
from pathlib import Path
from ..accelerate_repository import DongfuAccelerateSqlRepository
from tests.test_db_backend import db_backend

class DongfuAccelerateRepositoryTests(unittest.TestCase):
    def test_accelerate_replay_and_state_guard(self):
        with tempfile.TemporaryDirectory() as temp:
            game, player=Path(temp)/'game.db',Path(temp)/'player.db'; slots='[{"slot":1,"seed_id":1,"plant_start":"2026-01-01 00:00:00","plant_finish":"2026-01-02 00:00:00"}]'
            with db_backend.transaction(game) as c:
                c.execute('CREATE TABLE back(user_id TEXT,goods_id INTEGER,goods_num INTEGER)'); c.execute("INSERT INTO back VALUES('u',1,1)")
            with db_backend.transaction(player) as c:
                c.execute('CREATE TABLE dongfu_status(user_id TEXT PRIMARY KEY,built INTEGER,plant_slots TEXT,planting INTEGER,plant_seed_id INTEGER,plant_start TEXT,plant_finish TEXT)'); c.execute("INSERT INTO dongfu_status VALUES('u',1,?,1,1,'2026-01-01 00:00:00','2026-01-02 00:00:00')",(slots,))
            repo=DongfuAccelerateSqlRepository(game,player); first=repo.accelerate('a','u',slots,1,1,'2026-01-01 12:00:00','2026-01-01 18:00:00'); duplicate=repo.accelerate('a','u',slots,1,1,'now','later'); self.assertEqual((first.status,duplicate.status),('accelerated','duplicate'))
