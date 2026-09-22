import tempfile
import unittest
from pathlib import Path
from ..patrol_repository import DongfuPatrolSqlRepository
from tests.test_db_backend import db_backend

class DongfuPatrolRepositoryTests(unittest.TestCase):
    def test_patrol_replay_and_daily_limit(self):
        with tempfile.TemporaryDirectory() as temp:
            game,player=Path(temp)/'game.db',Path(temp)/'player.db'
            with db_backend.transaction(game) as c:
                c.execute('CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,user_stamina INTEGER,stone INTEGER)'); c.execute("INSERT INTO user_xiuxian VALUES('u',10,0)"); c.execute('CREATE TABLE back(user_id TEXT,goods_id INTEGER,goods_name TEXT,goods_type TEXT,goods_num INTEGER,bind_num INTEGER,UNIQUE(user_id,goods_id))')
            with db_backend.transaction(player) as c:
                c.execute('CREATE TABLE dongfu_status(user_id TEXT PRIMARY KEY,built INTEGER,patrol_date TEXT,patrol_count INTEGER,patrol_guard INTEGER)'); c.execute("INSERT INTO dongfu_status VALUES('u',1,'2026-01-01',0,0)")
            repo=DongfuPatrolSqlRepository(game,player); first=repo.patrol('p','u','2026-01-01',2,1,100,None,99); duplicate=repo.patrol('p','u','2026-01-01',2,1,100,None,99); limited=repo.patrol('q','u','2026-01-01',2,1,100,None,99); self.assertEqual((first.status,duplicate.status,limited.status),('patrolled','duplicate','daily_limit'))
