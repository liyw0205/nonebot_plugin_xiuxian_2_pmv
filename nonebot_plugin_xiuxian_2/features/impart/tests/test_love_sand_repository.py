import tempfile
import unittest
from pathlib import Path
from ..love_sand_repository import LoveSandSqlRepository
from tests.test_db_backend import db_backend

class LoveSandRepositoryTests(unittest.TestCase):
    def test_apply_replay_and_state_conflict(self):
        with tempfile.TemporaryDirectory() as temp:
            game, impart, player = (Path(temp)/name for name in ('game.db','impart.db','player.db'))
            with db_backend.transaction(game) as c:
                c.execute('CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY)'); c.execute("INSERT INTO user_xiuxian VALUES('u')")
                c.execute('CREATE TABLE back(user_id TEXT,goods_id INTEGER,goods_num INTEGER,bind_num INTEGER,UNIQUE(user_id,goods_id))'); c.execute("INSERT INTO back VALUES('u',1,3,3)")
            with db_backend.transaction(impart) as c:
                c.execute('CREATE TABLE xiuxian_impart(user_id TEXT PRIMARY KEY,stone_num INTEGER)'); c.execute("INSERT INTO xiuxian_impart VALUES('u',7)")
            first=LoveSandSqlRepository(game,impart,player).apply('x','u',1,1,20,3,7)
            duplicate=LoveSandSqlRepository(game,impart,player).apply('x','u',1,1,20,3,7)
            stale=LoveSandSqlRepository(game,impart,player).apply('y','u',1,1,20,3,7)
            self.assertEqual((first.status,duplicate.status,stale.status),('applied','duplicate','state_changed'))
