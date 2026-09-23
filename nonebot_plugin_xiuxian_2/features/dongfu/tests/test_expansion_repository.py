import tempfile
import unittest
from pathlib import Path
from ..expansion_repository import DongfuExpansionSqlRepository
from tests.test_db_backend import db_backend

class DongfuExpansionRepositoryTests(unittest.TestCase):
    def test_expand_replay_and_resource_guard(self):
        with tempfile.TemporaryDirectory() as temp:
            game,player=Path(temp)/'game.db',Path(temp)/'player.db'
            with db_backend.transaction(game) as c: c.execute('CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,stone INTEGER)'); c.execute("INSERT INTO user_xiuxian VALUES('u',100)"); c.execute('CREATE TABLE back(user_id TEXT,goods_id INTEGER,goods_num INTEGER)'); c.execute("INSERT INTO back VALUES('u',1,2)")
            with db_backend.transaction(player) as c: c.execute('CREATE TABLE dongfu_status(user_id TEXT PRIMARY KEY,built INTEGER,plot_count INTEGER)'); c.execute("INSERT INTO dongfu_status VALUES('u',1,3)")
            repo=DongfuExpansionSqlRepository(game,player); first=repo.expand('e','u',1,3,6,20); dup=repo.expand('e','u',1,3,6,20); self.assertEqual((first.status,dup.status),('expanded','duplicate'))
