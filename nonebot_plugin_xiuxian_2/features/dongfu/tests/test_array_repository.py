import tempfile
import unittest
from pathlib import Path
from ..array_repository import DongfuArrayUpgradeSqlRepository
from tests.test_db_backend import db_backend

class DongfuArrayUpgradeRepositoryTests(unittest.TestCase):
    def test_upgrade_replay_and_resource_guard(self):
        with tempfile.TemporaryDirectory() as temp:
            game,player=Path(temp)/'game.db',Path(temp)/'player.db'
            with db_backend.transaction(game) as c: c.execute('CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,stone INTEGER)'); c.execute("INSERT INTO user_xiuxian VALUES('u',100)"); c.execute('CREATE TABLE back(user_id TEXT,goods_id INTEGER,goods_num INTEGER)'); c.execute("INSERT INTO back VALUES('u',1,2)")
            with db_backend.transaction(player) as c: c.execute('CREATE TABLE dongfu_status(user_id TEXT PRIMARY KEY,built INTEGER,array_level INTEGER)'); c.execute("INSERT INTO dongfu_status VALUES('u',1,1)")
            repo=DongfuArrayUpgradeSqlRepository(game,player); first=repo.upgrade('a','u',1,2,10,1,1); dup=repo.upgrade('a','u',1,2,10,1,1); stale=repo.upgrade('b','u',1,2,10,1,1); self.assertEqual((first.status,dup.status,stale.status),('upgraded','duplicate','state_changed'))
