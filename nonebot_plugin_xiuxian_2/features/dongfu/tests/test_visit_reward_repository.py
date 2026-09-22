import tempfile
import unittest
from pathlib import Path
from ..visit_reward_repository import DongfuVisitRewardSqlRepository
from tests.test_db_backend import db_backend

class DongfuVisitRewardRepositoryTests(unittest.TestCase):
    def test_reward_replay_and_dongfu_guard(self):
        with tempfile.TemporaryDirectory() as temp:
            game,player=Path(temp)/'game.db',Path(temp)/'player.db'
            with db_backend.transaction(game) as c: c.execute('CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,stone INTEGER)'); c.execute("INSERT INTO user_xiuxian VALUES('v',0)")
            with db_backend.transaction(player) as c: c.execute('CREATE TABLE dongfu_status(user_id TEXT PRIMARY KEY,built INTEGER)'); c.execute("INSERT INTO dongfu_status VALUES('v',1)"); c.execute("INSERT INTO dongfu_status VALUES('t',1)")
            repo=DongfuVisitRewardSqlRepository(game,player); first=repo.reward('r','v','t',10); dup=repo.reward('r','v','t',10); self.assertEqual((first.status,dup.status),('rewarded','duplicate'))
