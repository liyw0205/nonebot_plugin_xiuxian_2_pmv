import tempfile
import unittest
from pathlib import Path
from ..explore_repository import ImpartExploreSqlRepository
from tests.test_db_backend import db_backend

class ImpartExploreRepositoryTests(unittest.TestCase):
    def test_explore_replay_and_snapshot_guard(self):
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp); game,impart,player=root/'game.db',root/'impart.db',root/'player.db'
            with db_backend.transaction(game) as c: c.execute('CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY)'); c.execute("INSERT INTO user_xiuxian VALUES('u')")
            with db_backend.transaction(impart) as c: c.execute('CREATE TABLE xiuxian_impart(user_id TEXT PRIMARY KEY,exp_day INTEGER,impart_lv INTEGER)'); c.execute("INSERT INTO xiuxian_impart VALUES('u',100,2)")
            with db_backend.transaction(player) as c: c.execute('CREATE TABLE impart_pk_daily(user_id TEXT PRIMARY KEY,impart_num INTEGER)'); c.execute("INSERT INTO impart_pk_daily VALUES('u',3)")
            repo=ImpartExploreSqlRepository(game,impart,player); first=repo.settle('e','u',event_type='up',expected_exp_day=100,expected_impart_lv=2,expected_impart_num=3,time_cost=5,new_impart_lv=3); dup=repo.settle('e','u',event_type='up',expected_exp_day=100,expected_impart_lv=2,expected_impart_num=3,time_cost=5,new_impart_lv=3); self.assertEqual((first.status,dup.status),('applied','duplicate'))
