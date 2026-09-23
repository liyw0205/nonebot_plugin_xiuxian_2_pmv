import tempfile
import unittest
from pathlib import Path
from ..battle_repository import ImpartBattleBatchSqlRepository
from tests.test_db_backend import db_backend

class ImpartBattleRepositoryTests(unittest.TestCase):
    def test_battle_replay_and_snapshot_guard(self):
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp); impart,player=root/'impart.db',root/'player.db'
            with db_backend.transaction(impart) as c: c.execute('CREATE TABLE xiuxian_impart(user_id TEXT PRIMARY KEY,stone_num INTEGER)'); c.execute("INSERT INTO xiuxian_impart VALUES('a',0)"); c.execute("INSERT INTO xiuxian_impart VALUES('b',0)")
            with db_backend.transaction(player) as c: c.execute('CREATE TABLE impart_pk_state(user_id TEXT PRIMARY KEY,pk_num INTEGER,win_num INTEGER)'); c.execute("INSERT INTO impart_pk_state VALUES('a',3,0)"); c.execute("INSERT INTO impart_pk_state VALUES('b',3,0)")
            repo=ImpartBattleBatchSqlRepository(impart,player); first=repo.settle('b','a',3,1,0,2,'b',3,0,1,2); dup=repo.settle('b','a',3,1,0,2,'b',3,0,1,2); self.assertEqual((first.status,dup.status),('applied','duplicate'))
