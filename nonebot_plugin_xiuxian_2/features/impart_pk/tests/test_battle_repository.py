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

    def test_exhaustion_removes_membership_only_after_successful_settlement(self):
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp); impart,player=root/'impart.db',root/'player.db'
            with db_backend.transaction(impart) as conn:
                conn.execute('CREATE TABLE xiuxian_impart(user_id TEXT PRIMARY KEY,stone_num INTEGER)')
                conn.execute("INSERT INTO xiuxian_impart VALUES('a',0)")
            with db_backend.transaction(player) as conn:
                conn.execute('CREATE TABLE impart_pk_state(user_id TEXT PRIMARY KEY,pk_num INTEGER,win_num INTEGER)')
                conn.execute("INSERT INTO impart_pk_state VALUES('a',1,0)")
                conn.execute('CREATE TABLE impart_project_members(user_id TEXT PRIMARY KEY,joined_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)')
                conn.execute("INSERT INTO impart_project_members(user_id) VALUES('a')")

            repo=ImpartBattleBatchSqlRepository(impart,player)
            failed=repo.settle('stale','a',2,0,1,10)
            with db_backend.transaction(player) as conn:
                self.assertIsNotNone(conn.execute("SELECT 1 FROM impart_project_members WHERE user_id='a'").fetchone())
            settled=repo.settle('exhaust','a',1,0,1,10)
            self.assertEqual((failed.status,settled.status),('state_changed','applied'))
            with db_backend.transaction(player) as conn:
                self.assertIsNone(conn.execute("SELECT 1 FROM impart_project_members WHERE user_id='a'").fetchone())
