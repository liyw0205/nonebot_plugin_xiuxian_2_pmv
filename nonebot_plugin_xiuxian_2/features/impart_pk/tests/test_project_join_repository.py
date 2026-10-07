import tempfile
import unittest
from pathlib import Path
from ..project_join_repository import ImpartProjectJoinSqlRepository
from tests.test_db_backend import db_backend

class ImpartProjectJoinRepositoryTests(unittest.TestCase):
    def test_join_replay_and_capacity_guard(self):
        with tempfile.TemporaryDirectory() as temp:
            db=Path(temp)/'player.db'; repo=ImpartProjectJoinSqlRepository(db,capacity=1); first=repo.join('j','u',legacy_pk_num=7,legacy_members=[]); dup=repo.join('j','u',legacy_pk_num=7,legacy_members=[]); other=repo.join('k','v',legacy_pk_num=7,legacy_members=[]); self.assertEqual((first.status,dup.status,other.status),('applied','duplicate','capacity_full'))

    def test_exhausted_projection_attempt_does_not_join_or_increment_stats(self):
        with tempfile.TemporaryDirectory() as temp:
            db=Path(temp)/'player.db'; repo=ImpartProjectJoinSqlRepository(db)
            with db_backend.transaction(db) as conn:
                conn.execute('CREATE TABLE impart_pk_state(user_id TEXT PRIMARY KEY,pk_num INTEGER NOT NULL,win_num INTEGER NOT NULL DEFAULT 0)')
                conn.execute("INSERT INTO impart_pk_state(user_id,pk_num) VALUES('stored-zero',0)")

            legacy_zero=repo.join('legacy-zero-op','legacy-zero',legacy_pk_num=0,legacy_members=[])
            stored_zero=repo.join('stored-zero-op','stored-zero',legacy_pk_num=7,legacy_members=[])
            self.assertEqual((legacy_zero.status,legacy_zero.pk_num),('pk_exhausted',0))
            self.assertEqual((stored_zero.status,stored_zero.pk_num),('pk_exhausted',0))
            with db_backend.transaction(db) as conn:
                self.assertEqual(conn.execute('SELECT COUNT(*) FROM impart_project_members').fetchone()[0],0)
                self.assertIsNone(conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='statistics'").fetchone())
