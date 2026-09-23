import tempfile
import unittest
from pathlib import Path
from ..project_join_repository import ImpartProjectJoinSqlRepository
from tests.test_db_backend import db_backend

class ImpartProjectJoinRepositoryTests(unittest.TestCase):
    def test_join_replay_and_capacity_guard(self):
        with tempfile.TemporaryDirectory() as temp:
            db=Path(temp)/'player.db'; repo=ImpartProjectJoinSqlRepository(db,capacity=1); first=repo.join('j','u',legacy_pk_num=7,legacy_members=[]); dup=repo.join('j','u',legacy_pk_num=7,legacy_members=[]); other=repo.join('k','v',legacy_pk_num=7,legacy_members=[]); self.assertEqual((first.status,dup.status,other.status),('applied','duplicate','capacity_full'))
