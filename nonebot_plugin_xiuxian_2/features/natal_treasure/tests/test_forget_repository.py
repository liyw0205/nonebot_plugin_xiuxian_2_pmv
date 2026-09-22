import tempfile
import unittest
from pathlib import Path

from ..forget_repository import NatalForgetSqlRepository
from ....infrastructure.database import DatabaseUnitOfWork


class NatalForgetSqlRepositoryTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(); root=Path(self.temp.name); self.game=root/'game.db'; self.player=root/'player.db'
        with DatabaseUnitOfWork(self.game) as uow:
            uow.execute("CREATE TABLE back(user_id TEXT,goods_id INTEGER,goods_num INTEGER,PRIMARY KEY(user_id,goods_id))"); uow.execute("INSERT INTO back VALUES('u',20009,2)")
        with DatabaseUnitOfWork(self.player) as uow:
            uow.execute("CREATE TABLE natal_treasure(user_id TEXT PRIMARY KEY,form INTEGER,effect1_type INTEGER,effect1_base_value REAL,effect1_level INTEGER,effect2_type INTEGER,effect2_base_value REAL,effect2_level INTEGER,effect3_type INTEGER,effect3_base_value REAL,effect3_level INTEGER)")
            uow.execute("INSERT INTO natal_treasure VALUES('u',1,1,0.1,1,2,0.2,3,3,0.3,2)")
    def tearDown(self): self.temp.cleanup()
    def test_forget_and_replay(self):
        repo=NatalForgetSqlRepository(self.game,self.player); first=repo.forget('op','u',2,20009,'神秘经书','神物',1,3,1000); replay=repo.forget('op','u',2,20009,'神秘经书','神物',1,3,1000)
        self.assertEqual((first['status'],replay['status'],first['scripture_change']),('forgotten','duplicate',1))
    def test_missing_effect(self):
        self.assertEqual(NatalForgetSqlRepository(self.game,self.player).forget('missing','u',9,20009,'神秘经书','神物',1,3,1000)['status'],'effect_missing')
