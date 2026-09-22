import tempfile
import unittest
from pathlib import Path

from ..engraving_repository import NatalEngravingSqlRepository
from ....infrastructure.database import DatabaseUnitOfWork


class NatalEngravingSqlRepositoryTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        root = Path(self.temp.name); self.game = root / "game.db"; self.player = root / "player.db"
        with DatabaseUnitOfWork(self.game) as uow:
            uow.execute("CREATE TABLE back(user_id TEXT, goods_id INTEGER, goods_num INTEGER, PRIMARY KEY(user_id,goods_id))")
            uow.execute("INSERT INTO back VALUES('u',20009,2)")
        with DatabaseUnitOfWork(self.player) as uow:
            uow.execute("CREATE TABLE natal_treasure(user_id TEXT PRIMARY KEY,form INTEGER,effect1_type INTEGER,effect1_base_value REAL,effect1_level INTEGER,effect2_type INTEGER,effect2_base_value REAL,effect2_level INTEGER,effect3_type INTEGER,effect3_base_value REAL,effect3_level INTEGER)")
            uow.execute("INSERT INTO natal_treasure VALUES('u',1,1,0.1,1,0,0,0,0,0,0)")

    def tearDown(self): self.temp.cleanup()

    def test_engrave_and_replay(self):
        result = NatalEngravingSqlRepository(self.game, self.player).engrave('op','u',20009,1,3,{1:(0.1,0.2),2:(0.3,0.4)},{2},7)
        replay = NatalEngravingSqlRepository(self.game, self.player).engrave('op','u',20009,1,3,{1:(0.1,0.2),2:(0.3,0.4)},{2},7)
        self.assertEqual((result['status'], replay['status']), ('engraved','duplicate'))

    def test_item_missing(self):
        with DatabaseUnitOfWork(self.game) as uow: uow.execute("UPDATE back SET goods_num=0 WHERE user_id='u'")
        self.assertEqual(NatalEngravingSqlRepository(self.game, self.player).engrave('missing','u',20009,1,3,{1:(0.1,0.2)},{1},7)['status'], 'item_insufficient')
