import tempfile
import unittest
from pathlib import Path

from ..repository import PetFusionBreakthroughSqlRepository
from ....infrastructure.database import DatabaseUnitOfWork


class PetFusionBreakthroughSqlRepositoryTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.database = Path(self.temp.name) / "player.db"
        with DatabaseUnitOfWork(self.database) as uow:
            uow.execute("CREATE TABLE player_pet_item(user_id TEXT, uid TEXT PRIMARY KEY, pet_id TEXT, stars INTEGER, exp INTEGER, total_exp INTEGER, skill_id TEXT, is_active INTEGER)")
            uow.execute("INSERT INTO player_pet_item VALUES('u','main','fox',4,100,500,'old',1)")
            uow.execute("INSERT INTO player_pet_item VALUES('u','body','fox',1,0,0,'body-skill',0)")
        self.main = ("main", "fox", 4, 100, 500, "old", 1)
        self.materials = [("body", "fox", 1, 0, 0, "body-skill", 0)]

    def tearDown(self):
        self.temp.cleanup()

    def test_breakthrough_and_replay(self):
        repository = PetFusionBreakthroughSqlRepository(self.database)
        first = repository.breakthrough("op", "u", self.main, self.materials, 5, 0)
        replay = repository.breakthrough("op", "u", self.main, self.materials, 5, 0)
        self.assertEqual((first.status, replay.status, first.stars), ("applied", "duplicate", 5))

    def test_state_changed_and_trigger_rollback(self):
        repository = PetFusionBreakthroughSqlRepository(self.database)
        self.assertEqual(repository.breakthrough("stale", "u", ("main", "fox", 3, 100, 500, "old", 1), self.materials, 5, 0).status, "state_changed")
        with DatabaseUnitOfWork(self.database) as uow:
            uow.execute("CREATE TRIGGER fail_fusion BEFORE INSERT ON pet_fusion_breakthrough_operations BEGIN SELECT RAISE(ABORT, 'failed'); END")
        with self.assertRaises(Exception):
            repository.breakthrough("rollback", "u", self.main, self.materials, 5, 0)
        with DatabaseUnitOfWork(self.database) as uow:
            self.assertIsNotNone(uow.query_one("SELECT uid FROM player_pet_item WHERE user_id='u' AND uid='body'"))
