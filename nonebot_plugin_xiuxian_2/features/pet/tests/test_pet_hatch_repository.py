import tempfile
import unittest
from pathlib import Path

from ..repository import PetHatchSqlRepository
from ..migrations import apply_pet_hatch
from ....infrastructure.database import DatabaseUnitOfWork


class PetHatchSqlRepositoryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        root = Path(self.temp.name)
        self.game = root / "game.db"
        self.player = root / "player.db"
        with DatabaseUnitOfWork(self.game) as uow:
            apply_pet_hatch(uow)
            uow.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY, stone INTEGER)")
            uow.execute("INSERT INTO user_xiuxian VALUES('u',100)")
        with DatabaseUnitOfWork(self.player) as uow:
            uow.execute("CREATE TABLE player_pet(user_id TEXT PRIMARY KEY, active_uid TEXT, egg_pity_count INTEGER, egg_pity_no_mythic_count INTEGER, travel TEXT)")
            uow.execute("INSERT INTO player_pet VALUES('u','',0,0,NULL)")
            uow.execute("CREATE TABLE player_pet_item(id TEXT,user_id TEXT,uid TEXT,is_active INTEGER,pet_id TEXT,stars INTEGER,exp INTEGER,total_exp INTEGER,skill_id TEXT,created_at INTEGER,updated_at INTEGER)")
        self.pet = {"uid": "x", "pet_id": "1", "stars": 1, "exp": 0, "total_exp": 0, "skill": {}}

    def tearDown(self) -> None:
        self.temp.cleanup()

    def test_hatch_and_replay(self) -> None:
        repository = PetHatchSqlRepository(self.game, self.player)
        first = repository.hatch("op", "u", 100, 10, ["", 0, 0, None], [(self.pet, True)], ["x", 1, 0], 10)
        replay = repository.hatch("op", "u", 90, 10, ["", 0, 0, None], [({**self.pet, "uid": "y"}, False)], ["y", 1, 0], 10)
        self.assertEqual((first.status, replay.status, first.cost), ("applied", "duplicate", 10))
        with DatabaseUnitOfWork(self.game) as uow:
            self.assertEqual(uow.query_one("SELECT stone FROM user_xiuxian WHERE user_id='u'")["stone"], 90)

    def test_stone_missing_and_rollback(self) -> None:
        repository = PetHatchSqlRepository(self.game, self.player)
        self.assertEqual(repository.hatch("poor", "u", 5, 10, ["", 0, 0, None], [(self.pet, True)], ["x", 1, 0], 10).status, "state_changed")
        with DatabaseUnitOfWork(self.game) as uow:
            uow.execute("CREATE TRIGGER fail_hatch BEFORE INSERT ON pet_hatch_operations BEGIN SELECT RAISE(ABORT, 'failed'); END")
        with self.assertRaises(Exception):
            repository.hatch("rollback", "u", 100, 10, ["", 0, 0, None], [(self.pet, True)], ["x", 1, 0], 10)
        with DatabaseUnitOfWork(self.game) as uow:
            self.assertEqual(uow.query_one("SELECT stone FROM user_xiuxian WHERE user_id='u'")["stone"], 100)
