import tempfile
import unittest
from pathlib import Path

from ..repository import PetSkillRerollSqlRepository
from ....infrastructure.database import DatabaseUnitOfWork


class PetSkillRerollSqlRepositoryTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        root = Path(self.temp.name)
        self.game = root / "game.db"
        self.player = root / "player.db"
        with DatabaseUnitOfWork(self.game) as uow:
            uow.execute("CREATE TABLE back(user_id TEXT, goods_id INTEGER, goods_num INTEGER, PRIMARY KEY(user_id,goods_id))")
            uow.execute("INSERT INTO back VALUES('u',9,2)")
        with DatabaseUnitOfWork(self.player) as uow:
            uow.execute("CREATE TABLE player_pet_item(user_id TEXT, uid TEXT, pet_id TEXT, stars INTEGER, exp INTEGER, total_exp INTEGER, skill_id TEXT, is_active INTEGER, updated_at INTEGER, PRIMARY KEY(user_id,uid))")
            uow.execute("INSERT INTO player_pet_item VALUES('u','p','fox',5,2,50,'old',1,0)")
        self.snapshot = ("p", "fox", 5, 2, 50, "old", 1)

    def tearDown(self):
        self.temp.cleanup()

    def test_reroll_and_replay(self):
        repository = PetSkillRerollSqlRepository(self.game, self.player)
        first = repository.reroll("op", "u", self.snapshot, "new", 9)
        replay = repository.reroll("op", "u", self.snapshot, "new", 9)
        self.assertEqual((first.status, replay.status, first.skill_id), ("applied", "duplicate", "new"))

    def test_item_missing_and_rollback(self):
        repository = PetSkillRerollSqlRepository(self.game, self.player)
        self.assertEqual(repository.reroll("missing", "u", self.snapshot, "new", 99).status, "item_missing")
        with DatabaseUnitOfWork(self.game) as uow:
            uow.execute("CREATE TRIGGER fail_reroll BEFORE INSERT ON pet_skill_reroll_operations BEGIN SELECT RAISE(ABORT, 'failed'); END")
        with self.assertRaises(Exception):
            repository.reroll("rollback", "u", self.snapshot, "new", 9)
        with DatabaseUnitOfWork(self.player) as uow:
            self.assertEqual(uow.query_one("SELECT skill_id FROM player_pet_item WHERE user_id='u' AND uid='p'")["skill_id"], "old")
