import tempfile
import unittest
from pathlib import Path

from ..repository import PetTravelStartSqlRepository
from ....infrastructure.database import DatabaseUnitOfWork


class PetTravelStartSqlRepositoryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.database = Path(self.temp.name) / "player.db"
        with DatabaseUnitOfWork(self.database) as uow:
            uow.execute("CREATE TABLE player_pet(user_id TEXT PRIMARY KEY, travel TEXT)")
            uow.execute("INSERT INTO player_pet VALUES('u',NULL)")
            uow.execute("CREATE TABLE player_pet_item(user_id TEXT, uid TEXT, is_active INTEGER)")
            uow.execute("INSERT INTO player_pet_item VALUES('u','p',1)")
        self.travel = {"pet_uid": "p", "start_at": 1}

    def tearDown(self) -> None:
        self.temp.cleanup()

    def test_start_and_replay(self) -> None:
        repository = PetTravelStartSqlRepository(self.database)
        first = repository.start("op", "u", "p", None, self.travel)
        replay = repository.start("op", "u", "p", None, self.travel)
        self.assertEqual((first.status, replay.status), ("applied", "duplicate"))
        with DatabaseUnitOfWork(self.database) as uow:
            self.assertIsNotNone(uow.query_one("SELECT travel FROM player_pet WHERE user_id='u'" )["travel"])

    def test_snapshot_and_active_state_rejections(self) -> None:
        repository = PetTravelStartSqlRepository(self.database)
        self.assertEqual(repository.start("stale", "u", "p", {"x": 1}, self.travel).status, "state_changed")
        self.assertEqual(repository.start("missing", "u", "missing", None, self.travel).status, "pet_changed")

    def test_trigger_rolls_back_travel(self) -> None:
        with DatabaseUnitOfWork(self.database) as uow:
            uow.execute("CREATE TABLE pet_travel_start_operations(operation_id TEXT PRIMARY KEY,payload TEXT,created_at TEXT)")
            uow.execute("CREATE TRIGGER fail_travel BEFORE INSERT ON pet_travel_start_operations BEGIN SELECT RAISE(ABORT, 'operation failed'); END")
        with self.assertRaises(Exception):
            PetTravelStartSqlRepository(self.database).start("rollback", "u", "p", None, self.travel)
        with DatabaseUnitOfWork(self.database) as uow:
            self.assertIsNone(uow.query_one("SELECT travel FROM player_pet WHERE user_id='u'")["travel"])
