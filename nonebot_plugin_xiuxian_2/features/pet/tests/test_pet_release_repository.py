import tempfile
import unittest
from pathlib import Path

from ..repository import PetReleaseSqlRepository
from ....infrastructure.database import DatabaseUnitOfWork


class PetReleaseSqlRepositoryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        root = Path(self.temp.name)
        self.game = root / "game.db"
        self.player = root / "player.db"
        with DatabaseUnitOfWork(self.game) as uow:
            uow.execute("CREATE TABLE back(user_id TEXT, goods_id INTEGER, goods_name TEXT, goods_type TEXT, goods_num INTEGER, bind_num INTEGER, PRIMARY KEY(user_id,goods_id))")
        with DatabaseUnitOfWork(self.player) as uow:
            uow.execute("CREATE TABLE player_pet(user_id TEXT PRIMARY KEY, active_uid TEXT, active TEXT)")
            uow.execute("INSERT INTO player_pet VALUES('u','active','active')")
            uow.execute("CREATE TABLE player_pet_item(user_id TEXT, uid TEXT, total_exp INTEGER, is_active INTEGER, PRIMARY KEY(user_id,uid))")
            uow.execute("INSERT INTO player_pet_item VALUES('u','active',1000,1)")
            uow.execute("INSERT INTO player_pet_item VALUES('u','bag',2000,0)")

    def tearDown(self) -> None:
        self.temp.cleanup()

    def test_release_and_replay(self) -> None:
        repository = PetReleaseSqlRepository(self.game, self.player)
        first = repository.release("op", "u", "bag", 2000, 9, "灵髓", "特殊道具", 3, 99, expected_is_active=False)
        replay = repository.release("op", "u", "bag", 2000, 9, "灵髓", "特殊道具", 3, 99, expected_is_active=False)
        self.assertEqual((first.status, replay.status, first.refund), ("applied", "duplicate", 3))
        with DatabaseUnitOfWork(self.player) as uow:
            self.assertIsNone(uow.query_one("SELECT uid FROM player_pet_item WHERE user_id='u' AND uid='bag'"))
        with DatabaseUnitOfWork(self.game) as uow:
            self.assertEqual(uow.query_one("SELECT goods_num FROM back WHERE user_id='u' AND goods_id=9")["goods_num"], 3)

    def test_active_and_inventory_rejections_preserve_state(self) -> None:
        repository = PetReleaseSqlRepository(self.game, self.player)
        self.assertEqual(repository.release("active", "u", "active", 1000, 9, "灵髓", "特殊道具", 3, 99, expected_is_active=True).status, "applied")
        self.assertEqual(repository.release("missing", "u", "missing", 0, 9, "灵髓", "特殊道具", 3, 99).status, "state_changed")

    def test_operation_trigger_rolls_back(self) -> None:
        with DatabaseUnitOfWork(self.game) as uow:
            uow.execute("CREATE TABLE pet_release_operations(operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,refund INTEGER NOT NULL,released_uids TEXT NOT NULL,created_at TEXT)")
            uow.execute("CREATE TRIGGER fail_release BEFORE INSERT ON pet_release_operations BEGIN SELECT RAISE(ABORT, 'failed'); END")
        with self.assertRaises(Exception):
            PetReleaseSqlRepository(self.game, self.player).release("rollback", "u", "bag", 2000, 9, "灵髓", "特殊道具", 3, 99, expected_is_active=False)
        with DatabaseUnitOfWork(self.player) as uow:
            self.assertIsNotNone(uow.query_one("SELECT uid FROM player_pet_item WHERE user_id='u' AND uid='bag'"))
