import tempfile
import unittest
from pathlib import Path

from ..repository import PetFeedSqlRepository
from ....infrastructure.database import DatabaseUnitOfWork


class PetFeedSqlRepositoryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        root = Path(self.temp.name)
        self.game = root / "game.db"
        self.player = root / "player.db"
        with DatabaseUnitOfWork(self.game) as uow:
            uow.execute("CREATE TABLE back(user_id TEXT, goods_id INTEGER, goods_num INTEGER, PRIMARY KEY(user_id,goods_id))")
            uow.execute("INSERT INTO back VALUES('u',1,3)")
        with DatabaseUnitOfWork(self.player) as uow:
            uow.execute("CREATE TABLE player_pet_item(user_id TEXT, uid TEXT, stars INTEGER, exp INTEGER, total_exp INTEGER, is_active INTEGER, updated_at INTEGER, PRIMARY KEY(user_id,uid))")
            uow.execute("INSERT INTO player_pet_item VALUES('u','p',1,2,3,1,0)")

    def tearDown(self) -> None:
        self.temp.cleanup()

    def test_feed_and_replay(self) -> None:
        repository = PetFeedSqlRepository(self.game, self.player)
        first = repository.feed("op", "u", "p", 1, 1, (1, 2, 3), (2, 4, 6))
        replay = repository.feed("op", "u", "p", 1, 1, (1, 2, 3), (2, 4, 6))
        self.assertEqual((first.status, first.stars, first.exp, first.total_exp), ("applied", 2, 4, 6))
        self.assertEqual((replay.status, replay.stars, replay.exp, replay.total_exp), ("duplicate", 2, 4, 6))
        with DatabaseUnitOfWork(self.game) as uow:
            self.assertEqual(uow.query_one("SELECT goods_num FROM back WHERE user_id='u' AND goods_id=1")["goods_num"], 2)

    def test_rejects_item_and_state_without_mutation(self) -> None:
        repository = PetFeedSqlRepository(self.game, self.player)
        self.assertEqual(repository.feed("missing", "u", "p", 9, 1, (1, 2, 3), (2, 4, 6)).status, "item_missing")
        self.assertEqual(repository.feed("stale", "u", "p", 1, 1, (9, 9, 9), (2, 4, 6)).status, "state_changed")
        with DatabaseUnitOfWork(self.game) as uow:
            self.assertEqual(uow.query_one("SELECT goods_num FROM back WHERE user_id='u' AND goods_id=1")["goods_num"], 3)

    def test_operation_trigger_rolls_back(self) -> None:
        with DatabaseUnitOfWork(self.game) as uow:
            uow.execute("CREATE TABLE pet_feed_operations(operation_id TEXT PRIMARY KEY,payload TEXT,stars INTEGER,exp INTEGER,total_exp INTEGER)")
            uow.execute("CREATE TRIGGER fail_pet_feed BEFORE INSERT ON pet_feed_operations BEGIN SELECT RAISE(ABORT, 'operation failed'); END")
        with self.assertRaises(Exception):
            PetFeedSqlRepository(self.game, self.player).feed("rollback", "u", "p", 1, 1, (1, 2, 3), (2, 4, 6))
        with DatabaseUnitOfWork(self.game) as uow:
            self.assertEqual(uow.query_one("SELECT goods_num FROM back WHERE user_id='u' AND goods_id=1")["goods_num"], 3)
