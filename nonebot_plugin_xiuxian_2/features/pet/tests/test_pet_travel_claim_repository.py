import json
import tempfile
import unittest
from pathlib import Path

from ..repository import PetTravelClaimSqlRepository
from ....infrastructure.database import DatabaseUnitOfWork


class PetTravelClaimSqlRepositoryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        root = Path(self.temp.name)
        self.game = root / "game.db"
        self.player = root / "player.db"
        self.travel = {"pet_uid": "p", "start_at": 1, "end_at": 2}
        with DatabaseUnitOfWork(self.game) as uow:
            uow.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY, stone INTEGER, exp INTEGER)")
            uow.execute("INSERT INTO user_xiuxian VALUES('u',100,200)")
            uow.execute("CREATE TABLE back(user_id TEXT, goods_id INTEGER, goods_name TEXT, goods_type TEXT, goods_num INTEGER, bind_num INTEGER, PRIMARY KEY(user_id,goods_id))")
        with DatabaseUnitOfWork(self.player) as uow:
            uow.execute("CREATE TABLE player_pet_item(user_id TEXT, uid TEXT, total_exp INTEGER)")
            uow.execute("INSERT INTO player_pet_item VALUES('u','p',0)")
            uow.execute("CREATE TABLE player_pet(user_id TEXT PRIMARY KEY, travel TEXT)")
            uow.execute("INSERT INTO player_pet VALUES('u',?)", (json.dumps(self.travel, ensure_ascii=False),))

    def tearDown(self) -> None:
        self.temp.cleanup()

    def test_claim_and_replay(self) -> None:
        repository = PetTravelClaimSqlRepository(self.game, self.player)
        rewards = [{"id": 1, "name": "item", "type": "type", "amount": 2}]
        first = repository.claim("op", "u", self.travel, 10, 20, rewards, 99)
        replay = repository.claim("op", "u", self.travel, 10, 20, rewards, 99)
        self.assertEqual((first.status, replay.status, first.items), ("applied", "duplicate", ((1, 2),)))
        with DatabaseUnitOfWork(self.game) as uow:
            self.assertEqual(uow.query_one("SELECT stone,exp FROM user_xiuxian WHERE user_id='u'")["stone"], 110)

    def test_stale_and_inventory_full_preserve_state(self) -> None:
        repository = PetTravelClaimSqlRepository(self.game, self.player)
        rewards = [{"id": 1, "name": "item", "type": "type", "amount": 2}]
        self.assertEqual(repository.claim("stale", "u", {**self.travel, "end_at": 3}, 10, 20, rewards, 99).status, "state_changed")
        with DatabaseUnitOfWork(self.game) as uow:
            uow.execute("INSERT INTO back VALUES('u',1,'item','type',99,99)")
        self.assertEqual(repository.claim("full", "u", self.travel, 10, 20, rewards, 99).status, "inventory_full")

    def test_rollback_trigger_preserves_assets(self) -> None:
        with DatabaseUnitOfWork(self.game) as uow:
            uow.execute("CREATE TABLE pet_travel_claim_operations(operation_id TEXT PRIMARY KEY,payload TEXT,created_at TEXT)")
            uow.execute("CREATE TRIGGER fail_claim BEFORE INSERT ON pet_travel_claim_operations BEGIN SELECT RAISE(ABORT, 'failed'); END")
        with self.assertRaises(Exception):
            PetTravelClaimSqlRepository(self.game, self.player).claim("rollback", "u", self.travel, 10, 20, [], 99)
        with DatabaseUnitOfWork(self.game) as uow:
            self.assertEqual(uow.query_one("SELECT stone,exp FROM user_xiuxian WHERE user_id='u'")["stone"], 100)
