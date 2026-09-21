from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from ....infrastructure.database import DatabaseUnitOfWork
from ....plugin import apply_platform_schema
from ..application import PetApplication


class _Repository:
    def __init__(self) -> None:
        self.calls: list[str] = []

    def travel_claim(self, *args, **kwargs):
        self.calls.append("claim")
        return {"status": "applied", "stone": 10, "exp": 20, "items": [(1, 2)]}

    def travel_start(self, *args, **kwargs):
        self.calls.append("start")
        return {"status": "applied"}

    def feed(self, *args, **kwargs):
        self.calls.append("feed")
        return {"status": "applied", "stars": 1, "exp": 2, "total_exp": 3}

    def hatch(self, *args, **kwargs):
        self.calls.append("hatch")
        return {"status": "applied", "cost": 10, "pets": [], "updated_meta": [], "bag_limit": 99}

    def hatch_result(self, *args, **kwargs):
        return None


class PetApplicationTests(unittest.TestCase):
    def test_claim_and_feed_are_idempotent(self):
        with tempfile.TemporaryDirectory() as directory:
            with DatabaseUnitOfWork(Path(directory) / "game.db") as uow:
                apply_platform_schema(uow)
            repository = _Repository()
            app = PetApplication(Path(directory) / "game.db", Path(directory) / "player.db", repository=repository)
            claim = dict(operation_id="pet-claim-1", user_id="u", expected_travel={"pet_uid": "p"}, stone=10, exp=20, items=[], max_goods_num=99)
            first = app.claim_travel(**claim)
            replay = app.claim_travel(**claim)
            self.assertTrue(first.ok and replay.replayed)
            feed = app.feed(operation_id="pet-feed-1", user_id="u", uid="p", item_id=1, count=1, expected=(0, 0, 0), updated=(1, 2, 3))
            self.assertTrue(feed.ok)
            self.assertEqual(repository.calls, ["claim", "feed"])

    def test_invalid_hatch_does_not_call_repository(self):
        with tempfile.TemporaryDirectory() as directory:
            repository = _Repository()
            app = PetApplication(Path(directory) / "game.db", Path(directory) / "player.db", repository=repository)
            with self.assertRaises(Exception):
                app.hatch(operation_id="", user_id="u", expected_stone=1, cost=1, expected_meta=[], pets=[], updated_meta=[], bag_limit=1)
            self.assertEqual(repository.calls, [])

    def test_default_switch_uses_feature_repository(self):
        with tempfile.TemporaryDirectory() as directory:
            player = Path(directory) / "player.db"
            with DatabaseUnitOfWork(player) as uow:
                uow.execute("CREATE TABLE player_pet(user_id TEXT PRIMARY KEY, active_uid TEXT, active TEXT)")
                uow.execute("INSERT INTO player_pet VALUES('u','old','old')")
                uow.execute("CREATE TABLE player_pet_item(user_id TEXT, uid TEXT, is_active INTEGER, updated_at INTEGER)")
                uow.execute("INSERT INTO player_pet_item VALUES('u','old',1,0)")
                uow.execute("INSERT INTO player_pet_item VALUES('u','new',0,0)")
            result = PetApplication(Path(directory) / "game.db", player).switch(
                operation_id="pet-switch-1", user_id="u", expected_active_uid="old", target_uid="new"
            )
            replay = PetApplication(Path(directory) / "game.db", player).switch(
                operation_id="pet-switch-1", user_id="u", expected_active_uid="old", target_uid="new"
            )
            self.assertEqual((result.status, replay.status), ("applied", "duplicate"))

    def test_default_feed_uses_feature_repository(self):
        with tempfile.TemporaryDirectory() as directory:
            game = Path(directory) / "game.db"
            player = Path(directory) / "player.db"
            with DatabaseUnitOfWork(game) as uow:
                apply_platform_schema(uow)
                uow.execute("CREATE TABLE back(user_id TEXT, goods_id INTEGER, goods_num INTEGER, PRIMARY KEY(user_id,goods_id))")
                uow.execute("INSERT INTO back VALUES('u',1,2)")
            with DatabaseUnitOfWork(player) as uow:
                uow.execute("CREATE TABLE player_pet_item(user_id TEXT, uid TEXT, stars INTEGER, exp INTEGER, total_exp INTEGER, is_active INTEGER, updated_at INTEGER, PRIMARY KEY(user_id,uid))")
                uow.execute("INSERT INTO player_pet_item VALUES('u','p',1,2,3,1,0)")
            app = PetApplication(game, player)
            result = app.feed(operation_id="pet-feed-default", user_id="u", uid="p", item_id=1, count=1, expected=(1, 2, 3), updated=(2, 4, 6))
            replay = app.feed(operation_id="pet-feed-default", user_id="u", uid="p", item_id=1, count=1, expected=(1, 2, 3), updated=(2, 4, 6))
            self.assertEqual((result.status, replay.status), ("applied", "replayed"))

    def test_default_travel_start_uses_feature_repository(self):
        with tempfile.TemporaryDirectory() as directory:
            game = Path(directory) / "game.db"
            player = Path(directory) / "player.db"
            with DatabaseUnitOfWork(game) as uow:
                apply_platform_schema(uow)
            with DatabaseUnitOfWork(player) as uow:
                uow.execute("CREATE TABLE player_pet(user_id TEXT PRIMARY KEY, travel TEXT)")
                uow.execute("INSERT INTO player_pet VALUES('u',NULL)")
                uow.execute("CREATE TABLE player_pet_item(user_id TEXT, uid TEXT, is_active INTEGER)")
                uow.execute("INSERT INTO player_pet_item VALUES('u','p',1)")
            app = PetApplication(game, player)
            travel = {"pet_uid": "p", "start_at": 1}
            result = app.start_travel(operation_id="pet-travel-default", user_id="u", pet_uid="p", expected_travel=None, travel=travel)
            replay = app.start_travel(operation_id="pet-travel-default", user_id="u", pet_uid="p", expected_travel=None, travel=travel)
            self.assertEqual((result.status, replay.status), ("applied", "replayed"))


if __name__ == "__main__":
    unittest.main()
