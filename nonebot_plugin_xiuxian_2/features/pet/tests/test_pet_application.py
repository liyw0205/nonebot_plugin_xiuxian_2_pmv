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


if __name__ == "__main__":
    unittest.main()
