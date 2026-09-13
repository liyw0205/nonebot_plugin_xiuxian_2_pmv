from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from ..application import SectApplication


class _Repository:
    def __init__(self) -> None:
        self.calls: list[str] = []

    def join(self, *args, **kwargs):
        self.calls.append("join")
        return {"status": "joined", "user_id": "u", "sect_id": 1, "member_count": 2, "member_limit": 20}

    def purchase(self, *args, **kwargs):
        self.calls.append("purchase")
        return {"status": "applied", "quantity": 1, "cost": 10}

    def learn_main(self, *args, **kwargs):
        self.calls.append("main")
        return {"status": "learned", "materials_cost": 10, "materials_left": 90}

    def learn_secondary(self, *args, **kwargs):
        self.calls.append("secondary")
        return {"status": "learned", "materials_cost": 10, "materials_left": 80}

    def claim_elixir(self, *args, **kwargs):
        self.calls.append("elixir")
        return {"status": "applied", "rewards": []}


class SectApplicationTests(unittest.TestCase):
    def test_join_and_purchase_are_idempotent(self):
        with tempfile.TemporaryDirectory() as directory:
            repository = _Repository()
            app = SectApplication(Path(directory) / "game.db", repository=repository)
            first = app.join(operation_id="sect-join-1", user_id="u", sect_id=1)
            replay = app.join(operation_id="sect-join-1", user_id="u", sect_id=1)
            purchase = app.purchase(operation_id="sect-buy-1", user_id="u", sect_id=1, item_id=2, item_name="丹", item_type="丹药", quantity=1, unit_cost=10, weekly_limit=1, legacy_purchased=0, max_goods_num=99)
            self.assertTrue(first.ok and replay.replayed and purchase.ok)
            self.assertEqual(repository.calls, ["join", "purchase"])

    def test_learning_and_claim_use_operation_ids(self):
        with tempfile.TemporaryDirectory() as directory:
            repository = _Repository()
            app = SectApplication(Path(directory) / "game.db", repository=repository)
            self.assertTrue(app.learn_main(operation_id="sect-main-1", user_id="u", sect_id=1, buff_id=2, materials_cost=10, expected_catalog="[]").ok)
            self.assertTrue(app.learn_secondary(operation_id="sect-sec-1", user_id="u", sect_id=1, buff_id=3, materials_cost=10, expected_catalog="[]").ok)
            self.assertTrue(app.claim_elixir(operation_id="sect-elixir-1", user_id="u", sect_id=1, contribution_required=1, materials_required=1, rewards=[], max_goods_num=99).ok)
            self.assertEqual(repository.calls, ["main", "secondary", "elixir"])


if __name__ == "__main__":
    unittest.main()
