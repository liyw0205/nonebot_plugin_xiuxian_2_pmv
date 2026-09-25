from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from ..application import ArenaApplication
from ....infrastructure.database import DatabaseUnitOfWork, OperationLedger


class _Repository:
    def __init__(self) -> None:
        self.calls: list[str] = []

    def purchase(self, *args, **kwargs):
        self.calls.append("purchase")
        return {"status": "applied", "quantity": 2, "cost": 20, "honor_points": 80, "purchased": 2, "inventory": 2}

    def purchase_challenges(self, *args, **kwargs):
        self.calls.append("challenge_purchase")
        return {"status": "applied", "amount": 1, "cost": 200, "stone": 800, "bought": 1, "extra": 1}

    def settle(self, *args, **kwargs):
        self.calls.append("settle")
        return {"status": "applied", "outcome": "win", "challenger_score": 1020, "challenger_rank": "青铜", "score_delta": 20, "used": 1, "remaining": 9, "stamina": 0, "challenged_at": "2026-09-12"}

    def use_challenge_ticket(self, *args, **kwargs):
        self.calls.append("challenge_ticket")
        return {"status": "applied", "used_tickets": 2, "item_remaining": 3, "challenges_used": 0, "challenges_remaining": 12, "challenge_cap": 12}


class ArenaApplicationTests(unittest.TestCase):
    def make_app(self, directory, repository):
        game_database = Path(directory) / "game.db"
        with DatabaseUnitOfWork(game_database) as uow:
            OperationLedger().ensure_schema(uow)
        return ArenaApplication(game_database, Path(directory) / "player.db", repository=repository)

    def test_purchase_and_challenge_purchase_are_idempotent(self):
        with tempfile.TemporaryDirectory() as directory:
            repository = _Repository()
            app = self.make_app(directory, repository)
            purchase = dict(operation_id="arena-buy-1", user_id="u", item_id=1, item_name="灵草", item_type="药材", quantity=2, unit_cost=10, weekly_limit=5, expected_honor=100, expected_weekly_purchases={"_last_reset": "2026-09-12"}, max_goods_num=99)
            first = app.purchase(**purchase)
            replay = app.purchase(**purchase)
            bought = app.purchase_challenges(operation_id="arena-count-1", user_id="u", amount=1, unit_cost=200, daily_limit=3, expected_stone=1000, expected_bought=0, expected_extra=0, expected_last_buy_date="2026-09-12")
            self.assertTrue(first.ok and replay.replayed and bought.ok)
            self.assertEqual(repository.calls, ["purchase", "challenge_purchase"])

    def test_settlement_and_replay(self):
        with tempfile.TemporaryDirectory() as directory:
            repository = _Repository()
            app = self.make_app(directory, repository)
            request = dict(operation_id="arena-settle-1", challenger_id="u", opponent_id=None, outcome="no_match", challenge_cap=10, stamina_cost=0, challenged_at="2026-09-12", expected_challenger_arena={"score": 1000, "daily_challenges_used": 0}, expected_opponent_arena=None, expected_challenger_player={"hp": 100, "mp": 100, "user_stamina": 0}, expected_opponent_player=None, final_challenger_hp=100, final_challenger_mp=100, final_opponent_hp=None, final_opponent_mp=None, win_points=20, lose_points=0, no_match_points=10)
            result = app.settle(**request)
            replay = app.settle(**request)
            self.assertTrue(result.ok and replay.replayed)
            self.assertEqual(repository.calls, ["settle"])

    def test_challenge_ticket_uses_repository_and_replays(self):
        with tempfile.TemporaryDirectory() as directory:
            repository = _Repository()
            app = self.make_app(directory, repository)
            request = dict(
                operation_id="arena-ticket-1",
                user_id="u",
                item_id=20024,
                requested_count=2,
                expected_item_count=5,
                expected_challenges_used=2,
                expected_extra_challenges=4,
                challenge_cap=12,
            )
            first = app.use_challenge_ticket(**request)
            replay = app.use_challenge_ticket(**request)
            self.assertTrue(first.ok and replay.replayed)
            self.assertEqual(repository.calls, ["challenge_ticket"])

    def test_validation_stops_repository(self):
        with tempfile.TemporaryDirectory() as directory:
            repository = _Repository()
            app = self.make_app(directory, repository)
            with self.assertRaises(Exception):
                app.purchase(operation_id="", user_id="u", item_id=1, item_name="x", item_type="y", quantity=1, unit_cost=1, weekly_limit=1, expected_honor=1, expected_weekly_purchases={}, max_goods_num=1)
            self.assertEqual(repository.calls, [])


if __name__ == "__main__":
    unittest.main()
