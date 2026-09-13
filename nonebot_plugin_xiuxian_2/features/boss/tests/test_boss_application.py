from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from ..application import BossApplication


class _Repository:
    def __init__(self, status: str = "applied") -> None:
        self.status = status
        self.calls: list[str] = []

    def purchase(self, *args, **kwargs):
        self.calls.append("purchase")
        return {"status": self.status, "quantity": 1, "cost": 10, "integral": 90, "purchased": 1, "inventory": 1}

    def settle(self, *args, **kwargs):
        self.calls.append("settle")
        return {"status": self.status, "boss_hp": 0, "stamina": 9, "battle_count": 1, "stone": 10, "exp": 20, "integral": 30, "activity_lines": []}


class BossApplicationTests(unittest.TestCase):
    def test_purchase_is_idempotent(self):
        with tempfile.TemporaryDirectory() as directory:
            repository = _Repository()
            app = BossApplication(Path(directory) / "game.db", Path(directory) / "player.db", repository=repository)
            request = dict(operation_id="boss-buy-1", user_id="u", item_id=1, item_name="灵草", item_type="药材", quantity=1, unit_cost=10, weekly_limit=2, expected_integral=100, expected_weekly_purchases={}, max_goods_num=99)
            first = app.purchase(**request)
            replay = app.purchase(**request)
            self.assertTrue(first.ok and replay.replayed)
            self.assertEqual(repository.calls, ["purchase"])

    def test_rejected_purchase_is_recorded(self):
        with tempfile.TemporaryDirectory() as directory:
            repository = _Repository("integral_insufficient")
            app = BossApplication(Path(directory) / "game.db", Path(directory) / "player.db", repository=repository)
            request = dict(operation_id="boss-buy-reject", user_id="u", item_id=1, item_name="灵草", item_type="药材", quantity=1, unit_cost=10, weekly_limit=2, expected_integral=100, expected_weekly_purchases={}, max_goods_num=99)
            result = app.purchase(**request)
            replay = app.purchase(**request)
            self.assertFalse(result.ok)
            self.assertEqual(result.code, "integral_insufficient")
            self.assertFalse(replay.ok)
            self.assertEqual(repository.calls, ["purchase"])

    def test_settlement_requires_operation_and_replays(self):
        with tempfile.TemporaryDirectory() as directory:
            repository = _Repository()
            app = BossApplication(Path(directory) / "game.db", Path(directory) / "player.db", repository=repository)
            request = dict(operation_id="boss-settle-1", user_id="u", expected_bosses=[{"气血": 10}], settled_bosses=[{"气血": 0}], boss_index=0, expected_stamina=10, stamina_cost=1, expected_hp=100, expected_mp=20, final_hp=90, final_mp=20, expected_exp=1, exp_reward=2, expected_stone=3, stone_reward=4, expected_daily_stone=0, expected_daily_integral=0, expected_total_integral=0, integral_reward=1, expected_battle_count=0, battle_limit=3, expected_checked_at="2026-09-12", checked_at="2026-09-12", item=None, max_goods_num=99, actual_damage=10, killed=True, daily_period="2026-09-12", weekly_period="2026-W37")
            first = app.settle(**request)
            replay = app.settle(**request)
            self.assertTrue(first.ok and replay.replayed)
            self.assertEqual(repository.calls, ["settle"])


if __name__ == "__main__":
    unittest.main()
