from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from ....core.errors import DomainError
from ..application import BossApplication
from ....infrastructure.database import DatabaseUnitOfWork, OperationLedger
from ....xiuxian.command_guard import result_failure


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
    @staticmethod
    def _application(directory, repository):
        game_database = Path(directory) / "game.db"
        with DatabaseUnitOfWork(game_database) as uow:
            OperationLedger().ensure_schema(uow)
        return BossApplication(
            game_database,
            Path(directory) / "player.db",
            repository=repository,
        )

    def test_purchase_is_idempotent(self):
        with tempfile.TemporaryDirectory() as directory:
            repository = _Repository()
            app = self._application(directory, repository)
            request = dict(operation_id="boss-buy-1", user_id="u", item_id=1, item_name="灵草", item_type="药材", quantity=1, unit_cost=10, weekly_limit=2, expected_integral=100, expected_weekly_purchases={}, max_goods_num=99)
            first = app.purchase(**request)
            replay = app.purchase(**request)
            self.assertTrue(first.ok and replay.replayed)
            self.assertEqual(repository.calls, ["purchase"])

    def test_rejected_purchase_is_recorded(self):
        with tempfile.TemporaryDirectory() as directory:
            repository = _Repository("integral_insufficient")
            app = self._application(directory, repository)
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
            app = self._application(directory, repository)
            request = dict(operation_id="boss-settle-1", user_id="u", expected_bosses=[{"气血": 10}], settled_bosses=[{"气血": 0}], boss_index=0, expected_stamina=10, stamina_cost=1, expected_hp=100, expected_mp=20, final_hp=90, final_mp=20, expected_exp=1, exp_reward=2, expected_stone=3, stone_reward=4, expected_daily_stone=0, expected_daily_integral=0, expected_total_integral=0, integral_reward=1, expected_battle_count=0, battle_limit=3, expected_checked_at="2026-09-12", checked_at="2026-09-12", item=None, max_goods_num=99, actual_damage=10, killed=True, daily_period="2026-09-12", weekly_period="2026-W37")
            first = app.settle(**request)
            replay = app.settle(**request)
            self.assertTrue(first.ok and replay.replayed)
            self.assertEqual(repository.calls, ["settle"])

    def test_settlement_write_rejection_preserves_reason_and_releases_retry(self):
        class RejectOnceRepository(_Repository):
            def settle(self, *args, **kwargs):
                self.calls.append("settle")
                if self.calls.count("settle") == 1:
                    raise DomainError(
                        "boss_battle_inventory_write_rejected",
                        "inventory write did not persist",
                        {"reason": "boss_battle_inventory_write_rejected", "subject": "u"},
                    )
                return {
                    "status": "applied", "boss_hp": 0, "stamina": 9,
                    "battle_count": 1, "stone": 10, "exp": 20, "integral": 30,
                }

        with tempfile.TemporaryDirectory() as directory:
            repository = RejectOnceRepository()
            app = self._application(directory, repository)
            request = dict(
                operation_id="boss-settle-write-retry", user_id="u",
                expected_bosses=[{"气血": 10}], settled_bosses=[{"气血": 0}], boss_index=0,
                expected_stamina=10, stamina_cost=1, expected_hp=100, expected_mp=20,
                final_hp=90, final_mp=20, expected_exp=1, exp_reward=2,
                expected_stone=3, stone_reward=4, expected_daily_stone=0,
                expected_daily_integral=0, expected_total_integral=0, integral_reward=1,
                expected_battle_count=0, battle_limit=3, expected_checked_at="old",
                checked_at="new", item=None, max_goods_num=99, actual_damage=10,
                killed=True, daily_period="2026-09-12", weekly_period="2026-W37",
            )

            rejected = app.settle_compat(**request)
            self.assertEqual(rejected.status, "state_changed")
            self.assertEqual(rejected.reason, "boss_battle_inventory_write_rejected")
            self.assertEqual(rejected.subject, "u")
            self.assertIn("掉落物未完整入库", result_failure("世界BOSS讨伐", rejected))

            applied = app.settle(**request)
            self.assertTrue(applied.ok)
            replay = app.settle(**request)
            self.assertTrue(replay.replayed)
            self.assertEqual(repository.calls, ["settle", "settle"])


if __name__ == "__main__":
    unittest.main()
