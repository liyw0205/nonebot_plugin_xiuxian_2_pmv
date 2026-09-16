from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from nonebot_plugin_xiuxian_2.features.combat_settlement.application import CombatSettlementApplication
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork
from nonebot_plugin_xiuxian_2.plugin import apply_platform_schema


class _Repository:
    def __init__(self, status: str = "applied") -> None:
        self.status = status
        self.calls = 0

    def settle(self, operation_id, user_id, expected_daily, snapshot, daily_limit, stone, items, max_goods_num):
        self.calls += 1
        return {"status": self.status, "stone": stone, "rewards": ((1, 2),)}


class CombatSettlementApplicationTests(unittest.TestCase):
    @staticmethod
    def _application(directory: str, repository: _Repository) -> CombatSettlementApplication:
        game = Path(directory) / "game.db"
        player = Path(directory) / "player.db"
        for database in (game, player):
            with DatabaseUnitOfWork(database) as uow:
                apply_platform_schema(uow)
        return CombatSettlementApplication(game, player, repository=repository)

    def _call(self, app, operation_id="combat-1"):
        return app.settle(
            operation_id=operation_id,
            user_id="user-1",
            expected_daily={"date": "2026-09-12", "combat_count": "1", "resource_total_count": "2"},
            snapshot='{"task_id":"combat-1"}',
            daily_limit=4,
            stone=10,
            items=({"id": 1, "name": "材料", "type": "材料", "amount": 2},),
            max_goods_num=99,
        )

    def test_settlement_is_audited_and_idempotent(self):
        with tempfile.TemporaryDirectory() as directory:
            repository = _Repository()
            app = self._application(directory, repository)
            first = self._call(app)
            second = self._call(app)
            self.assertTrue(first.ok)
            self.assertTrue(second.replayed)
            self.assertEqual(repository.calls, 1)
            with DatabaseUnitOfWork(Path(directory) / "game.db") as uow:
                ledger = uow.query_one("SELECT status FROM operation_ledger WHERE operation_id=?", ("combat-1",))
                audit = uow.query_one("SELECT category FROM operation_audit WHERE operation_id=?", ("combat-1",))
            self.assertEqual(ledger["status"], "applied")
            self.assertEqual(audit["category"], "combat_settlement")

    def test_business_rejection_is_stable(self):
        with tempfile.TemporaryDirectory() as directory:
            repository = _Repository("inventory_full")
            app = self._application(directory, repository)
            result = self._call(app, "combat-2")
            replay = self._call(app, "combat-2")
            self.assertFalse(result.ok)
            self.assertEqual(result.code, "inventory_full")
            self.assertEqual(replay.code, "inventory_full")
            self.assertEqual(repository.calls, 1)

    def test_invalid_request_does_not_call_repository(self):
        with tempfile.TemporaryDirectory() as directory:
            repository = _Repository()
            app = self._application(directory, repository)
            with self.assertRaises(Exception):
                self._call(app, "")
            self.assertEqual(repository.calls, 0)


if __name__ == "__main__":
    unittest.main()
