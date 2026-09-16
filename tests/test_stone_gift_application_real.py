from __future__ import annotations

import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from nonebot_plugin_xiuxian_2.core.errors import OperationConflictError
from nonebot_plugin_xiuxian_2.features.stone_gift.application import StoneGiftApplication
from nonebot_plugin_xiuxian_2.features.stone_gift.migrations import apply_stone_gift, apply_stone_gift_limits
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork
from nonebot_plugin_xiuxian_2.plugin import apply_platform_schema


class FixedClock:
    def now(self) -> datetime:
        return datetime(2026, 9, 13, 0, 0, tzinfo=timezone.utc)


class StoneGiftApplicationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.database = Path(self.temp_dir.name) / "stone-gift.sqlite3"
        with DatabaseUnitOfWork(self.database) as uow:
            uow.execute("CREATE TABLE user_xiuxian (user_id TEXT PRIMARY KEY, user_name TEXT, level TEXT, stone INTEGER)")
            uow.execute("INSERT INTO user_xiuxian VALUES (?, ?, ?, ?)", ("sender", "甲", "江湖好手", 1000))
            uow.execute("INSERT INTO user_xiuxian VALUES (?, ?, ?, ?)", ("recipient", "乙", "江湖好手", 100))
            apply_platform_schema(uow)
            apply_stone_gift(uow)
            apply_stone_gift_limits(uow)
        self.application = StoneGiftApplication(self.database, clock=FixedClock())

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def _balances(self) -> tuple[int, int]:
        with DatabaseUnitOfWork(self.database) as uow:
            rows = uow.query_all("SELECT user_id, stone FROM user_xiuxian ORDER BY user_id")
        values = {str(row["user_id"]): int(row["stone"]) for row in rows}
        return values["sender"], values["recipient"]

    def test_transfer_is_idempotent_and_updates_daily_projection(self) -> None:
        result = self.application.transfer(
            operation_id="new-gift-1",
            sender_id="sender",
            recipient_id="recipient",
            gross_amount=500,
            transfer_date="2026-09-13",
            send_limit=100_000_000,
            receive_limit=100_000_000,
        )
        replay = self.application.transfer(
            operation_id="new-gift-1",
            sender_id="sender",
            recipient_id="recipient",
            gross_amount=500,
            transfer_date="2026-09-13",
            send_limit=100_000_000,
            receive_limit=100_000_000,
        )

        self.assertTrue(result.ok)
        self.assertEqual(replay.status, "replayed")
        self.assertEqual(self._balances(), (500, 550))
        with DatabaseUnitOfWork(self.database) as uow:
            row = uow.query_one(
                "SELECT sent_amount, received_amount FROM stone_gift_limits WHERE limit_date = ? AND user_id = ?",
                ("2026-09-13", "sender"),
            )
        self.assertIsNotNone(row)
        assert row is not None
        self.assertEqual((row["sent_amount"], row["received_amount"]), (500, 0))

    def test_operation_payload_conflict_does_not_change_assets(self) -> None:
        self.application.transfer(
            operation_id="new-gift-conflict",
            sender_id="sender",
            recipient_id="recipient",
            gross_amount=500,
            transfer_date="2026-09-13",
            send_limit=100_000_000,
            receive_limit=100_000_000,
        )

        with self.assertRaises(OperationConflictError):
            self.application.transfer(
                operation_id="new-gift-conflict",
                sender_id="sender",
                recipient_id="recipient",
                gross_amount=900,
                transfer_date="2026-09-13",
                send_limit=100_000_000,
                receive_limit=100_000_000,
            )
        self.assertEqual(self._balances(), (500, 550))

    def test_insufficient_balance_rejects_without_asset_or_operation_projection(self) -> None:
        result = self.application.transfer(
            operation_id="new-gift-poor",
            sender_id="sender",
            recipient_id="recipient",
            gross_amount=1001,
            transfer_date="2026-09-13",
            send_limit=100_000_000,
            receive_limit=100_000_000,
        )

        self.assertFalse(result.ok)
        self.assertEqual(result.code, "stone_insufficient")
        self.assertEqual(self._balances(), (1000, 100))
        with DatabaseUnitOfWork(self.database) as uow:
            count = uow.query_one("SELECT COUNT(*) AS count FROM stone_gift_operations")
        self.assertIsNotNone(count)
        assert count is not None
        self.assertEqual(count["count"], 0)


if __name__ == "__main__":
    unittest.main()
