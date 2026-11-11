from __future__ import annotations

import tempfile
import unittest
from datetime import datetime, timezone
from contextlib import ExitStack
from pathlib import Path

from nonebot_plugin_xiuxian_2.core.errors import OperationConflictError
from nonebot_plugin_xiuxian_2.features.stone_gift.application import StoneGiftApplication
from nonebot_plugin_xiuxian_2.features.stone_gift.migrations import apply_stone_gift, apply_stone_gift_limits
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork
from nonebot_plugin_xiuxian_2.infrastructure.database.ledger import request_hash
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

    def test_replay_ignores_changed_daily_snapshots_and_calendar(self):
        request = dict(operation_id="dynamic", sender_id="sender", recipient_id="recipient", gross_amount=500)
        first = self.application.transfer(**request, transfer_date="2026-09-13", send_limit=1000, send_used=0)
        replay = self.application.transfer(**request, transfer_date="2026-09-14", send_limit=0, send_used=500)
        self.assertTrue(first.ok)
        self.assertTrue(replay.replayed)
        self.assertEqual(first.data, replay.data)
        self.assertEqual((500, 550), self._balances())

    def test_historical_hash_and_interrupted_ledger_recover_committed_gift(self):
        request = dict(operation_id="historical", sender_id="sender", recipient_id="recipient", gross_amount=500)
        first = self.application.transfer(**request, send_limit=1000, send_used=0)
        payload = dict(sender_id="sender", recipient_id="recipient", gross_amount=500,
                       fee_rate=0.1, transfer_date="2026-09-13", send_limit=1000,
                       receive_limit=None, send_used=0, receive_used=None)
        with DatabaseUnitOfWork(self.database) as uow:
            uow.execute("UPDATE operation_ledger SET request_hash=?,status='started',result_json=NULL WHERE operation_id='historical'",
                        (request_hash(payload),))
        replay = self.application.transfer(**request, send_limit=0, send_used=500)
        self.assertTrue(replay.replayed)
        self.assertEqual(first.data, replay.data)
        self.assertEqual((500, 550), self._balances())
        for changes in ({"gross_amount": 501}, {"sender_id": "other"}, {"recipient_id": "other"}):
            with self.subTest(changes=changes), self.assertRaises(OperationConflictError):
                self.application.transfer(**{**request, **changes})
        self.assertEqual((500, 550), self._balances())

    def test_known_write_rejections_roll_back_balances_limits_and_receipts(self):
        for table, condition, reason in (
            ("stone_gift_limits", "NEW.user_id='sender'", "gift_send_limit_write_rejected"),
            ("stone_gift_limits", "NEW.user_id='recipient'", "gift_receive_limit_write_rejected"),
            ("stone_gift_operations", "1", "gift_receipt_write_rejected"),
        ):
            with self.subTest(table=table, condition=condition), ExitStack() as cleanup:
                with DatabaseUnitOfWork(self.database) as uow:
                    uow.execute(f"CREATE TRIGGER reject_gift BEFORE INSERT ON {table} WHEN {condition} BEGIN SELECT RAISE(IGNORE); END")
                cleanup.callback(self._drop_rejection_trigger)
                result = self.application.transfer(operation_id=reason, sender_id="sender", recipient_id="recipient", gross_amount=500)
                self.assertFalse(result.ok)
                self.assertEqual(reason, result.data["reason"])
                self.assertEqual((1000, 100), self._balances())
                with DatabaseUnitOfWork(self.database) as uow:
                    self.assertEqual(0, uow.query_one("SELECT COUNT(*) AS n FROM stone_gift_limits")["n"])
                    self.assertEqual(0, uow.query_one("SELECT COUNT(*) AS n FROM stone_gift_operations")["n"])
                    frozen = self.application.ledger.get(uow, reason, self.application.action).outcome()
                    self.assertEqual(reason, frozen.data["reason"])

    def test_exact_integer_balances_and_very_large_text_amount(self):
        from nonebot_plugin_xiuxian_2.features.stone_gift.domain import calculate_amounts
        self.assertEqual(10 ** 349, calculate_amounts(10 ** 350, 0.1)[1])
        for amount, balance, text_storage in ((101, 2 ** 60 + 123, False), (101, 10 ** 351, True)):
            with self.subTest(text_storage=text_storage):
                with DatabaseUnitOfWork(self.database) as uow:
                    if text_storage:
                        uow.execute("DROP TABLE user_xiuxian")
                        uow.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,user_name TEXT,level TEXT,stone TEXT)")
                        uow.execute("INSERT INTO user_xiuxian VALUES ('sender','甲','江湖好手',?),('recipient','乙','江湖好手',100)", (balance,))
                    else:
                        uow.execute("UPDATE user_xiuxian SET stone=? WHERE user_id='sender'", (balance,))
                result = self.application.transfer(operation_id=f"exact-{text_storage}", sender_id="sender",
                                                   recipient_id="recipient", gross_amount=amount)
                self.assertTrue(result.ok, result)
                net, fee = calculate_amounts(amount, 0.1)
                self.assertEqual(amount // 10, fee)
                self.assertEqual((balance - amount, 100 + net), self._balances())

    def _drop_rejection_trigger(self):
        with DatabaseUnitOfWork(self.database) as uow:
            uow.execute("DROP TRIGGER IF EXISTS reject_gift")


if __name__ == "__main__":
    unittest.main()
