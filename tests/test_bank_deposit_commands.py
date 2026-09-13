from __future__ import annotations

import unittest
from datetime import datetime, timezone

from nonebot_plugin_xiuxian_2.features.bank.commands import parse_first_use_deposit


class Clock:
    def now(self) -> datetime:
        return datetime(2026, 9, 13, tzinfo=timezone.utc)


class BankDepositCommandTests(unittest.TestCase):
    def test_parse_uses_injected_clock_and_operation_id(self) -> None:
        command = parse_first_use_deposit(user_id="u1", text="300", operation_id="op-1", clock=Clock(), limit=1000)
        self.assertEqual(command.amount, 300)
        self.assertEqual(command.settled_at, "2026-09-13T00:00:00+00:00")
        self.assertEqual(command.operation_id, "op-1")

    def test_invalid_amount_is_rejected(self) -> None:
        with self.assertRaises(ValueError):
            parse_first_use_deposit(user_id="u1", text="0", operation_id="op-1", clock=Clock(), limit=1000)


if __name__ == "__main__":
    unittest.main()
