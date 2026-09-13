from __future__ import annotations

import unittest
from datetime import datetime, timezone

from nonebot_plugin_xiuxian_2.features.bank.clock import bank_clock, reset_bank_clock, set_bank_clock


class FixedClock:
    def now(self) -> datetime:
        return datetime(2026, 9, 14, tzinfo=timezone.utc)


class BankClockContextTests(unittest.TestCase):
    def test_runtime_clock_is_scoped_and_reset(self) -> None:
        token = set_bank_clock(FixedClock())
        try:
            self.assertEqual(bank_clock().now(), datetime(2026, 9, 14, tzinfo=timezone.utc))
        finally:
            reset_bank_clock(token)
        self.assertNotEqual(bank_clock().now(), datetime(2026, 9, 14, tzinfo=timezone.utc))


if __name__ == "__main__":
    unittest.main()
