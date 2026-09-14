import unittest
from datetime import datetime, timedelta

from ..domain import decide_tianti_settlement_window


class SettlementWindowTests(unittest.TestCase):
    def test_missing_last_time_initializes_without_gain(self):
        now = datetime(2026, 9, 14, 10)
        result = decide_tianti_settlement_window(last_settlement=None, now=now)
        self.assertEqual((result.status, result.minutes, result.next_settlement_time), ("init", 0, now))

    def test_same_minute_is_empty(self):
        now = datetime(2026, 9, 14, 10)
        result = decide_tianti_settlement_window(last_settlement=now - timedelta(seconds=59), now=now)
        self.assertEqual((result.status, result.minutes), ("empty", 0))

    def test_elapsed_minutes_are_floor_divided(self):
        now = datetime(2026, 9, 14, 10, 5, 59)
        result = decide_tianti_settlement_window(last_settlement=datetime(2026, 9, 14, 10), now=now)
        self.assertEqual((result.status, result.minutes, result.next_settlement_time), ("settle", 5, now))


if __name__ == "__main__":
    unittest.main()
