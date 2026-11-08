"""灵庄 interest must not be invented from an empty/corrupt ``savetime``.

Production traceback (xiu2.log 2026-11-07 05:46, 4 occurrences):

    File "xiuxian_bank/__init__.py", line 339, in get_give_stone
    ValueError: time data '' does not match format '%Y-%m-%d %H:%M:%S'

Legacy ``bankinfo`` rows can exist with an empty ``savetime`` (account created,
never settled).  Such a row must settle **zero** interest — never crash the
灵庄 command and never invent interest from a fabricated start time.
"""

from __future__ import annotations

import unittest
from datetime import datetime, timezone

from nonebot_plugin_xiuxian_2.features.bank.interest_rules import calculate_interest


class EmptySettlementTimeTests(unittest.TestCase):
    def test_empty_saved_at_yields_zero_interest(self) -> None:
        self.assertEqual(
            (0, 0.0),
            calculate_interest(
                saved_stone=1_000_000,
                saved_at="",
                settled_at=datetime(2026, 11, 8, 12, 0, tzinfo=timezone.utc),
                rate=0.002,
            ),
        )

    def test_whitespace_saved_at_yields_zero_interest(self) -> None:
        self.assertEqual(
            (0, 0.0),
            calculate_interest(
                saved_stone=5000,
                saved_at="   ",
                settled_at=datetime(2026, 11, 8, 12),
                rate=0.002,
            ),
        )

    def test_unparseable_saved_at_yields_zero_interest(self) -> None:
        for saved_at in ("0", "None", "not-a-time", "2026-13-40 99:99:99"):
            self.assertEqual(
                (0, 0.0),
                calculate_interest(
                    saved_stone=1000,
                    saved_at=saved_at,
                    settled_at=datetime(2026, 11, 8, 12, tzinfo=timezone.utc),
                    rate=0.002,
                ),
                saved_at,
            )

    def test_real_timestamps_still_accrue_interest(self) -> None:
        self.assertEqual(
            (4, 2.0),
            calculate_interest(
                saved_stone=1000,
                saved_at="2026-10-07 10:00:00",
                settled_at=datetime(2026, 10, 7, 12),
                rate=0.002,
            ),
        )

    def test_hour_boundary_uses_existing_centihour_rounding(self) -> None:
        # 1h00m accrued exactly at the boundary.
        self.assertEqual(
            (2, 1.0),
            calculate_interest(
                saved_stone=1000,
                saved_at="2026-10-07 11:00:00",
                settled_at=datetime(2026, 10, 7, 12),
                rate=0.002,
            ),
        )
        # 1h59m59s rounds to 2.00 centihours (existing rounding, unchanged).
        self.assertEqual(
            (4, 2.0),
            calculate_interest(
                saved_stone=1000,
                saved_at="2026-10-07 10:00:01",
                settled_at=datetime(2026, 10, 7, 12),
                rate=0.002,
            ),
        )


if __name__ == "__main__":
    unittest.main()
