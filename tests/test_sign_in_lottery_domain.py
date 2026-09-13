from __future__ import annotations

import unittest

from nonebot_plugin_xiuxian_2.features.sign_in.domain import lottery_prize, lottery_tier


class SignInLotteryDomainTests(unittest.TestCase):
    def test_lottery_tier_is_pure_and_deterministic(self) -> None:
        self.assertEqual(lottery_tier(6), "grand")
        self.assertEqual(lottery_tier(666), "grand")
        self.assertEqual(lottery_tier(1666), "first")
        self.assertEqual(lottery_tier(166), "second")
        self.assertEqual(lottery_tier(16), "third")
        self.assertEqual(lottery_tier(12345), "none")

    def test_lottery_prize_uses_pool_ratios(self) -> None:
        self.assertEqual(lottery_prize(1_000_000, "grand"), 1_000_000)
        self.assertEqual(lottery_prize(1_000_000, "first"), 100_000)
        self.assertEqual(lottery_prize(1_000_000, "second"), 10_000)
        self.assertEqual(lottery_prize(1_000_000, "third"), 1_000)
        self.assertEqual(lottery_prize(1_000_000, "none"), 0)


if __name__ == "__main__":
    unittest.main()
