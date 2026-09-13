from __future__ import annotations

import unittest

from nonebot_plugin_xiuxian_2.features.bank.rules import decide_deposit


class BankDepositRulesTests(unittest.TestCase):
    def test_deposit_updates_wallet_and_saved_balance(self) -> None:
        result = decide_deposit(wallet=1000, saved=200, amount=300, interest=10, limit=1000)
        self.assertEqual((result.wallet_after, result.saved_after, result.interest), (710, 500, 10))

    def test_insufficient_wallet_is_rejected(self) -> None:
        with self.assertRaisesRegex(ValueError, "stone_insufficient"):
            decide_deposit(wallet=100, saved=0, amount=101, interest=0, limit=1000)

    def test_limit_is_rejected(self) -> None:
        with self.assertRaisesRegex(ValueError, "limit_exceeded"):
            decide_deposit(wallet=1000, saved=900, amount=101, interest=0, limit=1000)


if __name__ == "__main__":
    unittest.main()
