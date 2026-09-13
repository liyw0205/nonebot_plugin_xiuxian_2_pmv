from __future__ import annotations

import unittest

from nonebot_plugin_xiuxian_2.features.bank.feature_flag import bank_first_use_enabled


class BankFirstUseFlagTests(unittest.TestCase):
    def test_default_is_disabled(self) -> None:
        self.assertFalse(bank_first_use_enabled(None))
        self.assertFalse(bank_first_use_enabled({}))

    def test_explicit_true_enables(self) -> None:
        self.assertTrue(bank_first_use_enabled({"bank_first_use_enabled": True}))
        self.assertFalse(bank_first_use_enabled({"bank_first_use_enabled": False}))


if __name__ == "__main__":
    unittest.main()
