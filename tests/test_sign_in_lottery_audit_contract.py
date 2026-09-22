import unittest

from scripts.check_full_refactor_progress import _slice_status


class SignInLotteryAuditContractTests(unittest.TestCase):
    def test_lottery_core_default_legacy_flag_matches_legacy_usage(self):
        self.assertFalse(_slice_status()["sign_in"]["lottery_core_default_legacy"])
