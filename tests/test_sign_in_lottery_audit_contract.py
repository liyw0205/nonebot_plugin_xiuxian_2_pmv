import unittest

from scripts.check_full_refactor_progress import _slice_status


class SignInLotteryAuditContractTests(unittest.TestCase):
    def test_lottery_default_is_feature_owned_and_fallback_is_explicit(self):
        sign_in = _slice_status()["sign_in"]
        self.assertTrue(sign_in["lottery_application_owned"])
        self.assertTrue(sign_in["legacy_fallback_explicit_only"])
