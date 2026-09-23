import unittest

from scripts.check_full_refactor_progress import _slice_status


class SectDisbandProgressContractTests(unittest.TestCase):
    def test_inactive_disband_uses_feature_application(self):
        self.assertTrue(_slice_status()["sect"]["disband_application_owned"])