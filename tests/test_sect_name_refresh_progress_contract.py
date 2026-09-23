import unittest

from scripts.check_full_refactor_progress import _slice_status


class SectNameRefreshProgressContractTests(unittest.TestCase):
    def test_name_refresh_uses_feature_application(self):
        self.assertTrue(_slice_status()["sect"]["name_refresh_application_owned"])