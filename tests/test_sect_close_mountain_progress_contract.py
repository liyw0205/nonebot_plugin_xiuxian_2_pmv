import unittest

from scripts.check_full_refactor_progress import _slice_status


class SectCloseMountainProgressContractTests(unittest.TestCase):
    def test_close_mountain_uses_feature_application(self):
        self.assertTrue(_slice_status()["sect"]["close_mountain_application_owned"])