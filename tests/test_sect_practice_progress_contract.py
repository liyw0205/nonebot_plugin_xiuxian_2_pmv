import unittest

from scripts.check_full_refactor_progress import _slice_status


class SectPracticeProgressContractTests(unittest.TestCase):
    def test_all_practice_upgrades_use_feature_application(self):
        self.assertTrue(_slice_status()["sect"]["practice_application_owned"])