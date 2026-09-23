import unittest

from scripts.check_full_refactor_progress import _slice_status


class SectBuffSearchProgressContractTests(unittest.TestCase):
    def test_main_and_secondary_search_use_feature_application(self):
        self.assertTrue(_slice_status()["sect"]["buff_search_application_owned"])