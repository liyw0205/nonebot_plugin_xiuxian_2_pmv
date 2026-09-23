import unittest

from scripts.check_full_refactor_progress import _slice_status


class SectCreationProgressContractTests(unittest.TestCase):
    def test_creation_paths_use_feature_application(self):
        self.assertTrue(_slice_status()["sect"]["creation_application_owned"])