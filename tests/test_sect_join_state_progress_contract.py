import unittest

from scripts.check_full_refactor_progress import _slice_status


class SectJoinStateProgressContractTests(unittest.TestCase):
    def test_join_state_uses_feature_application(self):
        self.assertTrue(_slice_status()["sect"]["join_state_application_owned"])