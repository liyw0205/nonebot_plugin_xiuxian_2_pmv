import unittest

from scripts.check_full_refactor_progress import _slice_status


class SectScheduledGrantProgressContractTests(unittest.TestCase):
    def test_scheduled_grant_uses_feature_application(self):
        self.assertTrue(_slice_status()["sect"]["scheduled_grant_application_owned"])