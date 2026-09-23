import unittest

from scripts.check_full_refactor_progress import _slice_status


class SectFairylandProgressContractTests(unittest.TestCase):
    def test_upgrade_uses_feature_application(self):
        self.assertTrue(_slice_status()["sect"]["fairyland_upgrade_application_owned"])