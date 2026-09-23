import unittest

from scripts.check_full_refactor_progress import _slice_status


class SectOwnerInheritProgressContractTests(unittest.TestCase):
    def test_owner_inherit_uses_feature_application(self):
        self.assertTrue(_slice_status()["sect"]["owner_inherit_application_owned"])