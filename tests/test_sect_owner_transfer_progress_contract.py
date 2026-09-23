import unittest

from scripts.check_full_refactor_progress import _slice_status


class SectOwnerTransferProgressContractTests(unittest.TestCase):
    def test_owner_transfer_uses_feature_application(self):
        self.assertTrue(_slice_status()["sect"]["owner_transfer_application_owned"])