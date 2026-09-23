import unittest

from scripts.check_full_refactor_progress import _slice_status


class SectTaskSettlementProgressContractTests(unittest.TestCase):
    def test_task_settlement_uses_feature_application(self):
        self.assertTrue(_slice_status()["sect"]["task_settlement_application_owned"])