import unittest

from scripts.check_full_refactor_progress import _slice_status


class AdminProgressContractTests(unittest.TestCase):
    def test_admin_single_mutations_are_application_owned(self):
        admin = _slice_status()["admin"]
        for key in (
            "item_destroy_application_owned",
            "exp_adjust_application_owned",
            "level_change_application_owned",
            "root_change_application_owned",
        ):
            self.assertTrue(admin[key], key)
