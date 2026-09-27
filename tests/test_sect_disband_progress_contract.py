import unittest

from scripts.check_full_refactor_progress import _slice_status


class SectDisbandProgressContractTests(unittest.TestCase):
    def test_inactive_disband_uses_feature_application(self):
        self.assertTrue(_slice_status()["sect"]["disband_application_owned"])

    def test_confirmed_disband_uses_feature_application(self):
        self.assertTrue(_slice_status()["sect"]["disband_confirmation_application_owned"])
        self.assertTrue(_slice_status()["sect"]["disband_confirmation_repository_owned"])
        self.assertTrue(_slice_status()["sect"]["disband_confirmation_request_path_has_no_ddl"])
        self.assertTrue(_slice_status()["sect"]["disband_confirmation_migration_registered"])
        self.assertTrue(_slice_status()["sect"]["disband_confirmation_migration_game_only"])
