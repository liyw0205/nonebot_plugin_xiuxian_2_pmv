import unittest

from scripts.check_full_refactor_progress import _slice_status


class SectScheduledGrantProgressContractTests(unittest.TestCase):
    def test_scheduled_grant_uses_feature_application(self):
        sect = _slice_status()["sect"]
        self.assertTrue(sect["scheduled_grant_application_owned"])
        self.assertTrue(sect["scheduled_grant_target_query_application_owned"])
        self.assertTrue(sect["scheduled_grant_repository_owned"])
        self.assertTrue(sect["scheduled_grant_request_path_has_no_ddl"])
        self.assertTrue(sect["scheduled_grant_success_result_owned"])
        self.assertTrue(sect["scheduled_grant_migration_registered"])
        self.assertTrue(sect["scheduled_grant_migration_game_only"])
