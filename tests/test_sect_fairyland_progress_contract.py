import unittest

from scripts.check_full_refactor_progress import _slice_status


class SectFairylandProgressContractTests(unittest.TestCase):
    def test_upgrade_is_feature_owned_and_uses_precreated_game_schema(self):
        sect = _slice_status()["sect"]
        self.assertTrue(sect["fairyland_upgrade_application_owned"])
        self.assertTrue(sect["fairyland_upgrade_repository_owned"])
        self.assertTrue(sect["fairyland_upgrade_request_path_has_no_ddl"])
        self.assertTrue(sect["fairyland_upgrade_migration_registered"])
        self.assertTrue(sect["fairyland_upgrade_migration_game_only"])
        self.assertTrue(sect["fairyland_upgrade_success_result_owned"])
        self.assertTrue(sect["fairyland_upgrade_duplicate_handled_before_effects"])
        self.assertTrue(sect["fairyland_claim_status_application_owned"])
        self.assertTrue(sect["fairyland_claim_status_repository_owned"])
        self.assertTrue(sect["fairyland_claim_status_read_only"])
        self.assertTrue(sect["fairyland_claim_status_no_player_manager"])
        self.assertTrue(sect["sect_default_repository_has_no_legacy_fallback"])
