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
            "impart_stone_request_path_has_no_ddl",
            "impart_stone_balance_owner_is_impart_database",
            "impart_stone_single_snapshot_feature_owned",
            "impart_stone_startup_migration_registered",
            "impart_stone_schema_missing_reported",
            "impart_stone_migration_game_only",
            "accessory_single_repository_owned",
            "accessory_single_request_path_has_no_ddl",
            "accessory_single_startup_migration_registered",
            "accessory_single_schema_missing_reported",
            "accessory_single_migration_game_only",
        ):
            self.assertTrue(admin[key], key)
