import unittest

from scripts.check_full_refactor_progress import _slice_status


class AdminProgressContractTests(unittest.TestCase):
    def test_admin_command_controls_and_router_share_the_state_owner(self):
        for key, value in _slice_status()["admin_command_control_owner"].items():
            if key != "status":
                self.assertTrue(value, key)

    def test_admin_blackhouse_commands_and_router_share_the_state_owner(self):
        for key, value in _slice_status()["admin_blackhouse_owner"].items():
            if key != "status":
                self.assertTrue(value, key)

    def test_existing_admin_mutations_report_the_actual_result(self):
        for key, value in _slice_status()["admin_existing_mutation_results"].items():
            if key != "status":
                self.assertTrue(value, key)

    def test_admin_config_commands_share_the_default_state_owner(self):
        config = _slice_status()["admin_config_owner"]
        for key, value in config.items():
            if key != "status":
                self.assertTrue(value, key)

    def test_admin_output_commands_keep_compatibility_boundaries(self):
        output = _slice_status()["admin_output_compatibility"]
        for key, value in output.items():
            if key != "status":
                self.assertTrue(value, key)

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
            "accessory_batch_application_owned",
            "accessory_batch_request_path_has_no_ddl",
            "accessory_batch_disk_preflight_and_bounded_targets",
            "accessory_batch_startup_migration_registered",
            "accessory_batch_migration_game_only",
            "item_batch_application_owned",
            "item_batch_request_path_has_no_ddl",
            "item_batch_disk_preflight_and_bounded_roster",
            "item_batch_startup_migration_registered",
            "item_batch_migration_game_only",
        ):
            self.assertTrue(admin[key], key)
