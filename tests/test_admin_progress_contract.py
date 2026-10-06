import unittest

from scripts.check_full_refactor_progress import PACKAGE, ROOT, _admin_broadcast_owner_status, _slice_status


class AdminProgressContractTests(unittest.TestCase):
    def test_admin_broadcast_commands_and_consumers_share_the_state_owner(self):
        for key, value in _slice_status()["admin_broadcast_owner"].items():
            if key != "status":
                self.assertTrue(value, key)

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


class AdminBroadcastGateMutationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        files = {
            "application": PACKAGE / "features/admin/broadcast_application.py",
            "repository": PACKAGE / "features/admin/broadcast_repository.py",
            "history": PACKAGE / "features/admin/broadcast_history_repository.py",
            "facade": PACKAGE / "xiuxian/broadcast_manager.py",
            "handlers": PACKAGE / "xiuxian/xiuxian_admin/__init__.py",
            "web": PACKAGE / "xiuxian/xiuxian_web/messages.py",
            "events": PACKAGE / "xiuxian/__init__.py",
            "entry_tests": ROOT / "tests/test_admin_broadcast.py",
            "core_tests": PACKAGE / "features/admin/tests/test_broadcast_application.py",
            "history_tests": PACKAGE / "features/admin/tests/test_broadcast_history_repository.py",
        }
        cls.sources = {name: path.read_text(encoding="utf-8") for name, path in files.items()}

    def test_gate_rejects_owner_permission_identity_and_accounting_regressions(self):
        ownership = "six_admin_handlers_and_compatibility_calls_use_one_memory_owner"
        lifecycle = "feature_lifecycle_claims_and_inflight_cancellation_are_atomic"
        identity = "sender_identity_and_result_status_are_preserved_through_ports"
        pending = "pending_failures_and_cancelled_coroutines_do_not_report_false_success"
        cases = (
            ("handlers", "msg = await start_broadcast(", "msg = await legacy_start_broadcast(", ownership),
            ("handlers", 'on_command("群聊广播", permission=SUPERUSER', 'on_command("群聊广播", permission=None', ownership),
            ("facade", "return _broadcast_application", "return AdminBroadcastApplication(AdminBroadcastRepository(), history=_history_targets, sender=_send_broadcast_to_target)", ownership),
            ("application", "self.repository.claim(", "self.repository.legacy_claim(", lifecycle),
            ("repository", 'task["_inflight"][key] = claim', 'task["_inflight"][key] = None', lifecycle),
            ("repository", 'task[f"pending_{bucket}"].add(key)', 'task[f"sent_{bucket}"].add(key)', pending),
            ("history", "adapter=? AND bot_id=?", "adapter=?", "history_queries_are_readonly_bot_scoped_and_off_the_event_loop"),
            ("facade", "bot_id=bot_id,", 'bot_id="another-bot",', identity),
            ("facade", "return await delivery_service.send(\n        bot,", "await delivery_service.send(\n        bot,", identity),
        )
        for source, before, after, gate in cases:
            with self.subTest(source=source, mutation=after):
                self.assertIn(before, self.sources[source])
                changed = dict(self.sources)
                changed[source] = changed[source].replace(before, after, 1)
                self.assertFalse(_admin_broadcast_owner_status(changed)[gate])

    def test_gate_does_not_accept_reintroduced_legacy_task_state(self):
        changed = dict(self.sources)
        changed["facade"] += "\nBROADCAST_TASKS = {}\n"
        self.assertFalse(_admin_broadcast_owner_status(changed)[
            "six_admin_handlers_and_compatibility_calls_use_one_memory_owner"
        ])
