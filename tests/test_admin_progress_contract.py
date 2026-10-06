import unittest

from scripts.check_full_refactor_progress import (
    PACKAGE, ROOT, _admin_broadcast_owner_status, _admin_runtime_owner_status,
    _avatar_identity_priority, _slice_status,
)


class AdminProgressContractTests(unittest.TestCase):
    def test_admin_rift_items_and_impersonation_use_their_feature_owners(self):
        for key, value in _slice_status()["admin_runtime_owner"].items():
            if key != "status":
                self.assertTrue(value, key)

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


class AdminRuntimeGateMutationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        files = {
            "handlers": "xiuxian/xiuxian_admin/__init__.py",
            "rift": "xiuxian/xiuxian_rift/__init__.py",
            "rift_application": "features/rift/application.py",
            "catalog_facade": "xiuxian/xiuxian_utils/item_json.py",
            "catalog_application": "features/admin/item_catalog_application.py",
            "catalog_repository": "features/admin/item_catalog_repository.py",
            "utils": "xiuxian/xiuxian_utils/utils.py",
            "impersonation_application": "features/admin/impersonation_application.py",
            "impersonation_repository": "features/admin/impersonation_repository.py",
        }
        cls.sources = {name: (PACKAGE / path).read_text(encoding="utf-8") for name, path in files.items()}

    def test_avatar_priority_rejects_reversed_or_missing_identity_edges(self):
        source = self.sources["utils"]
        avatar = "_player_avatar().get_active_user_id(original_user_id)"
        impersonation = "get_impersonating_target(original_user_id)"
        self.assertTrue(_avatar_identity_priority(source))
        changed = (
            source.replace(avatar, "missing_avatar_read"),
            source.replace(impersonation, "missing_impersonation_read"),
            source.replace(avatar, "__avatar_marker__")
                  .replace(impersonation, avatar).replace("__avatar_marker__", impersonation),
        )
        for index, mutation in enumerate(changed):
            with self.subTest(mutation=index):
                self.assertFalse(_avatar_identity_priority(mutation))

    def test_gate_rejects_owner_bypass_partial_publish_and_receipt_projection(self):
        owner = "three_superuser_handlers_reach_feature_owners"
        catalog = "item_catalog_strict_reload_publishes_once_after_build_without_clearing"
        impersonation = "impersonation_uses_real_identity_and_one_atomic_shared_mapping"
        rift = "manual_rift_success_projects_current_world_not_historical_receipt"
        cases = (
            ("handlers", "await create_rift(bot, event)", "await legacy_create_rift(bot, event)", owner),
            ("handlers", 'on_command("重载items", permission=SUPERUSER', 'on_command("重载items", permission=None', owner),
            ("catalog_facade", "AdminItemCatalogApplication(self.repository).reload()", "self.repository.reload()", owner),
            ("catalog_repository", "return self._load(strict=True)", "return self._load(strict=False)", catalog),
            ("catalog_repository", "self._state = (items, sources)",
             "self._state = ({}, {})\n            self._state = (items, sources)", catalog),
            ("utils", "_impersonating_users = impersonation_application.mapping", "_impersonating_users = {}", impersonation),
            ("impersonation_application", "return self.repository\n", "return self.repository.snapshot()\n", impersonation),
            ("rift", "_sync_world_projection(SimpleNamespace(**current_state), save_legacy=False)",
             "_sync_world_projection(result.state, save_legacy=False)", rift),
            ("rift", 'current_state["generation_id"] != result.state.generation_id',
             'current_state["generation_id"] == result.state.generation_id', rift),
        )
        for source, before, after, gate in cases:
            with self.subTest(source=source, mutation=after):
                self.assertIn(before, self.sources[source])
                changed = dict(self.sources)
                changed[source] = changed[source].replace(before, after, 1)
                self.assertFalse(_admin_runtime_owner_status(changed)[gate])

    def test_gate_rejects_reintroduced_impersonation_dictionary_and_direct_lookup(self):
        gate = "impersonation_uses_real_identity_and_one_atomic_shared_mapping"
        for addition in ("\n_impersonating_users = {}\n", "\nstale_target = _impersonating_users['admin']\n"):
            with self.subTest(addition=addition):
                changed = dict(self.sources)
                changed["utils"] += addition
                self.assertFalse(_admin_runtime_owner_status(changed)[gate])
