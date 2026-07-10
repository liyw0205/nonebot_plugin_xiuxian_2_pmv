from __future__ import annotations

import ast
import unittest
from pathlib import Path


SOURCE_ROOT = Path(__file__).resolve().parents[1] / "nonebot_plugin_xiuxian_2"
PROJECT_ROOT = SOURCE_ROOT.parent


class SourceQualityTests(unittest.TestCase):
    def test_runtime_sqlite_sidecars_are_ignored(self) -> None:
        source = (PROJECT_ROOT / ".gitignore").read_text(encoding="utf-8")
        self.assertIn("data/xiuxian/*.db-*", source)
        self.assertIn("data/xiuxian/activity/*.db-*", source)

    def test_python_sources_do_not_use_bare_except(self) -> None:
        violations: list[str] = []
        for path in SOURCE_ROOT.rglob("*.py"):
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
            for node in ast.walk(tree):
                if isinstance(node, ast.ExceptHandler) and node.type is None:
                    violations.append(f"{path.relative_to(SOURCE_ROOT)}:{node.lineno}")

        self.assertEqual(violations, [], "Bare except clauses found: " + ", ".join(violations))

    def test_business_sources_use_centralized_data_paths(self) -> None:
        violations: list[str] = []
        forbidden_fragments = (
            'Path() / "data" / "xiuxian"',
            "Path() / 'data' / 'xiuxian'",
        )

        for path in SOURCE_ROOT.rglob("*.py"):
            relative_path = path.relative_to(SOURCE_ROOT)
            if relative_path == Path("paths.py") or "vendor" in relative_path.parts:
                continue
            source = path.read_text(encoding="utf-8")
            if any(fragment in source for fragment in forbidden_fragments):
                violations.append(str(relative_path))

        self.assertEqual(
            violations,
            [],
            "Direct data/xiuxian path construction found: " + ", ".join(violations),
        )

    def test_message_database_uses_centralized_path(self) -> None:
        path = SOURCE_ROOT / "xiuxian" / "xiuxian_utils" / "message_db.py"
        source = path.read_text(encoding="utf-8")
        self.assertNotIn('MESSAGE_DB = Path() / "message.db"', source)
        self.assertIn("get_paths().message_db", source)
        self.assertIn("migrate_legacy_message_db()", source)

    def test_newapi_runtime_state_uses_central_json_store(self) -> None:
        source = (
            SOURCE_ROOT
            / "xiuxian"
            / "xiuxian_entertainment"
            / "mod"
            / "newapi_store.py"
        ).read_text(encoding="utf-8")
        self.assertNotIn("json.load(", source)
        self.assertNotIn("json.dump(", source)
        self.assertIn("update_json_file(", source)

    def test_entertainment_http_calls_use_central_client(self) -> None:
        paths = (
            SOURCE_ROOT / "xiuxian" / "xiuxian_entertainment" / "command.py",
            SOURCE_ROOT
            / "xiuxian"
            / "xiuxian_entertainment"
            / "mod"
            / "newapi_client.py",
            SOURCE_ROOT
            / "xiuxian"
            / "xiuxian_entertainment"
            / "mod"
            / "alist_webdav.py",
        )
        for path in paths:
            source = path.read_text(encoding="utf-8")
            self.assertNotIn("requests.get(", source)
            self.assertNotIn("requests.post(", source)
            self.assertNotIn("requests.request(", source)
            self.assertIn("http_client", source)

    def test_uptime_formatters_use_period_helpers(self) -> None:
        for relative in (
            Path("xiuxian/xiuxian_web/core.py"),
            Path("xiuxian/xiuxian_status/__init__.py"),
        ):
            source = (SOURCE_ROOT / relative).read_text(encoding="utf-8")
            self.assertIn("format_duration_full", source)

    def test_runtime_and_web_use_settings_provider(self) -> None:
        for relative in (
            Path("xiuxian/__init__.py"),
            Path("xiuxian/runtime.py"),
            Path("xiuxian/xiuxian_web/core.py"),
        ):
            source = (SOURCE_ROOT / relative).read_text(encoding="utf-8")
            self.assertIn("settings", source)

    def test_internal_imports_do_not_assume_top_level_package_name(self) -> None:
        violations: list[str] = []
        for path in SOURCE_ROOT.rglob("*.py"):
            if "vendor" in path.parts:
                continue
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
            for node in ast.walk(tree):
                if isinstance(node, ast.ImportFrom) and str(node.module or "").startswith(
                    "nonebot_plugin_xiuxian_2"
                ):
                    violations.append(f"{path.relative_to(SOURCE_ROOT)}:{node.lineno}")
                elif isinstance(node, ast.Import):
                    for alias in node.names:
                        if alias.name.startswith("nonebot_plugin_xiuxian_2"):
                            violations.append(
                                f"{path.relative_to(SOURCE_ROOT)}:{node.lineno}"
                            )

        self.assertEqual(
            violations,
            [],
            "Absolute package imports break src.plugins namespace loading: "
            + ", ".join(violations),
        )

    def test_plugin_import_does_not_run_startup_maintenance(self) -> None:
        entrypoint = SOURCE_ROOT / "xiuxian" / "__init__.py"
        source = entrypoint.read_text(encoding="utf-8")
        forbidden_calls = (
            "ensure_plugin_dependencies()",
            "download_xiuxian_data()",
            "initialize_backend()",
            "_run_startup_database_maintenance()",
        )

        violations = [call for call in forbidden_calls if call in source]
        self.assertEqual(
            violations,
            [],
            "Import-time startup maintenance found: " + ", ".join(violations),
        )

    def test_web_server_is_not_started_during_import(self) -> None:
        web_entrypoint = SOURCE_ROOT / "xiuxian" / "xiuxian_web" / "__init__.py"
        tree = ast.parse(
            web_entrypoint.read_text(encoding="utf-8"),
            filename=str(web_entrypoint),
        )
        module_level_starts = [
            node.lineno
            for node in tree.body
            if isinstance(node, ast.Expr)
            and isinstance(node.value, ast.Call)
            and isinstance(node.value.func, ast.Attribute)
            and node.value.func.attr in {"run", "start", "serve_forever"}
        ]

        self.assertEqual(
            module_level_starts,
            [],
            f"Web server starts during import at lines: {module_level_starts}",
        )

    def test_web_defaults_are_local_and_high_risk_features_are_disabled(self) -> None:
        config_path = SOURCE_ROOT / "xiuxian" / "xiuxian_config.py"
        source = config_path.read_text(encoding="utf-8")
        self.assertIn('self.web_host = "127.0.0.1"', source)
        self.assertIn("self.web_enable_database_write = False", source)
        self.assertIn("self.web_enable_backup_restore = False", source)
        self.assertIn("self.web_enable_message_send = False", source)

        web_entrypoint = SOURCE_ROOT / "xiuxian" / "xiuxian_web" / "__init__.py"
        entrypoint_source = web_entrypoint.read_text(encoding="utf-8")
        self.assertIn("if not web_auth_is_configured():", entrypoint_source)
        self.assertIn("XIUXIAN_WEB_PASSWORD_HASH", entrypoint_source)

    def test_web_modules_do_not_use_core_star_imports(self) -> None:
        web_root = SOURCE_ROOT / "xiuxian" / "xiuxian_web"
        violations = []
        for path in web_root.glob("*.py"):
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
            if any(
                isinstance(node, ast.ImportFrom)
                and node.module == "core"
                and any(alias.name == "*" for alias in node.names)
                for node in tree.body
            ):
                violations.append(path.name)

        self.assertEqual(
            violations,
            [],
            "Web modules importing core with *: " + ", ".join(violations),
        )

    def test_web_message_send_uses_delivery_service(self) -> None:
        messages = SOURCE_ROOT / "xiuxian" / "xiuxian_web" / "messages.py"
        source = messages.read_text(encoding="utf-8")
        tree = ast.parse(source, filename=str(messages))
        send_route = next(
            node
            for node in tree.body
            if isinstance(node, ast.FunctionDef) and node.name == "api_messages_send"
        )
        send_source = ast.get_source_segment(source, send_route) or ""

        self.assertIn("delivery_service.send", send_source)
        self.assertNotIn("bot.send_to_group", send_source)
        self.assertNotIn("bot.send_to_c2c", send_source)
        self.assertNotIn("bot.send_to_channel", send_source)
        self.assertNotIn("bot.send_to_dms", send_source)

    def test_presenter_text_outlet_uses_delivery_service(self) -> None:
        utils = SOURCE_ROOT / "xiuxian" / "xiuxian_utils" / "utils.py"
        source = utils.read_text(encoding="utf-8")
        tree = ast.parse(source, filename=str(utils))
        send_outlet = next(
            node
            for node in tree.body
            if isinstance(node, ast.AsyncFunctionDef)
            and node.name == "_send_event_message"
        )
        outlet_source = ast.get_source_segment(source, send_outlet) or ""

        self.assertIn("delivery_service.reply", outlet_source)
        self.assertNotIn("bot.send(", outlet_source)
        self.assertNotIn("send_reference_reply", outlet_source)

    def test_direct_adapter_sends_are_limited_to_migration_allowlist(self) -> None:
        allowed_prefixes = {
            "adapter_compat.py",
            "adapter_message_sender.py",
            "messaging/delivery.py",
        }
        direct_methods = {
            "send",
            "send_to_group",
            "send_to_c2c",
            "send_to_channel",
            "send_to_dms",
        }
        violations = []
        for path in (SOURCE_ROOT / "xiuxian").rglob("*.py"):
            if "vendor" in path.parts:
                continue
            relative = path.relative_to(SOURCE_ROOT / "xiuxian").as_posix()
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
            has_direct_send = any(
                isinstance(node, ast.Call)
                and isinstance(node.func, ast.Attribute)
                and node.func.attr in direct_methods
                and isinstance(node.func.value, ast.Name)
                and node.func.value.id == "bot"
                for node in ast.walk(tree)
            )
            allowed = any(
                relative == prefix or relative.startswith(prefix + "/")
                for prefix in allowed_prefixes
            )
            if has_direct_send and not allowed:
                violations.append(relative)

        self.assertEqual(
            violations,
            [],
            "Direct Adapter sends outside migration allowlist: "
            + ", ".join(violations),
        )

    def test_web_and_broadcast_do_not_generate_qq_msg_seq(self) -> None:
        paths = (
            SOURCE_ROOT / "xiuxian" / "xiuxian_web" / "messages.py",
            SOURCE_ROOT / "xiuxian" / "broadcast_manager.py",
        )
        violations = [
            path.relative_to(SOURCE_ROOT).as_posix()
            for path in paths
            if "msg_seq=" in path.read_text(encoding="utf-8")
        ]
        self.assertEqual(violations, [])

    def test_runtime_http_calls_use_central_client_or_documented_protocols(self) -> None:
        checked = (
            "xiuxian_admin/empty_fallback.py",
            "xiuxian_entertainment/mod/anime_reaction.py",
            "xiuxian_entertainment/mod/music_utils.py",
            "xiuxian_info/draw_changelog.py",
            "xiuxian_utils/external_api.py",
            "xiuxian_web/core.py",
            "xiuxian_web/messages.py",
        )
        forbidden = ("requests.get(", "requests.post(", "httpx.AsyncClient(", "aiohttp.ClientSession(")
        violations = []
        for relative in checked:
            source = (SOURCE_ROOT / "xiuxian" / relative).read_text(encoding="utf-8")
            if any(token in source for token in forbidden):
                violations.append(relative)
        self.assertEqual(violations, [])

    def test_high_risk_mutable_json_uses_central_store(self) -> None:
        checked = (
            "xiuxian_compensation/common.py",
            "xiuxian_compensation/invitation.py",
            "xiuxian_Interactive/__init__.py",
            "xiuxian_activity/activity_config.py",
            "xiuxian_base/xiangyuan.py",
            "xiuxian_entertainment/mod/gomoku.py",
            "xiuxian_entertainment/mod/half_ten.py",
            "xiuxian_entertainment/mod/minesweeper.py",
            "xiuxian_sect/sectconfig.py",
            "xiuxian_work/reward_data_source.py",
        )
        violations = []
        for relative in checked:
            source = (SOURCE_ROOT / "xiuxian" / relative).read_text(encoding="utf-8")
            tree = ast.parse(source, filename=relative)
            direct_calls = [
                node.lineno
                for node in ast.walk(tree)
                if isinstance(node, ast.Call)
                and isinstance(node.func, ast.Attribute)
                and isinstance(node.func.value, ast.Name)
                and node.func.value.id == "json"
                and node.func.attr in {"load", "dump"}
            ]
            if relative == "xiuxian_activity/activity_config.py":
                direct_calls = [
                    line for line in direct_calls
                    if line != next(
                        child.lineno
                        for node in tree.body
                        if isinstance(node, ast.FunctionDef)
                        and node.name == "_load_default_config"
                        for child in ast.walk(node)
                        if isinstance(child, ast.Call)
                        and isinstance(child.func, ast.Attribute)
                        and child.func.attr == "load"
                    )
                ]
            if direct_calls:
                violations.append(f"{relative}:{direct_calls}")
        self.assertEqual(violations, [])

    def test_illusion_state_uses_central_json_store(self) -> None:
        source = (
            SOURCE_ROOT
            / "xiuxian"
            / "xiuxian_Illusion"
            / "IllusionData.py"
        ).read_text(encoding="utf-8")
        self.assertNotIn("json.load(", source)
        self.assertNotIn("json.dump(", source)
        self.assertIn("update_json_file(", source)

    def test_xianshi_purchase_uses_transactional_service(self) -> None:
        trade_root = SOURCE_ROOT / "xiuxian" / "xiuxian_trade"
        command_source = (trade_root / "__init__.py").read_text(encoding="utf-8")
        repository_source = (trade_root / "repository.py").read_text(encoding="utf-8")
        self.assertIn("xianshi_purchase_service.purchase(", command_source)
        self.assertNotIn("xianshi_buy_refund", command_source)
        self.assertIn("BEGIN IMMEDIATE", repository_source)
        self.assertIn("xianshi_operations", repository_source)

    def test_equipment_change_uses_transactional_service(self) -> None:
        back_root = SOURCE_ROOT / "xiuxian" / "xiuxian_back"
        command_source = (back_root / "__init__.py").read_text(encoding="utf-8")
        service_source = (back_root / "equipment_service.py").read_text(encoding="utf-8")
        self.assertIn("equipment_service.change(", command_source)
        self.assertIn("BEGIN IMMEDIATE", service_source)
        self.assertIn("equipment_operations", service_source)

    def test_interaction_ack_is_wired_into_event_lifecycle(self) -> None:
        entrypoint = SOURCE_ROOT / "xiuxian" / "__init__.py"
        source = entrypoint.read_text(encoding="utf-8")
        self.assertIn("@event_preprocessor\nasync def arm_qq_interaction_ack", source)
        self.assertIn("@run_postprocessor\nasync def ack_failed_qq_interaction", source)
        self.assertIn("@event_postprocessor\nasync def ack_completed_qq_interaction", source)

    def test_qq_lifecycle_state_is_wired_into_event_preprocessing(self) -> None:
        source = (SOURCE_ROOT / "xiuxian" / "__init__.py").read_text(
            encoding="utf-8"
        )
        self.assertIn(
            "@event_preprocessor\nasync def track_qq_lifecycle_event",
            source,
        )
        self.assertIn("result = apply_lifecycle_event(bot, event)", source)
        self.assertIn("if is_lifecycle_event(event):\n        return", source)

    def test_reliability_queues_are_wired_into_runtime_lifecycle(self) -> None:
        source = (SOURCE_ROOT / "xiuxian" / "runtime.py").read_text(encoding="utf-8")
        self.assertIn('BackgroundJobQueue(\n    "background"', source)
        self.assertIn('BackgroundJobQueue(\n    "critical"', source)
        self.assertIn("await background_jobs.start()", source)
        self.assertIn("await critical_jobs.start()", source)
        self.assertIn("@driver.on_shutdown", source)
        self.assertIn("await critical_jobs.stop(drain=True)", source)

    def test_web_bot_lookup_uses_unified_selector(self) -> None:
        source = (SOURCE_ROOT / "xiuxian" / "xiuxian_web" / "core.py").read_text(
            encoding="utf-8"
        )
        self.assertIn("from ..qq_compat import bot_selector", source)
        self.assertIn("return bot_selector.select(adapter=", source)

    def test_entertainment_media_uses_delivery_media_facade(self) -> None:
        source = (
            SOURCE_ROOT / "xiuxian" / "xiuxian_entertainment" / "command.py"
        ).read_text(encoding="utf-8")
        self.assertIn("MediaInput(media, media_names[media_type])", source)
        self.assertIn("lambda: delivery_service.reply_media(", source)
        runtime = (SOURCE_ROOT / "xiuxian" / "runtime.py").read_text(encoding="utf-8")
        self.assertIn("await submit_background_job(media_resolver.cleanup)", runtime)

    def test_activity_rule_helpers_have_explicit_dependencies(self) -> None:
        activity_rules = SOURCE_ROOT / "xiuxian" / "xiuxian_activity" / "activity_rules.py"
        tree = ast.parse(
            activity_rules.read_text(encoding="utf-8"),
            filename=str(activity_rules),
        )
        imported_names = {
            alias.asname or alias.name
            for node in tree.body
            if isinstance(node, (ast.Import, ast.ImportFrom))
            for alias in node.names
        }

        self.assertIn("_get_extensions", imported_names)

    def test_database_backup_scheduler_does_not_block_event_loop(self) -> None:
        scheduler = SOURCE_ROOT / "xiuxian" / "xiuxian_scheduler" / "__init__.py"
        source = scheduler.read_text(encoding="utf-8")

        self.assertIn(
            "await asyncio.to_thread(UpdateManager().backup_db_files)",
            source,
        )

    def test_entertainment_network_calls_use_io_runtime(self) -> None:
        entertainment = SOURCE_ROOT / "xiuxian" / "xiuxian_entertainment"
        expected_calls = {
            entertainment / "mod" / "alist_webdav.py": (
                "await run_blocking_io(\n            _cached_propfind",
                "await run_blocking_io(\n            _propfind",
                "await run_blocking_io(\n            _format_link_message",
            ),
            entertainment / "mod" / "newapi_commands.py": (
                "await run_blocking_io(\n                _run_checkin_for_account",
                "await run_blocking_io(\n                fetch_user_self",
            ),
            entertainment / "media_parser" / "service.py": (
                "await run_blocking_io(ensure_vendor_core",
            ),
            entertainment / "mod" / "anime_reaction.py": (
                "await run_blocking_io(_fetch_nekos_sync",
            ),
            entertainment / "mod" / "music.py": (
                "songs = await run_blocking_io(",
            ),
        }

        missing = []
        for path, calls in expected_calls.items():
            source = path.read_text(encoding="utf-8")
            missing.extend(
                f"{path.relative_to(SOURCE_ROOT)}: {call}"
                for call in calls
                if call not in source
            )

        self.assertEqual(
            missing,
            [],
            "Blocking entertainment I/O found: " + ", ".join(missing),
        )

        direct_to_thread = []
        for path in entertainment.rglob("*.py"):
            if path.name == "io_runtime.py" or "vendor" in path.parts:
                continue
            if "asyncio.to_thread" in path.read_text(encoding="utf-8"):
                direct_to_thread.append(str(path.relative_to(SOURCE_ROOT)))

        self.assertEqual(
            direct_to_thread,
            [],
            "Entertainment code bypasses bounded I/O runtime: "
            + ", ".join(direct_to_thread),
        )


if __name__ == "__main__":
    unittest.main()
