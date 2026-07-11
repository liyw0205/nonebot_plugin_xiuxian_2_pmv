from __future__ import annotations

import ast
import unittest
from pathlib import Path


SOURCE_ROOT = Path(__file__).resolve().parents[1] / "nonebot_plugin_xiuxian_2"
PROJECT_ROOT = SOURCE_ROOT.parent


class SourceQualityTests(unittest.TestCase):
    def test_registration_uses_concurrent_insert_path(self) -> None:
        source = (
            SOURCE_ROOT / "xiuxian" / "xiuxian_base" / "__init__.py"
        ).read_text(encoding="utf-8")
        start = source.index("async def run_xiuxian_")
        end = source.index("@sign_in.handle", start)
        registration = source[start:end]
        self.assertIn("registration_batcher.submit(", registration)
        self.assertNotIn("sql_message.get_user_info_with_name(user_name)", registration)

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

    def test_puppet_auto_harvest_scheduler_prevents_overlapping_runs(self) -> None:
        source = (
            SOURCE_ROOT / "xiuxian" / "xiuxian_puppet" / "__init__.py"
        ).read_text(encoding="utf-8")
        start = source.index('id="auto_harvest"')
        decorator = source[source.rfind("@scheduler.scheduled_job", 0, start):start + 120]
        self.assertIn("coalesce=True", decorator)
        self.assertIn("max_instances=1", decorator)
        self.assertIn("misfire_grace_time=300", decorator)

    def test_puppet_purchase_and_upgrade_use_transactional_service(self) -> None:
        puppet_root = SOURCE_ROOT / "xiuxian" / "xiuxian_puppet"
        command_source = (puppet_root / "__init__.py").read_text(encoding="utf-8")
        service_source = (puppet_root / "operation_service.py").read_text(
            encoding="utf-8"
        )
        self.assertIn("puppet_operation_service.purchase(", command_source)
        self.assertIn("puppet_operation_service.upgrade(", command_source)
        self.assertNotIn("sql_message.update_ls(user_id, cost, 2)", command_source)
        self.assertIn("BEGIN IMMEDIATE", service_source)
        self.assertIn("puppet_operations", service_source)

    def test_daily_dungeon_reset_scheduler_prevents_overlapping_runs(self) -> None:
        source = (
            SOURCE_ROOT / "xiuxian" / "xiuxian_dungeon" / "__init__.py"
        ).read_text(encoding="utf-8")
        start = source.index('id="daily_dungeon_reset"')
        decorator = source[source.rfind("@scheduler.scheduled_job", 0, start):start + 120]
        self.assertIn("coalesce=True", decorator)
        self.assertIn("max_instances=1", decorator)
        self.assertIn("misfire_grace_time=300", decorator)

    def test_world_boss_scheduler_prevents_overlapping_runs(self) -> None:
        source = (
            SOURCE_ROOT / "xiuxian" / "xiuxian_boss" / "__init__.py"
        ).read_text(encoding="utf-8")
        start = source.index('id="generate_all_bosses"')
        job = source[source.rfind("scheduler.add_job(", 0, start):start + 160]
        self.assertIn("coalesce=True", job)
        self.assertIn("max_instances=1", job)
        self.assertIn("misfire_grace_time=60", job)

    def test_guishi_matching_scheduler_prevents_overlapping_runs(self) -> None:
        source = (
            SOURCE_ROOT / "xiuxian" / "xiuxian_trade" / "__init__.py"
        ).read_text(encoding="utf-8")
        start = source.index('id="auto_guishi_transactions"')
        decorator = source[
            source.rfind("@scheduler.scheduled_job", 0, start):start + 160
        ]
        self.assertIn("coalesce=True", decorator)
        self.assertIn("max_instances=1", decorator)
        self.assertIn("misfire_grace_time=300", decorator)

    def test_guishi_expiry_scheduler_prevents_overlapping_runs(self) -> None:
        source = (
            SOURCE_ROOT / "xiuxian" / "xiuxian_trade" / "__init__.py"
        ).read_text(encoding="utf-8")
        start = source.index('id="clear_expired_baitan_orders"')
        decorator = source[
            source.rfind("@scheduler.scheduled_job", 0, start):start + 160
        ]
        self.assertIn("coalesce=True", decorator)
        self.assertIn("max_instances=1", decorator)
        self.assertIn("misfire_grace_time=300", decorator)

    def test_auction_schedulers_prevent_overlapping_runs(self) -> None:
        source = (
            SOURCE_ROOT / "xiuxian" / "xiuxian_trade" / "__init__.py"
        ).read_text(encoding="utf-8")
        for job_id in ("auto_start_auction", "check_auction_end"):
            start = source.index(f'id="{job_id}"')
            decorator = source[
                source.rfind("@scheduler.scheduled_job", 0, start):start + 160
            ]
            self.assertIn("coalesce=True", decorator)
            self.assertIn("max_instances=1", decorator)
            self.assertIn("misfire_grace_time=300", decorator)

    def test_demon_invasion_scheduler_prevents_overlapping_runs(self) -> None:
        source = (
            SOURCE_ROOT / "xiuxian" / "xiuxian_world_events" / "__init__.py"
        ).read_text(encoding="utf-8")
        start = source.index('id="demon_invasion_schedule"')
        decorator = source[
            source.rfind("@scheduler.scheduled_job", 0, start):start + 160
        ]
        self.assertIn("coalesce=True", decorator)
        self.assertIn("max_instances=1", decorator)
        self.assertIn("misfire_grace_time=300", decorator)

    def test_web_defaults_are_local_and_have_no_feature_gates(self) -> None:
        config_path = SOURCE_ROOT / "xiuxian" / "xiuxian_config.py"
        source = config_path.read_text(encoding="utf-8")
        self.assertIn('self.web_host = "127.0.0.1"', source)
        self.assertNotIn("self.web_enable_database_write", source)
        self.assertNotIn("self.web_enable_backup_restore", source)
        self.assertNotIn("self.web_enable_message_send", source)

        web_entrypoint = SOURCE_ROOT / "xiuxian" / "xiuxian_web" / "__init__.py"
        entrypoint_source = web_entrypoint.read_text(encoding="utf-8")
        self.assertNotIn("web_auth_is_configured", entrypoint_source)
        self.assertIn("initialize_web_storage()", entrypoint_source)

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

    def test_skill_learning_uses_transactional_service(self) -> None:
        back_root = SOURCE_ROOT / "xiuxian" / "xiuxian_back"
        command_source = (back_root / "__init__.py").read_text(encoding="utf-8")
        service_source = (back_root / "skill_learning_service.py").read_text(
            encoding="utf-8"
        )
        start = command_source.index("async def confirm_use_(")
        end = command_source.index("@use_item.handle", start)
        command = command_source[start:end]

        self.assertIn("skill_learning_service.learn(", command)
        self.assertNotIn("sql_message.update_back_j(", command)
        self.assertNotIn("updata_user_", command)
        self.assertIn("BEGIN IMMEDIATE", service_source)
        self.assertIn("skill_learning_operations", service_source)

    def test_cultivation_item_use_is_atomic_and_idempotent(self) -> None:
        back_root = SOURCE_ROOT / "xiuxian" / "xiuxian_back"
        command_source = (back_root / "__init__.py").read_text(encoding="utf-8")
        utility_source = (back_root / "back_util.py").read_text(encoding="utf-8")
        service_source = (back_root / "cultivation_item_service.py").read_text(
            encoding="utf-8"
        )
        start = command_source.index('elif goods_type == "神物"')
        end = command_source.index('elif goods_type == "聚灵旗"', start)
        command = command_source[start:end]
        growth_branch = command[command.index("                exp = goods_info['buff'] * num"):]

        self.assertIn("cultivation_item_service.apply(", growth_branch)
        self.assertNotIn("sql_message.update_exp(", growth_branch)
        self.assertNotIn("sql_message.update_user_attribute(", growth_branch)
        self.assertNotIn("sql_message.update_back_j(", growth_branch)
        elixir_start = utility_source.index('elif goods_info[\'buff_type\'] == "exp_up"')
        elixir_end = utility_source.index("    else:\n        msg =", elixir_start)
        elixir_branch = utility_source[elixir_start:elixir_end]
        self.assertIn("cultivation_item_service.apply(", elixir_branch)
        self.assertNotIn("sql_message.update_exp(", elixir_branch)
        self.assertNotIn("sql_message.update_user_attribute(", elixir_branch)
        self.assertNotIn("sql_message.update_back_j(", elixir_branch)
        self.assertIn("BEGIN IMMEDIATE", service_source)
        self.assertIn("cultivation_item_operations", service_source)

    def test_three_cultivation_pill_use_is_atomic_and_idempotent(self) -> None:
        back_root = SOURCE_ROOT / "xiuxian" / "xiuxian_back"
        command_source = (back_root / "__init__.py").read_text(encoding="utf-8")
        service_source = (back_root / "three_cultivation_pill_service.py").read_text(
            encoding="utf-8"
        )
        start = command_source.index("async def use_three_cultivation_pill(")
        end = command_source.index("\n\n@chakan_wupin.handle", start)
        command = command_source[start:end]

        self.assertIn("three_cultivation_pill_service.apply(", command)
        self.assertNotIn("sql_message.update_exp(", command)
        self.assertNotIn("sql_message.update_user_attribute(", command)
        self.assertNotIn("sql_message.update_back_j(", command)
        self.assertIn("BEGIN IMMEDIATE", service_source)
        self.assertIn("three_cultivation_pill_operations", service_source)

    def test_breakthrough_rate_elixir_use_is_atomic_and_idempotent(self) -> None:
        back_root = SOURCE_ROOT / "xiuxian" / "xiuxian_back"
        utility_source = (back_root / "back_util.py").read_text(encoding="utf-8")
        service_source = (back_root / "breakthrough_rate_item_service.py").read_text(
            encoding="utf-8"
        )
        start = utility_source.index('if goods_info[\'buff_type\'] == "level_up_rate"')
        end = utility_source.index('elif goods_info[\'buff_type\'] == "hp"', start)
        command = utility_source[start:end]

        self.assertGreaterEqual(command.count("breakthrough_rate_item_service.apply("), 2)
        self.assertNotIn("sql_message.update_levelrate(", command)
        self.assertNotIn("sql_message.update_back_j(", command)
        self.assertIn("BEGIN IMMEDIATE", service_source)
        self.assertIn("breakthrough_rate_item_operations", service_source)

    def test_recovery_elixir_use_is_atomic_and_idempotent(self) -> None:
        back_root = SOURCE_ROOT / "xiuxian" / "xiuxian_back"
        utility_source = (back_root / "back_util.py").read_text(encoding="utf-8")
        service_source = (back_root / "recovery_item_service.py").read_text(
            encoding="utf-8"
        )
        start = utility_source.index('elif goods_info[\'buff_type\'] == "hp"')
        end = utility_source.index('elif goods_info[\'buff_type\'] == "atk_buff"', start)
        command = utility_source[start:end]

        self.assertGreaterEqual(command.count("recovery_item_service.apply("), 4)
        self.assertNotIn("sql_message.update_user_hp_mp(", command)
        self.assertNotIn("sql_message.update_user_hp(", command)
        self.assertNotIn("sql_message.update_user_stamina(", command)
        self.assertNotIn("sql_message.update_back_j(", command)
        self.assertIn("BEGIN IMMEDIATE", service_source)
        self.assertIn("recovery_item_operations", service_source)

    def test_permanent_attack_elixir_use_is_atomic_and_idempotent(self) -> None:
        back_root = SOURCE_ROOT / "xiuxian" / "xiuxian_back"
        utility_source = (back_root / "back_util.py").read_text(encoding="utf-8")
        service_source = (back_root / "permanent_atk_item_service.py").read_text(
            encoding="utf-8"
        )
        start = utility_source.index('elif goods_info[\'buff_type\'] == "atk_buff"')
        end = utility_source.index('elif goods_info[\'buff_type\'] == "exp_up"', start)
        command = utility_source[start:end]

        self.assertGreaterEqual(command.count("permanent_atk_item_service.apply("), 2)
        self.assertNotIn("sql_message.updata_user_atk_buff(", command)
        self.assertNotIn("sql_message.update_back_j(", command)
        self.assertIn("BEGIN IMMEDIATE", service_source)
        self.assertIn("permanent_atk_item_operations", service_source)

    def test_unbind_charm_use_is_atomic_and_idempotent(self) -> None:
        back_root = SOURCE_ROOT / "xiuxian" / "xiuxian_back"
        command_source = (back_root / "__init__.py").read_text(encoding="utf-8")
        service_source = (back_root / "unbind_item_service.py").read_text(
            encoding="utf-8"
        )
        start = command_source.index("async def use_unbind_charm(")
        end = command_source.index("\nasync def use_spirit_stone_bag", start)
        command = command_source[start:end]

        self.assertIn("unbind_item_service.apply(", command)
        self.assertNotIn("sql_message.unbind_item(", command)
        self.assertNotIn("sql_message.update_back_j(", command)
        self.assertIn("BEGIN IMMEDIATE", service_source)
        self.assertIn("unbind_item_operations", service_source)

    def test_sect_owner_transfer_uses_transactional_service(self) -> None:
        sect_root = SOURCE_ROOT / "xiuxian" / "xiuxian_sect"
        command_source = (sect_root / "__init__.py").read_text(encoding="utf-8")
        service_source = (sect_root / "membership_service.py").read_text(
            encoding="utf-8"
        )
        self.assertIn("sect_membership_service.transfer_owner(", command_source)
        self.assertIn("BEGIN IMMEDIATE", service_source)
        self.assertIn("sect_operations", service_source)

    def test_sign_in_uses_transactional_idempotent_service(self) -> None:
        base_root = SOURCE_ROOT / "xiuxian" / "xiuxian_base"
        command_source = (base_root / "__init__.py").read_text(encoding="utf-8")
        service_source = (base_root / "sign_service.py").read_text(encoding="utf-8")
        self.assertIn("sign_in_service.sign(", command_source)
        self.assertNotIn("sql_message.get_sign(user_id)", command_source)
        self.assertIn("BEGIN IMMEDIATE", service_source)
        self.assertIn("sign_in_operations", service_source)

    def test_auction_session_uses_central_json_store(self) -> None:
        source = (
            SOURCE_ROOT / "xiuxian" / "xiuxian_trade" / "auction_config.py"
        ).read_text(encoding="utf-8")
        self.assertNotIn("json.loads(", source)
        self.assertNotIn("json.dumps(", source)
        self.assertNotIn("write_text(", source)
        self.assertIn("load_json_file(", source)
        self.assertIn("save_json_file(", source)
        self.assertIn("delete_json_file(", source)

    def test_auction_background_jobs_use_observable_boundary(self) -> None:
        trade_root = SOURCE_ROOT / "xiuxian" / "xiuxian_trade"
        command_source = (trade_root / "__init__.py").read_text(encoding="utf-8")
        jobs_source = (trade_root / "auction_jobs.py").read_text(encoding="utf-8")
        self.assertIn('run_auction_job("auto_start"', command_source)
        self.assertIn('run_auction_job("end_check"', command_source)
        self.assertIn('"startup_reconcile"', command_source)
        self.assertIn("failure_count", jobs_source)
        self.assertIn('return "database"', jobs_source)
        self.assertIn('return "filesystem"', jobs_source)

    def test_normal_compensation_claim_uses_transactional_service(self) -> None:
        compensation_root = SOURCE_ROOT / "xiuxian" / "xiuxian_compensation"
        common_source = (compensation_root / "common.py").read_text(encoding="utf-8")
        service_source = (compensation_root / "reward_service.py").read_text(
            encoding="utf-8"
        )
        claim_body = common_source.split("async def claim_normal_reward", 1)[1].split(
            "\ndef delete_record", 1
        )[0]
        self.assertIn("reward_claim_service.claim(", claim_body)
        self.assertNotIn("send_reward_to_user(", claim_body)
        self.assertNotIn("mark_claimed(", claim_body)
        self.assertIn("BEGIN IMMEDIATE", service_source)
        self.assertIn("reward_claims", service_source)

    def test_redeem_code_uses_transactional_limited_claim(self) -> None:
        compensation_root = SOURCE_ROOT / "xiuxian" / "xiuxian_compensation"
        source = (compensation_root / "redeem_code.py").read_text(encoding="utf-8")
        self.assertIn("reward_claim_service.claim(", source)
        self.assertIn("usage_limit=usage_limit", source)
        self.assertNotIn("send_reward_to_user(", source)
        self.assertNotIn("mark_claimed(", source)

    def test_dungeon_team_json_fields_use_typed_normalization(self) -> None:
        source = (
            SOURCE_ROOT / "xiuxian" / "xiuxian_dungeon" / "team_manager.py"
        ).read_text(encoding="utf-8")
        self.assertIn("def _normalize_team_record", source)
        self.assertIn("except (json.JSONDecodeError, TypeError, ValueError)", source)
        self.assertNotIn(
            'teams[team_id]["members"] = json.loads',
            source,
        )

    def test_command_disable_uses_central_json_store(self) -> None:
        source = (SOURCE_ROOT / "xiuxian" / "command_disable.py").read_text(
            encoding="utf-8"
        )
        self.assertNotIn("json.loads(", source)
        self.assertNotIn("json.dumps(", source)
        self.assertNotIn("with open(", source)
        self.assertIn("load_json_file(", source)
        self.assertIn("save_json_file(", source)

    def test_sect_fairyland_upgrade_uses_transactional_service(self) -> None:
        sect_root = SOURCE_ROOT / "xiuxian" / "xiuxian_sect"
        command_source = (sect_root / "__init__.py").read_text(encoding="utf-8")
        service_source = (sect_root / "membership_service.py").read_text(
            encoding="utf-8"
        )
        self.assertIn("sect_membership_service.upgrade_fairyland(", command_source)
        self.assertIn("sect_fairyland_operations", service_source)
        self.assertIn("BEGIN IMMEDIATE", service_source)

    def test_sect_elixir_room_upgrade_uses_transactional_service(self) -> None:
        sect_root = SOURCE_ROOT / "xiuxian" / "xiuxian_sect"
        command_source = (sect_root / "__init__.py").read_text(encoding="utf-8")
        service_source = (sect_root / "membership_service.py").read_text(
            encoding="utf-8"
        )
        self.assertIn(
            "sect_membership_service.upgrade_elixir_room(", command_source
        )
        self.assertIn('"elixir_room_upgrade"', command_source)
        self.assertIn("sect_elixir_room_operations", service_source)
        self.assertIn("BEGIN IMMEDIATE", service_source)

    def test_sect_mainbuff_search_uses_transactional_service(self) -> None:
        sect_root = SOURCE_ROOT / "xiuxian" / "xiuxian_sect"
        command_source = (sect_root / "__init__.py").read_text(encoding="utf-8")
        service_source = (sect_root / "membership_service.py").read_text(
            encoding="utf-8"
        )
        self.assertIn("sect_membership_service.apply_buff_search(", command_source)
        self.assertIn('"mainbuff_search"', command_source)
        self.assertIn("sect_buff_search_operations", service_source)
        self.assertNotIn(
            "sql_message.update_sect_mainbuff(sect_id, sql)", command_source
        )

    def test_sect_secbuff_search_uses_transactional_service(self) -> None:
        sect_root = SOURCE_ROOT / "xiuxian" / "xiuxian_sect"
        command_source = (sect_root / "__init__.py").read_text(encoding="utf-8")
        self.assertIn("sect_membership_service.apply_buff_search(", command_source)
        self.assertIn('"secbuff_search"', command_source)
        self.assertIn('"secondary"', command_source)
        self.assertNotIn(
            "sql_message.update_sect_secbuff(sect_id, sql)", command_source
        )

    def test_auction_runtime_tables_use_main_trade_repository(self) -> None:
        trade_root = SOURCE_ROOT / "xiuxian" / "xiuxian_trade"
        command_source = (trade_root / "__init__.py").read_text(encoding="utf-8")
        service_source = (trade_root / "auction_service.py").read_text(
            encoding="utf-8"
        )
        repository_source = (trade_root / "repository.py").read_text(
            encoding="utf-8"
        )
        self.assertIn("auction_current_from_trade_db_v1", repository_source)
        self.assertIn("auction_history_from_trade_db_v1", repository_source)
        self.assertIn("auction_repository.get_current_auction", service_source)
        self.assertIn("auction_repository.settle_auction_item", service_source)
        self.assertIn("auction_settlement_operations", repository_source)
        self.assertIn('conn.execute("BEGIN IMMEDIATE")', repository_source)
        self.assertNotIn("trade_manager.get_current_auction", command_source)
        self.assertNotIn("trade_manager.get_auction_history", command_source)

    def test_world_boss_state_uses_central_json_store(self) -> None:
        source = (
            SOURCE_ROOT / "xiuxian" / "xiuxian_boss" / "old_boss_info.py"
        ).read_text(encoding="utf-8")
        self.assertIn("load_json_file(", source)
        self.assertIn("update_json_file(", source)
        self.assertNotIn("json.load(", source)

    def test_attack_practice_upgrade_uses_transactional_service(self) -> None:
        sect_root = SOURCE_ROOT / "xiuxian" / "xiuxian_sect"
        command_source = (sect_root / "__init__.py").read_text(encoding="utf-8")
        service_source = (sect_root / "membership_service.py").read_text(
            encoding="utf-8"
        )
        self.assertIn("sect_membership_service.upgrade_practice(", command_source)
        self.assertIn('"health_practice_upgrade"', command_source)
        self.assertIn('"mana_practice_upgrade"', command_source)
        self.assertIn("sect_practice_operations", service_source)
        self.assertIn("BEGIN IMMEDIATE", service_source)

    def test_direct_breakthrough_uses_transactional_service(self) -> None:
        base_root = SOURCE_ROOT / "xiuxian" / "xiuxian_base"
        command_source = (base_root / "breakthrough_tribulation.py").read_text(
            encoding="utf-8"
        )
        service_source = (base_root / "breakthrough_service.py").read_text(
            encoding="utf-8"
        )
        self.assertIn("breakthrough_service.apply_failure(", command_source)
        self.assertIn("breakthrough_service.apply_success(", command_source)
        self.assertIn("direct_breakthrough_operations", service_source)
        self.assertIn("BEGIN IMMEDIATE", service_source)

    def test_tribulation_breakthrough_uses_transactional_service(self) -> None:
        base_root = SOURCE_ROOT / "xiuxian" / "xiuxian_base"
        command_source = (base_root / "breakthrough_tribulation.py").read_text(
            encoding="utf-8"
        )
        service_source = (base_root / "breakthrough_service.py").read_text(
            encoding="utf-8"
        )
        self.assertIn(
            "breakthrough_service.apply_tribulation_failure(", command_source
        )
        self.assertIn(
            "breakthrough_service.apply_tribulation_success(", command_source
        )
        self.assertIn('"tribulation_gold"', command_source)
        self.assertIn("tribulation_breakthrough_operations", service_source)

    def test_continuous_breakthrough_uses_transactional_service(self) -> None:
        base_root = SOURCE_ROOT / "xiuxian" / "xiuxian_base"
        command_source = (base_root / "breakthrough_tribulation.py").read_text(
            encoding="utf-8"
        )
        service_source = (base_root / "breakthrough_service.py").read_text(
            encoding="utf-8"
        )
        self.assertIn("breakthrough_service.apply_continuous(", command_source)
        self.assertIn('"continuous"', command_source)
        self.assertIn("continuous_breakthrough_operations", service_source)

    def test_continuous_tribulation_uses_transactional_service(self) -> None:
        base_root = SOURCE_ROOT / "xiuxian" / "xiuxian_base"
        command_source = (base_root / "breakthrough_tribulation.py").read_text(
            encoding="utf-8"
        )
        service_source = (base_root / "breakthrough_service.py").read_text(
            encoding="utf-8"
        )
        self.assertIn(
            "breakthrough_service.apply_continuous_tribulation(", command_source
        )
        self.assertIn('"continuous_tribulation"', command_source)
        self.assertIn('"continuous_tribulation_gold"', command_source)
        self.assertIn("continuous_tribulation_operations", service_source)

    def test_destiny_pill_fusion_uses_transactional_service(self) -> None:
        base_root = SOURCE_ROOT / "xiuxian" / "xiuxian_base"
        command_source = (base_root / "breakthrough_tribulation.py").read_text(
            encoding="utf-8"
        )
        service_source = (base_root / "pill_fusion_service.py").read_text(
            encoding="utf-8"
        )
        start = command_source.index("@fusion_destiny_pill.handle")
        end = command_source.index("@start_tribulation.handle", start)
        command = command_source[start:end]

        self.assertEqual(command.count("pill_fusion_service.apply("), 2)
        self.assertNotIn("sql_message.get_back_msg(", command)
        self.assertNotIn("sql_message.update_back_j(", command)
        self.assertNotIn("sql_message.send_back(", command)
        self.assertIn("BEGIN IMMEDIATE", service_source)
        self.assertIn("pill_fusion_operations", service_source)

    def test_player_rename_uses_transactional_service(self) -> None:
        base_root = SOURCE_ROOT / "xiuxian" / "xiuxian_base"
        command_source = (base_root / "__init__.py").read_text(encoding="utf-8")
        service_source = (base_root / "player_rename_service.py").read_text(
            encoding="utf-8"
        )
        start = command_source.index("async def remaname_(")
        end = command_source.index("@run_xiuxian.handle", start)
        command = command_source[start:end]

        self.assertIn("player_rename_service.rename_user(", command)
        self.assertIn("player_rename_service.rename_root(", command)
        self.assertNotIn("sql_message.update_ls(", command)
        self.assertNotIn("sql_message.update_back_j(", command)
        self.assertNotIn("sql_message.update_user_name(", command)
        self.assertNotIn("sql_message.update_root_name(", command)
        self.assertIn("BEGIN IMMEDIATE", service_source)
        self.assertIn("player_rename_operations", service_source)

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

    def test_user_info_image_rendering_does_not_block_event_loop(self) -> None:
        source = (
            SOURCE_ROOT / "xiuxian" / "xiuxian_info" / "draw_user_info.py"
        ).read_text(encoding="utf-8")
        self.assertIn("await asyncio.to_thread(_render_user_info", source)
        self.assertIn("await asyncio.to_thread(_prepare_and_cache_background", source)
        self.assertNotIn("return await convert_img(final_img)", source)

    def test_sect_rename_uses_transactional_service(self) -> None:
        source = (
            SOURCE_ROOT / "xiuxian" / "xiuxian_sect" / "__init__.py"
        ).read_text(encoding="utf-8")
        start = source.index("async def sect_rename_")
        end = source.index("@create_sect.handle", start)
        command = source[start:end]
        self.assertIn("sect_membership_service.rename_sect(", command)
        self.assertNotIn("sql_message.update_sect_name(", command)
        self.assertNotIn("sql_message.update_back_j(", command)
        self.assertNotIn("sql_message.update_sect_used_stone(", command)

    def test_sect_member_removal_commands_use_transactional_service(self) -> None:
        source = (
            SOURCE_ROOT / "xiuxian" / "xiuxian_sect" / "__init__.py"
        ).read_text(encoding="utf-8")

        kick_start = source.index("async def sect_kick_out_")
        out_start = source.index("async def sect_out_", kick_start)
        donate_start = source.index("async def sect_donate_", out_start)
        kick_command = source[kick_start:out_start]
        out_command = source[out_start:donate_start]

        self.assertIn("sect_membership_service.kick_member(", kick_command)
        self.assertNotIn("sql_message.update_usr_sect(", kick_command)
        self.assertNotIn("sql_message.update_user_sect_contribution(", kick_command)

        self.assertIn("sect_membership_service.leave_sect(", out_command)
        self.assertNotIn("sql_message.update_usr_sect(", out_command)
        self.assertNotIn("sql_message.update_user_sect_contribution(", out_command)

    def test_sect_position_update_uses_transactional_service(self) -> None:
        source = (
            SOURCE_ROOT / "xiuxian" / "xiuxian_sect" / "__init__.py"
        ).read_text(encoding="utf-8")
        start = source.index("async def sect_position_update_")
        end = source.index("@join_sect.handle", start)
        command = source[start:end]

        self.assertIn("sect_membership_service.change_position(", command)
        self.assertNotIn("sql_message.update_usr_sect(", command)

    def test_sect_task_completion_preserves_period_and_initializes_message(self) -> None:
        sect_root = SOURCE_ROOT / "xiuxian" / "xiuxian_sect"
        member_utils = (sect_root / "sect_member_utils.py").read_text(encoding="utf-8")
        source = (sect_root / "__init__.py").read_text(encoding="utf-8")
        start = source.index("async def sect_task_complete_")
        end = source.index("@sect_owner_change.handle", start)
        command = source[start:end]

        self.assertIn("userstask[user_id] = dict(task)", member_utils)
        self.assertIn('msg = ""', command)
        self.assertIn('userstask[user_id]["period"]', command)

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
