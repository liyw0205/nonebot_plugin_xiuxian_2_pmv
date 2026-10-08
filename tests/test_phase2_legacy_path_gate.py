from __future__ import annotations

import ast
import hashlib
import io
import json
import unittest
from pathlib import Path
from contextlib import redirect_stdout, redirect_stderr
from unittest.mock import patch

from scripts.phase2_legacy_path_gate import (
    _refreshed_command_graph,
    evaluate_phase2_scope,
    load_phase2_scope_report,
    main,
)


def _snapshot_hash(inventory: dict, fields: list[str]) -> str:
    projection = {field: inventory.get(field) for field in fields}
    payload = json.dumps(
        projection,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _membership_hash(items: list[dict]) -> str:
    membership = [
        {
            key: item.get(key)
            for key in ("id", "kind", "entry", "source", "legacy_command_exclusion")
        }
        for item in items
    ]
    payload = json.dumps(membership, ensure_ascii=False, separators=(",", ":"), sort_keys=True).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _source_projection(inventory: dict, fields: list[str]) -> dict:
    return {field: inventory.get(field) for field in fields}


def _handler_line(source: str, handler: str) -> int:
    path = Path(__file__).resolve().parents[1] / source
    return next(node.lineno for node in ast.parse(path.read_text(encoding="utf-8")).body
                if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == handler)


class Phase2LegacyPathGateTests(unittest.TestCase):
    def test_sect_frozen_commands_have_source_bound_owners_and_no_remaining_blocker(self):
        report = load_phase2_scope_report(include_items=True)
        items = {item["id"]: item for item in report["items"]}
        source = "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_sect/__init__.py"
        expected = {
            "加入宗门": ("join_sect_", "join", "sect_member_join_operations", "legacy_sect_member_join.py"),
            "学习宗门功法": ("sect_mainbuff_learn_", "learn_main", "sect_mainbuff_learn_operations", "legacy_sect_main_buff_learn.py"),
        }
        for name, (handler, action, receipt, compatibility) in expected.items():
            with self.subTest(command=name):
                item = items[f"command:sect:{name}"]
                line = _handler_line(source, handler)
                self.assertEqual(item["status"], "已迁移")
                self.assertNotIn("unknown_edge", item)
                edges = [edge for edge in item["call_graph"]
                         if edge.startswith(f"{source}:{line} {handler} ->")]
                self.assertEqual(len(edges), 1)
                self.assertIn(f"SectApplication.{action}", edges[0])
                self.assertIn(f"{source}:{line}", item["evidence"])
                graph = " ".join(item["call_graph"])
                self.assertIn(f"SectRenameSqlRepository.{action}", graph)
                self.assertIn(receipt, graph)
                self.assertIn("explicit rollback-only", graph)
                self.assertIn(compatibility, " ".join(item["evidence"]))
                self.assertNotIn("legacy downstream effect not closed", graph)
        self.assertFalse(any(item["status"] == "受阻" for item in report["items"]
                             if (item.get("source") or {}).get("feature") == "sect"))
        self.assertEqual(report["path_count"], 496)
        self.assertEqual(report["status_counts"],
                         {"不可达": 19, "允许保留的兼容路径": 135, "受阻": 42, "已迁移": 300})
        self.assertTrue(report["frozen_membership_valid"])
        self.assertEqual(report["integrity_errors"], [])

    def test_beg_commands_have_source_bound_command_owners_and_no_remaining_blocker(self):
        report = load_phase2_scope_report(include_items=True)
        items = {item["id"]: item for item in report["items"]}
        source = "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_beg/__init__.py"
        expected = {
            "仙途奇缘": ("beg_stone_", "daily_settle", "BegRepository.settle_daily"),
            "仙途奇缘帮助": ("beg_help_", "help", "render_beg_reply"),
            "新手礼包": ("novice_", "novice_claim", "BegRepository.claim_novice"),
        }
        for name, (handler, action, terminal) in expected.items():
            with self.subTest(command=name):
                item = items[f"command:beg:{name}"]
                line = _handler_line(source, handler)
                self.assertEqual(item["status"], "已迁移")
                self.assertNotIn("unknown_edge", item)
                edges = [edge for edge in item["call_graph"] if edge.startswith(f"{source}:{line} {handler} ->")]
                self.assertEqual(len(edges), 1)
                self.assertIn(f"BegCommandApplication.execute({action})", edges[0])
                self.assertIn(terminal, edges[0])
                self.assertIn(f"{source}:{line}", item["evidence"])
                self.assertIn("tests/test_beg_command_ingress.py", item["evidence"])
                if action != "help":
                    self.assertIn("read-only BegCommandRepository.receipt/profile", edges[0])
                    self.assertIn("BegApplication.execute", edges[0])
        self.assertIn("separate activity transaction", " ".join(items["command:beg:仙途奇缘"]["call_graph"]))
        self.assertIn("without business DB access", " ".join(items["command:beg:仙途奇缘帮助"]["call_graph"]))
        self.assertFalse(any(item["status"] == "受阻" for item in report["items"]
                             if (item.get("source") or {}).get("feature") == "beg"))
        self.assertEqual(len(report["items"]), 496)
        self.assertTrue(report["frozen_membership_valid"])
        self.assertEqual(report["integrity_errors"], [])

    def test_bank_sub_boundary_has_command_owner_but_non_command_family_stays_blocked(self):
        root = Path(__file__).resolve().parents[1]
        report = load_phase2_scope_report(include_items=True)
        family = next(item for item in report["items"] if item["id"] == "legacy.matcher.non_command_dispatch")
        summary = json.loads((root / "docs/refactor_phase2_legacy_paths.json").read_text(encoding="utf-8"))
        declared = next(item for item in summary["explicit_paths"] if item["id"] == family["id"])
        self.assertEqual(family["status"], "受阻")
        for field in ("status", "reason", "call_graph", "evidence"):
            self.assertEqual(family[field], declared[field])
        graph = " ".join(family["call_graph"])
        for owner in ("bank_ -> BankCommandApplication.execute", "BankCommandReceiptRepository.find",
                      "BankDepositApplication", "BankWithdrawalApplication", "BankUpgradeApplication",
                      "BankInterestApplication", "BankAccountInfoApplication.get_info", "render_bank_reply",
                      "expected_saved_stone", "original expected_saved_at", "calculate_interest"):
            self.assertIn(owner, graph)
        self.assertIn("validates user/action/original amount before current account/configuration", graph)
        self.assertIn("information bypasses mutation receipts", graph)
        self.assertNotIn("legacy handler still owns result-to-reply mapping", graph)
        self.assertIn("only the bank sub-boundary, not the matcher family", family["reason"])
        self.assertIn("savef(sync_snapshot=False)", graph)
        self.assertIn("handle_group_lifecycle", graph)
        self.assertIn("media_parse_link", graph)
        for evidence in ("nonebot_plugin_xiuxian_2/features/bank/command_application.py",
                         "nonebot_plugin_xiuxian_2/features/bank/command_receipt_repository.py",
                         "nonebot_plugin_xiuxian_2/features/bank/command_replies.py",
                         "tests/test_bank_command_ingress.py"):
            self.assertIn(evidence, family["evidence"])
        self.assertEqual(len(report["items"]), 496)
        self.assertTrue(report["frozen_membership_valid"])
        self.assertEqual(report["integrity_errors"], [])

    def test_admin_qqid_has_source_bound_recoverable_batch_owner(self):
        report = load_phase2_scope_report(include_items=True)
        item = next(item for item in report["items"] if item["id"] == "command:admin:转换QQID")
        source = "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_admin/__init__.py"
        line = _handler_line(source, "migrate_qqid_cmd_")
        self.assertEqual(item["status"], "已迁移")
        self.assertNotIn("unknown_edge", item)
        edges = [edge for edge in item["call_graph"]
                 if edge.startswith(f"{source}:{line} migrate_qqid_cmd_ ->")]
        self.assertEqual(len(edges), 1)
        for owner in ("AdminQqidApplication.run", "AdminQqidBatchRepository",
                      "AdminApplication.update_user_id", "AdminIdUpdateSqlRepository"):
            self.assertIn(owner, edges[0])
        self.assertIn(f"{source}:{line}", item["evidence"])
        self.assertIn("tests/test_admin_qqid_conversion.py", item["evidence"])
        self.assertIn("legacy.admin.008", " ".join(item["call_graph"]))
        self.assertIn("not globally atomic", item["reason"])
        self.assertFalse(any(entry["status"] == "受阻" for entry in report["items"]
                             if (entry.get("source") or {}).get("feature") == "admin"))
        self.assertTrue(report["frozen_membership_valid"])
        self.assertEqual(report["integrity_errors"], [])

    def test_admin_runtime_controls_have_source_bound_feature_owner_edges(self):
        report = load_phase2_scope_report(include_items=True)
        items = {item["id"]: item for item in report["items"]}
        source = "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_admin/__init__.py"
        expected = {
            "生成秘境": ("create_new_rift_", "RiftApplication.generate", "RiftGenerationSqlRepository.generate", "test_admin_rift_generation.py"),
            "重载items": ("items_refresh_", "AdminItemCatalogApplication.reload", "AdminItemCatalogRepository.reload", "test_admin_items_reload.py"),
            "用户伪装": ("impersonate_user_command_", "AdminImpersonationApplication", "AdminImpersonationRepository", "test_admin_impersonation.py"),
        }
        for name, (handler, application, repository, suite) in expected.items():
            with self.subTest(command=name):
                line = _handler_line(source, handler)
                item = items[f"command:admin:{name}"]
                self.assertEqual(item["status"], "已迁移")
                self.assertNotIn("unknown_edge", item)
                owner_edges = [edge for edge in item["call_graph"]
                               if edge.startswith(f"{source}:{line} {handler} ->") and application in edge]
                self.assertEqual(len(owner_edges), 1)
                self.assertIn(repository, owner_edges[0])
                self.assertIn(f"{source}:{line}", item["evidence"])
                self.assertIn("tests/" + suite, item["evidence"])
        self.assertTrue(report["frozen_membership_valid"])
        self.assertEqual(report["integrity_errors"], [])

    def test_admin_broadcast_commands_have_source_bound_feature_owner_edges(self):
        report = load_phase2_scope_report(include_items=True)
        items = {item["id"]: item for item in report["items"]}
        source = "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_admin/__init__.py"
        expected = {
            "群聊广播": ("group_broadcast_cmd_", "start", "create/claim/finish"),
            "私聊广播": ("private_broadcast_cmd_", "start", "create/claim/finish"),
            "全局广播": ("global_broadcast_cmd_", "start", "create/claim/finish"),
            "查看广播": ("view_broadcast_cmd_", "status", "status"),
            "取消广播": ("cancel_broadcast_cmd_", "cancel", "cancel"),
            "清空广播": ("clear_broadcast_cmd_", "clear", "clear"),
        }
        for name, (handler, application_method, repository_method) in expected.items():
            with self.subTest(command=name):
                line = _handler_line(source, handler)
                item = items[f"command:admin:{name}"]
                self.assertEqual(item["status"], "已迁移")
                self.assertNotIn("unknown_edge", item)
                self.assertIn(f"{source}:{line}", item["evidence"])
                edges = [edge for edge in item["call_graph"]
                         if edge.startswith(f"{source}:{line} {handler} ->")]
                self.assertEqual(len(edges), 1)
                self.assertIn("broadcast_manager.", edges[0])
                self.assertIn(f"AdminBroadcastApplication.{application_method}", edges[0])
                self.assertIn(f"AdminBroadcastRepository.{repository_method}", edges[0])
                graph = " ".join(item["call_graph"])
                self.assertIn("AdminBroadcastHistoryRepository.targets", graph)
                self.assertIn("read-only" if application_method != "start" else "read_only=True", graph)
                self.assertIn("sender", graph)
                self.assertIn("process-local", graph + item["reason"])
                self.assertIn("in flight", (graph + item["reason"]).replace("-", " "))
                self.assertNotIn("effect not closed", graph)
                for evidence in (
                    "nonebot_plugin_xiuxian_2/features/admin/tests/test_broadcast_application.py",
                    "nonebot_plugin_xiuxian_2/features/admin/tests/test_broadcast_history_repository.py",
                    "tests/test_admin_broadcast.py",
                ):
                    self.assertIn(evidence, item["evidence"])
        self.assertTrue(report["frozen_membership_valid"])
        self.assertEqual(report["integrity_errors"], [])

    def test_admin_command_controls_have_source_bound_shared_owner_edges(self):
        report = load_phase2_scope_report(include_items=True)
        items = {item["id"]: item for item in report["items"]}
        source = "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_admin/command_controls.py"
        expected = {
            "指令禁用": (28, "cmd_disable_", "apply_disable_targets"),
            "指令解禁": (53, "cmd_enable_", "apply_disable_targets"),
            "指令列表": (101, "cmd_list_", "collect_command_list_rows"),
        }
        for name, (line, handler, method) in expected.items():
            with self.subTest(command=name):
                item = items[f"command:admin:{name}"]
                self.assertEqual(item["status"], "已迁移")
                self.assertNotIn("unknown_edge", item)
                edges = [edge for edge in item["call_graph"]
                         if edge.startswith(f"{source}:{line} {handler} ->")]
                self.assertEqual(len(edges), 1)
                self.assertIn(f"AdminCommandControlApplication.{method}", edges[0])
                self.assertIn(f"AdminCommandControlRepository.{method}", edges[0])
                self.assertIn("command_disable.json", " ".join(item["call_graph"]))
                self.assertIn(f"{source}:{line}", item["evidence"])
                self.assertIn("tests/test_admin_command_control.py", item["evidence"])
        self.assertTrue(report["frozen_membership_valid"])
        self.assertEqual(report["integrity_errors"], [])

    def test_admin_output_commands_are_compatibility_with_behavioral_evidence(self):
        report = load_phase2_scope_report(include_items=True)
        items = {item["id"]: item for item in report["items"]}
        event_names = ("消息信息", "取链接", "取raw", "取reply")
        output_names = ("修仙手册", "广播帮助", "艾特测试", "按钮测试", "全量申请")
        for name in event_names + output_names:
            with self.subTest(command=name):
                item = items[f"command:admin:{name}"]
                self.assertEqual(item["status"], "允许保留的兼容路径")
                self.assertNotIn("unknown_edge", item)
                suite = "event_debug" if name in event_names else "output_commands"
                self.assertIn(f"tests/test_admin_{suite}_compat.py", item["evidence"])
                self.assertTrue(any("record_send_message" in edge for edge in item["call_graph"]))
        self.assertEqual(report["integrity_errors"], [])

    def test_admin_blackhouse_commands_have_source_bound_sql_owner_edges(self):
        report = load_phase2_scope_report(include_items=True)
        items = {item["id"]: item for item in report["items"]}
        expected = {
            "小黑屋": ("blackhouse_", "AdminBlackhouseSqlRepository.snapshot/set_banned"),
            "解除小黑屋": ("unblackhouse_", "AdminBlackhouseSqlRepository.snapshot/set_banned"),
            "查看小黑屋": ("view_blackhouse_", "AdminBlackhouseSqlRepository.list_banned"),
        }
        source = "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_admin/__init__.py"
        for name, (handler, terminal) in expected.items():
            with self.subTest(command=name):
                line = _handler_line(source, handler)
                item = items[f"command:admin:{name}"]
                self.assertEqual(item["status"], "已迁移")
                self.assertNotIn("unknown_edge", item)
                self.assertIn(f"{source}:{line}", item["evidence"])
                handler_edges = [edge for edge in item["call_graph"]
                                 if edge.startswith(f"{source}:{line} {handler} ->")]
                self.assertEqual(sum(terminal in edge for edge in handler_edges), 1)
                self.assertFalse(any("effect not closed" in edge for edge in item["call_graph"]))
                for evidence in (
                    "tests/test_admin_blackhouse_status.py",
                    "tests/test_admin_blackhouse_routing.py",
                    "tests/test_admin_blackhouse_migration.py",
                    "nonebot_plugin_xiuxian_2/features/admin/tests/test_blackhouse_repository.py",
                ):
                    self.assertIn(evidence, item["evidence"])
                graph = " ".join(item["call_graph"])
                self.assertIn("legacy.admin.007", graph)
                self.assertIn("legacy_json_and_is_ban_v1", graph)
                self.assertIn("admin_blackhouse_users", graph)
                self.assertIn("_filter_blackhoused_matchers", graph)
        self.assertTrue(report["frozen_membership_valid"])
        self.assertEqual(report["integrity_errors"], [])

    def test_command_binding_refresh_preserves_custom_downstream_edges(self):
        existing_graph = [
            "initialized NoneBot -> old command path",
            "legacy/back.py:10 back_cmd = on_command('我的背包') -> compatibility matcher",
            "legacy/back.py:19 back_cmd_ -> legacy downstream effect not closed in this frozen item",
            "legacy/back.py:19 back_cmd_ -> BackApplication.show_inventory -> BackSqlRepository.snapshot",
            "BackSqlRepository.snapshot -> read-only inventory query",
        ]
        record = {
            "file": "legacy/back.py",
            "line": 11,
            "name": "我的背包",
            "matcher_names": ["back_cmd"],
            "aliases": [],
            "handlers": [{"name": "back_cmd_", "line": 22}],
        }

        call_graph, evidence = _refreshed_command_graph(existing_graph, (record,))

        self.assertTrue(any("legacy/back.py:11 back_cmd = on_command" in edge for edge in call_graph))
        self.assertTrue(
            any("legacy/back.py:22 back_cmd_ -> BackApplication.show_inventory" in edge for edge in call_graph)
        )
        self.assertIn("BackSqlRepository.snapshot -> read-only inventory query", call_graph)
        self.assertFalse(any("legacy downstream effect not closed" in edge for edge in call_graph))
        self.assertIn("legacy/back.py:22", evidence)

    def test_command_binding_refresh_preserves_branch_qualified_edges(self):
        existing = [
            "legacy/admin.py:19 reset_ no-argument branch -> BatchRepository.reset",
            "legacy/admin.py:19 reset_ mention branch -> SingleRepository.reset",
        ]
        records = ({
            "file": "legacy/admin.py", "line": 11, "name": "reset",
            "matcher_names": ["reset"], "aliases": [],
            "handlers": [{"name": "reset_", "line": 25}],
        },)
        graph, evidence = _refreshed_command_graph(existing, records)
        for edge in existing:
            self.assertIn(edge.replace(":19 ", ":25 "), graph)
        self.assertFalse(any("legacy downstream effect not closed" in edge for edge in graph))
        self.assertIn("legacy/admin.py:25", evidence)

    def test_status_help_compatibility_and_system_info_application_owner(self):
        report = load_phase2_scope_report(include_items=True)
        items = {item["id"]: item for item in report["items"]}
        source = "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_status/__init__.py"

        help_item = items["command:status:插件帮助"]
        help_line = _handler_line(source, "handle_status")
        self.assertEqual(help_item["status"], "允许保留的兼容路径")
        self.assertNotIn("unknown_edge", help_item)
        help_graph = " ".join(help_item["call_graph"])
        self.assertIn(f"{source}:89 status_cmd = on_command('插件帮助') -> compatibility matcher", help_graph)
        self.assertIn(
            f"{source}:{help_line} handle_status -> static help text/buttons -> send_help_message -> handle_send",
            help_graph,
        )
        self.assertIn("Cooldown(cd_time=0) -> shared config/policy admission", help_graph)
        self.assertIn("shared message-history delivery", help_graph)
        self.assertIn(f"{source}:{help_line}", help_item["evidence"])
        self.assertIn(f"{source}:187", help_item["evidence"])
        self.assertIn("tests/test_phase2_legacy_path_gate.py", help_item["evidence"])
        self.assertNotIn("legacy downstream effect not closed", help_graph)

        system_item = items["command:status:系统信息"]
        system_line = _handler_line(source, "handle_sys_info")
        self.assertEqual(system_item["status"], "已迁移")
        self.assertNotIn("unknown_edge", system_item)
        system_graph = " ".join(system_item["call_graph"])
        self.assertIn(f"{source}:87 sys_info_cmd = on_command('系统信息') -> compatibility matcher", system_graph)
        self.assertIn(
            f"{source}:{system_line} handle_sys_info -> get_system_info -> StatusApplication.system_info -> SystemInfoProvider.snapshot -> SystemInfoSnapshot.render",
            system_graph,
        )
        self.assertIn("SystemInfoProvider.snapshot -> platform and optional psutil host metrics only", system_graph)
        self.assertIn("no feature persistence, repository, or ledger", system_graph)
        self.assertIn("handle_sys_info -> handle_send -> shared message-history delivery", system_graph)
        self.assertIn("Cooldown(cd_time=0) -> shared config/policy admission", system_graph)
        for evidence in (
            f"{source}:175",
            f"{source}:159-161,174-178",
            "nonebot_plugin_xiuxian_2/features/status/application.py:40-42",
            "nonebot_plugin_xiuxian_2/features/status/system_info.py:28-38,41-137",
            "tests/test_status_system_info_contract.py",
            "tests/test_phase2_legacy_path_gate.py",
        ):
            self.assertIn(evidence, system_item["evidence"])
        self.assertNotIn("legacy downstream effect not closed", system_graph)
        self.assertEqual(report["status_counts"],
                         {"不可达": 19, "允许保留的兼容路径": 135, "受阻": 42, "已迁移": 300})
        self.assertTrue(report["frozen_membership_valid"])
        self.assertEqual(report["integrity_errors"], [])

    def test_dashboard_web_routes_share_status_application_owners(self):
        report = load_phase2_scope_report(include_items=True)
        items = {item["id"]: item for item in report["items"]}
        source = "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_web/system.py"
        expected = {
            "get_stats": (
                "/get_stats",
                ("_collect_dashboard_stats", "StatusApplication.dashboard_stats", "DashboardStatsSqlRepository.snapshot"),
                (
                    "nonebot_plugin_xiuxian_2/features/status/dashboard_stats_repository.py",
                    "nonebot_plugin_xiuxian_2/features/status/tests/test_dashboard_stats_repository.py",
                ),
            ),
            "get_system_info_extended": (
                "/get_system_info_extended",
                ("_collect_system_snapshot", "StatusApplication.system_info", "SystemInfoProvider.snapshot"),
                (
                    "nonebot_plugin_xiuxian_2/features/status/system_info.py",
                    "tests/test_status_system_info_contract.py",
                ),
            ),
            "get_process_info": (
                "/get_process_info",
                ("_collect_process_snapshot(5)", "StatusApplication.process_info", "ProcessInfoProvider.snapshot"),
                (
                    "nonebot_plugin_xiuxian_2/features/status/process_info.py",
                    "nonebot_plugin_xiuxian_2/features/status/tests/test_process_info.py",
                ),
            ),
            "api_dashboard_summary": (
                "/api/dashboard/summary",
                (
                    "_collect_dashboard_stats",
                    "StatusApplication.dashboard_stats",
                    "DashboardStatsSqlRepository.snapshot",
                    "_collect_system_snapshot",
                    "StatusApplication.system_info",
                    "_collect_process_snapshot(5)",
                    "StatusApplication.process_info",
                ),
                (
                    "nonebot_plugin_xiuxian_2/features/status/dashboard_stats_repository.py",
                    "nonebot_plugin_xiuxian_2/features/status/system_info.py",
                    "nonebot_plugin_xiuxian_2/features/status/process_info.py",
                ),
            ),
        }

        for handler, (route_path, owners, evidence_paths) in expected.items():
            with self.subTest(route=route_path):
                item = items[f"route:GET:{route_path}:{source}:{handler}"]
                line = _handler_line(source, handler)
                self.assertEqual(item["status"], "已迁移")
                self.assertNotIn("unknown_edge", item)
                graph = " ".join(item["call_graph"])
                self.assertIn(f"{source}:{line} {handler} ->", graph)
                for owner in owners:
                    self.assertIn(owner, graph)
                self.assertIn(f"{source}:{line}", item["evidence"])
                self.assertIn("tests/test_dashboard_system_routes.py", item["evidence"])
                for evidence_path in evidence_paths:
                    self.assertTrue(
                        any(
                            evidence == evidence_path or evidence.startswith(f"{evidence_path}:")
                            for evidence in item["evidence"]
                        )
                    )
                self.assertNotIn("downstream state effect not closed", graph)

        self.assertEqual(report["status_counts"],
                         {"不可达": 19, "允许保留的兼容路径": 135, "受阻": 42, "已迁移": 300})
        self.assertEqual(report["path_count"], 496)
        self.assertTrue(report["frozen_membership_valid"])
        self.assertEqual(report["integrity_errors"], [])

    def test_tasks_commands_share_progress_and_claim_application_owners(self):
        report = load_phase2_scope_report(include_items=True)
        items = {item["id"]: item for item in report["items"]}
        source = "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_tasks/__init__.py"
        expected = {
            "周常任务": (16, "weekly_task_", 78, "build_status_message(weekly)"),
            "我的任务": (14, "task_info_", 30, "build_status_message(all cycles)"),
            "每日任务": (15, "daily_task_", 54, "build_status_message(daily)"),
        }
        for name, (declaration, handler, line, edge) in expected.items():
            with self.subTest(command=name):
                item = items[f"command:tasks:{name}"]
                graph = " ".join(item["call_graph"])
                self.assertEqual(item["status"], "已迁移")
                self.assertNotIn("unknown_edge", item)
                self.assertIn(f"{source}:{declaration}", item["evidence"])
                self.assertIn(f"{source}:{line}", item["evidence"])
                self.assertIn(
                    f"{source}:{line} {handler} -> task_manager.{edge} -> "
                    "TaskProgressApplication.read_states -> TasksProgressRepository.read_states",
                    graph,
                )
                self.assertIn("read-only player projection", graph)
                self.assertIn("tests/test_tasks_command_owner.py", item["evidence"])
                self.assertNotIn("legacy downstream effect not closed", graph)

        claim = items["command:tasks:领取任务奖励"]
        claim_graph = " ".join(claim["call_graph"])
        self.assertEqual(claim["status"], "已迁移")
        self.assertNotIn("unknown_edge", claim)
        self.assertIn(f"{source}:17", claim["evidence"])
        self.assertIn(f"{source}:102", claim["evidence"])
        self.assertIn(
            f"{source}:102 claim_task_ -> task_manager.claim_rewards -> "
            "TaskClaimApplication.claim_rewards -> TaskClaimPlayerRepository.prepare/finalize + "
            "TaskClaimGameRepository.grant/finalize",
            claim_graph,
        )
        self.assertIn("operation ledger/outbox/reconcile", claim_graph)
        self.assertIn("tests/test_task_reward_claim.py", claim["evidence"])
        self.assertIn("tests/test_tasks_command_owner.py", claim["evidence"])
        self.assertNotIn("legacy downstream effect not closed", claim_graph)
        self.assertEqual(
            report["status_counts"],
            {"不可达": 19, "允许保留的兼容路径": 135, "受阻": 42, "已迁移": 300},
        )
        self.assertTrue(report["frozen_membership_valid"])
        self.assertEqual(report["integrity_errors"], [])

    def test_title_commands_close_existing_owners_without_reopening_transactions(self):
        report = load_phase2_scope_report(include_items=True)
        items = {item["id"]: item for item in report["items"]}

        migrated = {
            "装备称号": ("TitleRepository.equip", "nonebot_plugin_xiuxian_2/features/title/tests/test_title_application.py"),
            "卸下称号": ("TitleRepository.unequip", "nonebot_plugin_xiuxian_2/features/title/tests/test_title_application.py"),
            "我的称号": ("TitleEligibilityApplication.read_snapshot", "nonebot_plugin_xiuxian_2/features/title/tests/test_title_eligibility.py"),
            "我的成就": ("TitleEligibilityApplication.read_snapshot", "nonebot_plugin_xiuxian_2/features/title/tests/test_title_eligibility.py"),
            "检查称号": ("TitleEligibilityApplication.read_snapshot", "nonebot_plugin_xiuxian_2/features/title/tests/test_title_eligibility.py"),
            "检查成就": ("TitleEligibilityApplication.read_snapshot", "nonebot_plugin_xiuxian_2/features/title/tests/test_title_eligibility.py"),
        }
        for name, (owner, test_path) in migrated.items():
            with self.subTest(command=name):
                item = items[f"command:title:{name}"]
                graph = " ".join(item["call_graph"])
                self.assertEqual(item["status"], "已迁移")
                self.assertNotIn("unknown_edge", item)
                self.assertNotIn("legacy downstream effect not closed", graph)
                self.assertIn(owner, graph)
                if name in {"装备称号", "卸下称号"}:
                    self.assertIn("title_application.get_result/get_state", graph)
                elif name in {"我的成就", "检查成就"}:
                    self.assertIn("get_title_achievement_records", graph)
                self.assertIn(test_path, item["evidence"])
                self.assertIn("tests/test_phase2_legacy_path_gate.py", item["evidence"])

        compatible = {
            "刷新称号": ("refresh_title_cache clears the in-memory catalog", "title_data.py:28-43,586-592"),
            "称号帮助": ("static __title_help__ text/buttons", "__init__.py:608-650"),
            "称号详情": ("static catalog lookup", "title_data.py:28-68"),
            "赠送称号": (
                "single-target lookup -> _sql_message().get_user_info_with_id/get_user_info_with_name",
                "features/title/application.py:31-37,194-211",
            ),
        }
        for name, (edge, evidence_suffix) in compatible.items():
            with self.subTest(command=name):
                item = items[f"command:title:{name}"]
                graph = " ".join(item["call_graph"])
                self.assertEqual(item["status"], "允许保留的兼容路径")
                self.assertNotIn("unknown_edge", item)
                self.assertNotIn("legacy downstream effect not closed", graph)
                self.assertIn(edge, graph)
                self.assertTrue(any(evidence_suffix in evidence for evidence in item["evidence"]))
                self.assertIn("tests/test_phase2_legacy_path_gate.py", item["evidence"])

        self.assertFalse(any(
            items[f"command:title:{name}"]["status"] == "受阻"
            for name in ("我的称号", "我的成就", "检查称号", "检查成就")
        ))

        self.assertEqual(
            report["status_counts"],
            {"不可达": 19, "允许保留的兼容路径": 135, "受阻": 42, "已迁移": 300},
        )

    def test_world_events_demon_claim_command_owns_atomic_claim_statistic(self):
        report = load_phase2_scope_report(include_items=True)
        item = next(
            item for item in report["items"]
            if item["id"] == "command:world_events:领取魔修奖励"
        )
        source = "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_world_events/__init__.py"
        handler_line = _handler_line(source, "claim_demon_reward_")
        graph = " ".join(item["call_graph"])

        self.assertEqual(item["status"], "已迁移")
        self.assertNotIn("unknown_edge", item)
        self.assertIn(f"{source}:191", item["evidence"])
        self.assertIn(f"{source}:{handler_line}", item["evidence"])
        self.assertIn(
            f"{source}:{handler_line} claim_demon_reward_ ->", graph
        )
        self.assertIn("DemonClaimApplication.claim", graph)
        self.assertIn("WorldEventClaimSqlRepository.claim", graph)
        self.assertIn("player statistics increment", graph)
        self.assertIn("world_events.007", item["reason"])
        self.assertIn("tests/test_demon_claim_repository.py", item["evidence"])
        self.assertNotIn(
            "update_statistics_value",
            (Path(__file__).resolve().parents[1] / source).read_text(encoding="utf-8"),
        )
        self.assertEqual(report["integrity_errors"], [])
        self.assertEqual(
            report["status_counts"],
            {"不可达": 19, "允许保留的兼容路径": 135, "受阻": 42, "已迁移": 300},
        )
        self.assertTrue(report["frozen_membership_valid"])

    def test_tianti_display_commands_share_existing_read_owners(self):
        report = load_phase2_scope_report(include_items=True)
        items = {item["id"]: item for item in report["items"]}
        source = "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_tianti/__init__.py"

        my_tianti = items["command:tianti:我的炼体"]
        my_tianti_graph = " ".join(my_tianti["call_graph"])
        self.assertEqual(my_tianti["status"], "已迁移")
        self.assertNotIn("unknown_edge", my_tianti)
        self.assertIn(f"{source}:54", my_tianti["evidence"])
        self.assertIn(f"{source}:600", my_tianti["evidence"])
        self.assertIn(f"{source}:600 _ -> TiantiTrainingApplication.read_profile", my_tianti_graph)
        self.assertIn("TiantiProfileSqlReader.read -> calculate_tianti_gain_rate", my_tianti_graph)
        self.assertIn("_get_tianti_sect_fairyland_level -> sect_application.get_sect_info", my_tianti_graph)
        self.assertIn("tests/test_tianti_frozen_entry_owner.py", my_tianti["evidence"])

        my_qiaoxue = items["command:tianti:我的体窍"]
        my_qiaoxue_graph = " ".join(my_qiaoxue["call_graph"])
        self.assertEqual(my_qiaoxue["status"], "已迁移")
        self.assertNotIn("unknown_edge", my_qiaoxue)
        self.assertIn(f"{source}:56", my_qiaoxue["evidence"])
        self.assertIn(f"{source}:743", my_qiaoxue["evidence"])
        self.assertIn(f"{source}:743 _ -> TiantiTrainingApplication.read_profile", my_qiaoxue_graph)
        self.assertIn("TiantiProfileSqlReader.read -> calc_qiaoxue_bonus", my_qiaoxue_graph)
        self.assertIn("legacy static qiaoxue catalog adapter", my_qiaoxue_graph)
        self.assertIn("tests/test_tianti_frozen_entry_owner.py", my_qiaoxue["evidence"])

        static_commands = {
            "炼体帮助": (50, 195),
            "炼体境界": (57, 816),
        }
        for name, (declaration, handler_line) in static_commands.items():
            with self.subTest(command=name):
                item = items[f"command:tianti:{name}"]
                graph = " ".join(item["call_graph"])
                self.assertEqual(item["status"], "允许保留的兼容路径")
                self.assertNotIn("unknown_edge", item)
                self.assertIn(f"{source}:{declaration}", item["evidence"])
                self.assertIn(f"{source}:{handler_line}", item["evidence"])
                self.assertIn("static message only; no Tianti state or asset access", graph)
                self.assertNotIn("legacy downstream effect not closed", graph)

        self.assertEqual(
            report["status_counts"],
            {"不可达": 19, "允许保留的兼容路径": 135, "受阻": 42, "已迁移": 300},
        )
        self.assertTrue(report["frozen_membership_valid"])
        self.assertEqual(report["integrity_errors"], [])

    def test_command_registry_web_routes_share_the_command_control_owner(self):
        report = load_phase2_scope_report(include_items=True)
        items = {item["id"]: item for item in report["items"]}
        source = "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_web/command_registry_web.py"
        expected = {
            "route:GET:/command_registry:" + source + ":command_registry": (
                50,
                "command_registry",
                "AdminCommandControlApplication.collect_command_list_rows",
                "tests/test_command_registry_routes.py:74-120",
            ),
            "route:POST:/api/command_registry/toggle:" + source + ":api_command_registry_toggle": (
                72,
                "api_command_registry_toggle",
                "AdminCommandControlApplication.set_command_disabled",
                "tests/test_command_registry_routes.py:141-219",
            ),
            "route:POST:/api/command_registry/bulk_toggle:" + source + ":api_command_registry_bulk_toggle": (
                97,
                "api_command_registry_bulk_toggle",
                "AdminCommandControlApplication.apply_disable_targets",
                "tests/test_command_registry_routes.py:221-281",
            ),
        }
        route_items = [
            item for item in report["items"]
            if item.get("kind") == "legacy_routes"
            and (item.get("source") or {}).get("file") == source
        ]
        self.assertEqual({item["id"] for item in route_items}, set(expected))
        for item_id, (line, function, owner, test_evidence) in expected.items():
            with self.subTest(route=item_id):
                item = items[item_id]
                self.assertEqual(item["status"], "已迁移")
                self.assertNotIn("unknown_edge", item)
                self.assertIn(f"{source}:{line}", item["evidence"])
                self.assertIn(test_evidence, item["evidence"])
                self.assertTrue(any(
                    edge.startswith(f"{source}:{line} {function} -> {owner}")
                    for edge in item["call_graph"]
                ))

        self.assertEqual(report["integrity_errors"], [])

    def test_frozen_repository_scope_has_call_graphs_and_reports_open_paths(self):
        report = load_phase2_scope_report(include_items=True)

        self.assertFalse(report["ready"])
        self.assertTrue(report["frozen_membership_valid"])
        self.assertEqual(report["path_count"], 496)
        self.assertEqual(
            report["status_counts"],
            {"不可达": 19, "允许保留的兼容路径": 135, "受阻": 42, "已迁移": 300},
        )
        self.assertGreater(report["blocked_count"], 0)
        self.assertTrue(all(item["call_graph"] and item["evidence"] for item in report["items"]))
        self.assertEqual(report["default_legacy_command_count"], 637)
        self.assertEqual(report["default_legacy_command_backlog_count"], 323)
        self.assertEqual(len(report["backlog"]), 328)
        self.assertEqual(
            report["default_legacy_command_discovery"],
            "static_ast_startup_import_closure_not_live_runtime_observation",
        )
        self.assertFalse(any("Explicit path summary" in error for error in report["integrity_errors"]))
        self.assertEqual(report["path_count"], 496)
        self.assertIn(
            "admin.player_status.reset.single",
            {item["id"] for item in report["items"]},
        )
        id_update = next(item for item in report["items"] if item["id"] == "command:admin:ID更新")
        self.assertEqual(id_update["status"], "已迁移")
        self.assertTrue(any("AdminApplication.update_user_id" in edge for edge in id_update["call_graph"]))
        self.assertTrue(any("AdminIdUpdateSqlRepository._resume" in edge for edge in id_update["call_graph"]))
        bot_info = next(item for item in report["items"] if item["id"] == "command:status:bot信息")
        self.assertEqual(bot_info["status"], "已迁移")
        self.assertTrue(any("BotOverviewSqlRepository.snapshot" in edge for edge in bot_info["call_graph"]))
        my_id = next(item for item in report["items"] if item["id"] == "command:info:我的ID")
        self.assertEqual(my_id["status"], "已迁移")
        self.assertTrue(any("AvatarStateSqlRepository.get_active_id" in edge for edge in my_id["call_graph"]))
        interactive = [
            item for item in report["items"]
            if item.get("kind") == "commands" and (item.get("source") or {}).get("feature") == "interactive"
        ]
        self.assertEqual(len(interactive), 21)
        self.assertEqual(sum(item["status"] == "已迁移" for item in interactive), 4)
        self.assertEqual(sum(item["status"] == "允许保留的兼容路径" for item in interactive), 17)

        for name in ("早安", "晚安", "给点修为", "给点灵石"):
            item = next(entry for entry in interactive if entry["source"]["name"] == name)
            self.assertTrue(any("InteractiveApplication.execute" in edge for edge in item["call_graph"]))
            self.assertTrue(any("OperationLedger" in edge for edge in item["call_graph"]))
        legacy_fortune = next(
            item for item in report["items"] if item["id"] == "legacy-command-suppressed:今日运势"
        )
        self.assertEqual(legacy_fortune["status"], "不可达")
        backlog_command = next(
            item for item in report["backlog"] if item["id"] == "default-legacy-command:back:我的背包"
        )
        self.assertIn("xiuxian_back", " ".join(backlog_command["call_graph"]))
        self.assertNotIn(backlog_command["id"], {item["id"] for item in report["items"]})
        news_query = next(
            item for item in report["items"] if item["id"] == "command:entertainment:60S读世界"
        )
        self.assertEqual(news_query["status"], "允许保留的兼容路径")
        self.assertTrue(any("http_client.get_json" in edge for edge in news_query["call_graph"]))
        dm = next(item for item in report["items"] if item["id"] == "command:admin:dm")
        self.assertEqual(dm["status"], "允许保留的兼容路径")
        self.assertTrue(any("delivery_service.reply" in edge for edge in dm["call_graph"]))
        self.assertTrue(any("message_db.py" in evidence for evidence in dm["evidence"]))
        markdown_template = next(
            item for item in report["items"] if item["id"] == "command:admin:md模板"
        )
        self.assertEqual(markdown_template["status"], "允许保留的兼容路径")
        self.assertTrue(
            any("MessageSegment.markdown_template" in edge for edge in markdown_template["call_graph"])
        )
        self.assertTrue(
            any("message_db.py" in evidence for evidence in markdown_template["evidence"])
        )
        steam_query = next(
            item for item in report["items"] if item["id"] == "command:entertainment:Steam喜加一"
        )
        self.assertEqual(steam_query["status"], "允许保留的兼容路径")
        self.assertTrue(
            any(
                "1 MiB" in edge and "streaming GET" in edge
                for edge in steam_query["call_graph"]
            )
        )
        self.assertTrue(any("http_proxy.py:187-225" in evidence for evidence in steam_query["evidence"]))
        newapi_info = next(
            item for item in report["items"] if item["id"] == "command:entertainment:newapi信息"
        )
        self.assertEqual(newapi_info["status"], "已迁移")
        self.assertTrue(any("EntertainmentRepository.resolve_info_targets" in edge for edge in newapi_info["call_graph"]))
        self.assertTrue(any("streaming JSON response cap 512 KiB" in edge for edge in newapi_info["call_graph"]))
        newapi_list = next(
            item for item in report["items"] if item["id"] == "command:entertainment:newapi查看"
        )
        self.assertEqual(newapi_list["status"], "已迁移")
        self.assertTrue(
            any("EntertainmentRepository.list_account_summaries" in edge for edge in newapi_list["call_graph"])
        )
        newapi_checkin = next(
            item for item in report["items"] if item["id"] == "command:entertainment:newapi签到"
        )
        self.assertEqual(newapi_checkin["status"], "已迁移")
        self.assertTrue(
            any("EntertainmentRepository.resolve_checkin_targets" in edge for edge in newapi_checkin["call_graph"])
        )
        self.assertTrue(
            any("EntertainmentRepository.append_checkin_history" in edge for edge in newapi_checkin["call_graph"])
        )
        self.assertTrue(any("1 MiB" in edge for edge in newapi_checkin["call_graph"]))
        for webdav_name in ("webdav查看", "webdav列表", "webdav信息", "webdav绑定", "webdav删除", "webdav链接", "webdav文件"):
            webdav = next(
                item for item in report["items"] if item["id"] == f"command:entertainment:{webdav_name}"
            )
            self.assertEqual(webdav["status"], "已迁移")
            self.assertTrue(any("EntertainmentApplication" in edge for edge in webdav["call_graph"]))
        newapi_checkin_risk = next(
            item for item in report["backlog"] if item["id"] == "newapi-checkin-remote-history-window"
        )
        self.assertEqual(newapi_checkin_risk["source"], "command:entertainment:newapi签到")
        newapi_delete = next(
            item for item in report["items"] if item["id"] == "command:entertainment:newapi删除"
        )
        self.assertEqual(newapi_delete["status"], "已迁移")
        self.assertTrue(any("EntertainmentApplication.delete_accounts" in edge for edge in newapi_delete["call_graph"]))
        self.assertTrue(any("immediate SQLite UoW" in edge for edge in newapi_delete["call_graph"]))
        newapi_help = next(
            item for item in report["items"] if item["id"] == "command:entertainment:newapi帮助"
        )
        self.assertEqual(newapi_help["status"], "允许保留的兼容路径")
        self.assertTrue(any("static __NEWAPI_HELP__" in edge for edge in newapi_help["call_graph"]))
        self.assertTrue(
            any("without opening the database or account files" in edge for edge in newapi_help["call_graph"])
        )
        self.assertTrue(
            any("shared operational message-history writer" in edge for edge in newapi_help["call_graph"])
        )
        cooldown_risk = next(
            item for item in report["backlog"] if item["id"] == "legacy-cooldown-rate-map-cardinality"
        )
        self.assertEqual(cooldown_risk["source"], "command:entertainment:newapi帮助")
        self.assertEqual(report["p7_gate"]["status"], "independent")

    def test_web_page_session_and_static_routes_are_explicit_compatibility(self):
        report = load_phase2_scope_report(include_items=True)
        expected_handlers = {
            "route:GET:/:nonebot_plugin_xiuxian_2/xiuxian/xiuxian_web/pages.py:home": ("pages.py:16", "pages.py:16 home"),
            "route:GET:/favicon.ico:nonebot_plugin_xiuxian_2/xiuxian/xiuxian_web/pages.py:favicon": ("pages.py:23", "pages.py:23 favicon"),
            "route:GET,POST:/login:nonebot_plugin_xiuxian_2/xiuxian/xiuxian_web/pages.py:login": ("pages.py:33", "pages.py:33 login"),
            "route:GET:/logout:nonebot_plugin_xiuxian_2/xiuxian/xiuxian_web/pages.py:logout": ("pages.py:49", "pages.py:49 logout"),
            "route:GET:/robots.txt:nonebot_plugin_xiuxian_2/xiuxian/xiuxian_web/pages.py:robots_txt": ("pages.py:29", "pages.py:29 robots_txt"),
            "route:GET:/update:nonebot_plugin_xiuxian_2/xiuxian/xiuxian_web/pages.py:update": ("pages.py:54", "pages.py:54 update"),
        }
        items = {item["id"]: item for item in report["items"]}

        self.assertTrue(expected_handlers.keys() <= items.keys())
        for item_id, (source_line, handler_edge) in expected_handlers.items():
            item = items[item_id]
            self.assertEqual(item["status"], "允许保留的兼容路径")
            source_location = f"nonebot_plugin_xiuxian_2/xiuxian/xiuxian_web/{source_line}"
            self.assertIn(source_location, item["evidence"])
            self.assertTrue(any(handler_edge in edge for edge in item["call_graph"]))
            self.assertFalse(any("downstream state effect not closed" in edge for edge in item["call_graph"]))

        for route in ("check_update", "get_releases", "perform_update"):
            item = next(
                item for item in report["items"]
                if item.get("source", {}).get("function") == route
                and item.get("source", {}).get("file") == "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_web/pages.py"
            )
            self.assertEqual(item["status"], "已迁移")

        backups = next(
            item for item in report["items"]
            if item.get("source", {}).get("function") == "get_backups"
            and item.get("source", {}).get("file") == "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_web/pages.py"
        )
        self.assertEqual(backups["status"], "已迁移")
        self.assertIn("nonebot_plugin_xiuxian_2/xiuxian/xiuxian_web/pages.py:129", backups["evidence"])
        self.assertTrue(
            any(
                "pages.py:129 get_backups -> PluginBackupCatalogApplication.list_plugin_backups"
                in edge
                for edge in backups["call_graph"]
            )
        )
        self.assertFalse(any("downstream state effect not closed" in edge for edge in backups["call_graph"]))

    def test_local_plugin_backup_file_routes_have_source_bound_application_edges(self):
        report = load_phase2_scope_report(include_items=True)
        expected = {
            "batch_delete_backups": (
                "backups.py:316",
                "backups.py:316 batch_delete_backups -> plugin_backup_file_application.delete_plugin_backups",
            ),
            "delete_backup": (
                "backups.py:614",
                "backups.py:614 delete_backup -> plugin_backup_file_application.delete_plugin_backup",
            ),
            "download_backup": (
                "backups.py:596",
                "backups.py:596 download_backup -> plugin_backup_file_application.open_plugin_backup",
            ),
        }
        for function, (source_line, handler_edge) in expected.items():
            item = next(
                item for item in report["items"]
                if item.get("source", {}).get("function") == function
                and item.get("source", {}).get("file") == "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_web/backups.py"
            )
            self.assertEqual(item["status"], "已迁移")
            self.assertIn(f"nonebot_plugin_xiuxian_2/xiuxian/xiuxian_web/{source_line}", item["evidence"])
            self.assertTrue(any(handler_edge in edge for edge in item["call_graph"]))
            self.assertFalse(any("downstream state effect not closed" in edge for edge in item["call_graph"]))

    def test_plugin_backup_restore_routes_have_source_bound_application_edges(self):
        report = load_phase2_scope_report(include_items=True)
        expected = {
            "cloud_restore_backup": (
                "backups.py:108",
                "backups.py:108 cloud_restore_backup -> plugin_backup_cloud_application.local_backup_exists",
            ),
            "restore_backup": (
                "backups.py:210",
                "backups.py:210 restore_backup -> plugin_backup_restore_application.restore_backup",
            ),
        }
        for function, (source_line, edge) in expected.items():
            item = next(
                item for item in report["items"]
                if item.get("source", {}).get("function") == function
                and item.get("source", {}).get("file") == "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_web/backups.py"
            )
            self.assertEqual(item["status"], "已迁移")
            self.assertIn(f"nonebot_plugin_xiuxian_2/xiuxian/xiuxian_web/{source_line}", item["evidence"])
            self.assertTrue(any(edge in call_edge for call_edge in item["call_graph"]))
            self.assertFalse(any("downstream state effect not closed" in edge for edge in item["call_graph"]))

    def test_cloud_plugin_backup_routes_have_source_bound_feature_edges(self):
        report = load_phase2_scope_report(include_items=True)
        expected = {
            "get_cloud_backups": (
                "backups.py:70",
                "backups.py:70 get_cloud_backups -> plugin_backup_cloud_application.list_cloud_backups",
            ),
            "sync_cloud_backup": (
                "backups.py:81",
                "backups.py:81 sync_cloud_backup -> plugin_backup_cloud_application.sync_cloud_backup",
            ),
            "batch_sync_cloud_backups": (
                "backups.py:339",
                "backups.py:339 batch_sync_cloud_backups -> plugin_backup_cloud_application.sync_cloud_backups",
            ),
            "batch_delete_cloud_backups": (
                "backups.py:419",
                "backups.py:419 batch_delete_cloud_backups -> plugin_backup_cloud_application.delete_cloud_backups",
            ),
        }
        for function, (source_line, handler_edge) in expected.items():
            item = next(
                item for item in report["items"]
                if item.get("source", {}).get("function") == function
                and item.get("source", {}).get("file") == "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_web/backups.py"
            )
            self.assertEqual(item["status"], "已迁移")
            self.assertIn(f"nonebot_plugin_xiuxian_2/xiuxian/xiuxian_web/{source_line}", item["evidence"])
            self.assertTrue(any(handler_edge in edge for edge in item["call_graph"]))
            self.assertFalse(any("downstream state effect not closed" in edge for edge in item["call_graph"]))

    def test_config_backup_routes_have_source_bound_feature_edges(self):
        report = load_phase2_scope_report(include_items=True)
        expected = {
            "cloud_backup_config": ("backups.py:141", "backup_cloud_config"),
            "get_cloud_config_backups": ("backups.py:153", "list_cloud_backups"),
            "sync_cloud_config_backup": ("backups.py:165", "sync_cloud_backup"),
            "cloud_restore_config_backup": ("backups.py:190", "restore_cloud_backup"),
            "export_config": ("backups.py:467", "export_config"),
            "import_config": ("backups.py:482", "import_config"),
            "backup_config": ("backups.py:513", "create_local_backup"),
            "get_config_backups": ("backups.py:533", "list_local_backups"),
            "restore_config_backup": ("backups.py:546", "restore_local_backup"),
            "delete_config_backup": ("backups.py:641", "delete_local_backup"),
        }
        for function, (source_line, application_method) in expected.items():
            item = next(
                item for item in report["items"]
                if item.get("source", {}).get("function") == function
                and item.get("source", {}).get("file") == "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_web/backups.py"
            )
            self.assertEqual(item["status"], "已迁移")
            self.assertIn(f"nonebot_plugin_xiuxian_2/xiuxian/xiuxian_web/{source_line}", item["evidence"])
            self.assertTrue(any(
                edge.startswith(f"nonebot_plugin_xiuxian_2/xiuxian/xiuxian_web/{source_line} {function} ->")
                and f"ConfigBackupApplication.{application_method}" in edge
                for edge in item["call_graph"]
            ), (function, item["call_graph"]))
            self.assertFalse(any("downstream state effect not closed" in edge for edge in item["call_graph"]))

    def test_backup_feature_has_no_open_routes_and_manual_orchestration_is_source_bound(self):
        report = load_phase2_scope_report(include_items=True)
        self.assertEqual(report["integrity_errors"], [])
        items = [
            item for item in report["items"]
            if item.get("source", {}).get("file")
            == "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_web/backups.py"
        ]
        self.assertEqual(len(items), 30)
        self.assertTrue(all(item["status"] != "受阻" for item in items))
        page = next(item for item in items if item["source"]["function"] == "backups")
        manual = next(item for item in items if item["source"]["function"] == "manual_backup")
        self.assertEqual(page["status"], "允许保留的兼容路径")
        self.assertEqual(manual["status"], "已迁移")
        self.assertTrue(any("render_template('backups.html')" in edge for edge in page["call_graph"]))
        self.assertTrue(any("ManualBackupApplication.create_backup" in edge for edge in manual["call_graph"]))

    def test_updater_routes_have_source_bound_application_edges(self):
        report = load_phase2_scope_report(include_items=True)
        expected = {
            "check_update": ("pages.py:60", "pages.py:60 check_update -> UpdateApplication.check_update"),
            "get_releases": ("pages.py:90", "pages.py:90 get_releases -> UpdateApplication.latest_releases"),
            "perform_update": ("pages.py:107", "pages.py:107 perform_update -> UpdateApplication.perform_update_with_backup"),
        }
        for function, (source_line, edge) in expected.items():
            item = next(
                item for item in report["items"]
                if item.get("source", {}).get("function") == function
                and item.get("source", {}).get("file") == "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_web/pages.py"
            )
            self.assertEqual(item["status"], "已迁移")
            self.assertIn(f"nonebot_plugin_xiuxian_2/xiuxian/xiuxian_web/{source_line}", item["evidence"])
            self.assertTrue(any(edge in call_edge for call_edge in item["call_graph"]))

    def test_phase2_completes_when_every_frozen_item_is_closed(self):
        inventory = {"commands": [], "legacy_jobs": [], "legacy_routes": []}
        fields = ["commands", "legacy_jobs", "legacy_routes"]
        frozen_items = [
            {
                "id": "migrated",
                "kind": "explicit",
                "entry": "migrated path",
                "status": "已迁移",
                "call_graph": ["entry -> application"],
                "evidence": ["src.py:1"],
            },
            {
                "id": "compat",
                "kind": "explicit",
                "entry": "compat path",
                "status": "允许保留的兼容路径",
                "call_graph": ["entry -> compatibility adapter"],
                "evidence": ["src.py:2"],
            },
            {
                "id": "unreachable",
                "kind": "explicit",
                "entry": "unreachable path",
                "status": "不可达",
                "call_graph": ["no production caller"],
                "evidence": ["src.py:3"],
            },
        ]
        scope = {
            "scope_id": "test-scope",
            "source_snapshot": {"fields": fields, "sha256": _snapshot_hash(inventory, fields)},
            "frozen_membership_sha256": _membership_hash(frozen_items),
            "p7_gate": {"status": "not_ready_yet", "independent": True},
        }

        report = evaluate_phase2_scope(
            scope,
            inventory,
            frozen_items=frozen_items,
            frozen_scope_id="test-scope",
            frozen_source_projection=_source_projection(inventory, fields),
        )

        self.assertTrue(report["ready"])
        self.assertEqual(report["status_counts"]["受阻"], 0)
        self.assertEqual(report["p7_gate"]["status"], "not_ready_yet")

    def test_blocked_status_prevents_completion(self):
        inventory = {"commands": [], "legacy_jobs": [], "legacy_routes": []}
        fields = ["commands", "legacy_jobs", "legacy_routes"]
        frozen_items = [
            {"id": "blocked", "kind": "explicit", "entry": "blocked path", "status": "受阻", "call_graph": ["entry -> legacy"], "evidence": ["x.py:1"]}
        ]
        scope = {
            "scope_id": "test-scope",
            "source_snapshot": {"fields": fields, "sha256": _snapshot_hash(inventory, fields)},
            "frozen_membership_sha256": _membership_hash(frozen_items),
        }

        report = evaluate_phase2_scope(
            scope,
            inventory,
            frozen_items=frozen_items,
            frozen_scope_id="test-scope",
            frozen_source_projection=_source_projection(inventory, fields),
        )

        self.assertFalse(report["ready"])
        self.assertEqual(report["blocked_count"], 1)

    def test_frozen_command_must_be_reachable_unsuppressed_and_handler_bound(self):
        inventory = {"commands": [], "legacy_jobs": [], "legacy_routes": []}
        fields = ["commands", "legacy_jobs", "legacy_routes"]
        frozen_items = [
            {
                "id": "command:back:我的背包",
                "kind": "commands",
                "entry": "back:我的背包",
                "status": "受阻",
                "source": {"feature": "back", "name": "我的背包"},
                "call_graph": ["legacy/back.py:10 back_cmd -> back_cmd_"],
                "evidence": ["legacy/back.py:10", "legacy/back.py:20"],
            }
        ]
        scope = {
            "scope_id": "test-scope",
            "source_snapshot": {"fields": fields, "sha256": _snapshot_hash(inventory, fields)},
            "frozen_membership_sha256": _membership_hash(frozen_items),
        }
        base_record = {
            "file": "legacy/back.py",
            "line": 10,
            "matcher_names": ["back_cmd"],
            "handlers": [{"name": "back_cmd_", "line": 20}],
        }

        for record, expected_error in (
            ({**base_record, "suppressed": True}, "is suppressed"),
            ({**base_record, "suppressed": False, "handlers": []}, "no bound handler"),
        ):
            with self.subTest(expected_error=expected_error):
                report = evaluate_phase2_scope(
                    scope,
                    inventory,
                    frozen_items=frozen_items,
                    frozen_scope_id="test-scope",
                    frozen_source_projection=_source_projection(inventory, fields),
                    default_legacy_commands={("back", "我的背包"): (record,)},
                )
                self.assertFalse(report["ready"])
                self.assertTrue(any(expected_error in error for error in report["integrity_errors"]))

    def test_command_call_graph_must_bind_handler_for_nonblocked_paths(self):
        inventory = {"commands": [], "legacy_jobs": [], "legacy_routes": []}
        fields = ["commands", "legacy_jobs", "legacy_routes"]
        frozen_items = [
            {
                "id": "command:back:我的背包",
                "kind": "commands",
                "entry": "back:我的背包",
                "status": "允许保留的兼容路径",
                "source": {"feature": "back", "name": "我的背包"},
                "call_graph": ["legacy/back.py:10 back_cmd = on_command('我的背包') -> compatibility matcher"],
                "evidence": ["legacy/back.py:10", "legacy/back.py:20"],
            }
        ]
        scope = {
            "scope_id": "test-scope",
            "source_snapshot": {"fields": fields, "sha256": _snapshot_hash(inventory, fields)},
            "frozen_membership_sha256": _membership_hash(frozen_items),
        }
        command = {
            "feature": "back",
            "name": "我的背包",
            "file": "legacy/back.py",
            "line": 10,
            "matcher_names": ["back_cmd"],
            "aliases": [],
            "handlers": [{"name": "back_cmd_", "line": 20}],
            "suppressed": False,
        }

        report = evaluate_phase2_scope(
            scope,
            inventory,
            frozen_items=frozen_items,
            frozen_scope_id="test-scope",
            frozen_source_projection=_source_projection(inventory, fields),
            default_legacy_commands={("back", "我的背包"): (command,)},
        )

        self.assertTrue(
            any("omits source-bound handler back_cmd_" in error for error in report["integrity_errors"])
        )

        frozen_items[0]["call_graph"] = [
            "legacy/back.py:10 back_cmd = on_command('我的背包') -> compatibility matcher",
            "legacy/back.py:20 back_cmd_ -> reviewed downstream call-graph edges",
        ]
        report = evaluate_phase2_scope(
            scope,
            inventory,
            frozen_items=frozen_items,
            frozen_scope_id="test-scope",
            frozen_source_projection=_source_projection(inventory, fields),
            default_legacy_commands={("back", "我的背包"): (command,)},
        )
        self.assertTrue(
            any("Closed command has no source-bound downstream edge" in error for error in report["integrity_errors"])
        )

        frozen_items[0]["call_graph"] = [
            "legacy/back.py:10 back_cmd = on_command('我的背包') -> compatibility matcher",
            "back_cmd_ -> BackApplication.show_inventory",
        ]
        report = evaluate_phase2_scope(
            scope,
            inventory,
            frozen_items=frozen_items,
            frozen_scope_id="test-scope",
            frozen_source_projection=_source_projection(inventory, fields),
            default_legacy_commands={("back", "我的背包"): (command,)},
        )
        self.assertTrue(
            any("omits source-bound handler back_cmd_" in error for error in report["integrity_errors"])
        )

        frozen_items[0]["call_graph"].append(
            "legacy/back.py:20 back_cmd_ -> legacy downstream effect not closed in this frozen item"
        )
        report = evaluate_phase2_scope(
            scope,
            inventory,
            frozen_items=frozen_items,
            frozen_scope_id="test-scope",
            frozen_source_projection=_source_projection(inventory, fields),
            default_legacy_commands={("back", "我的背包"): (command,)},
        )
        self.assertTrue(
            any("Closed command status retains an unresolved downstream edge" in error for error in report["integrity_errors"])
        )

    def test_membership_hash_freezes_source_identity(self):
        inventory = {"commands": [], "legacy_jobs": [], "legacy_routes": []}
        fields = ["commands", "legacy_jobs", "legacy_routes"]
        frozen_items = [
            {
                "id": "command:back:我的背包",
                "kind": "commands",
                "entry": "back:我的背包",
                "status": "已迁移",
                "source": {"feature": "back", "name": "我的背包"},
                "call_graph": ["legacy/back.py:10 back_cmd -> back_cmd_"],
                "evidence": ["legacy/back.py:10", "legacy/back.py:20"],
            }
        ]
        scope = {
            "scope_id": "test-scope",
            "source_snapshot": {"fields": fields, "sha256": _snapshot_hash(inventory, fields)},
            "frozen_membership_sha256": _membership_hash(frozen_items),
        }
        frozen_items[0]["source"] = {"feature": "admin", "name": "ID更新"}

        report = evaluate_phase2_scope(
            scope,
            inventory,
            frozen_items=frozen_items,
            frozen_scope_id="test-scope",
            frozen_source_projection=_source_projection(inventory, fields),
        )

        self.assertFalse(report["frozen_membership_valid"])
        self.assertTrue(any("membership hash is invalid" in error for error in report["integrity_errors"]))

    def test_frozen_job_requires_registered_default_execution_path(self):
        inventory = {"commands": [], "legacy_jobs": ["backup_database_files"], "legacy_routes": []}
        fields = ["commands", "legacy_jobs", "legacy_routes"]
        frozen_items = [
            {
                "id": "job:backup_database_files",
                "kind": "legacy_jobs",
                "entry": "job:backup_database_files",
                "status": "允许保留的兼容路径",
                "source": {"job_id": "backup_database_files"},
                "call_graph": ["old scheduler path"],
                "evidence": ["legacy.py:1"],
            }
        ]
        scope = {
            "scope_id": "test-scope",
            "source_snapshot": {"fields": fields, "sha256": _snapshot_hash(inventory, fields)},
            "frozen_membership_sha256": _membership_hash(frozen_items),
        }

        report = evaluate_phase2_scope(
            scope,
            inventory,
            frozen_items=frozen_items,
            frozen_scope_id="test-scope",
            frozen_source_projection=_source_projection(inventory, fields),
        )

        self.assertTrue(
            any("omits the default manual execution path" in error for error in report["integrity_errors"])
        )

    def test_closed_legacy_route_requires_source_bound_handler_edge(self):
        relative = "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_web/system.py"
        route = {
            "methods": ["GET"],
            "path": "/search_users",
            "file": relative,
            "function": "search_users",
        }
        inventory = {"commands": [], "legacy_jobs": [], "legacy_routes": [route]}
        fields = ["commands", "legacy_jobs", "legacy_routes"]
        location = f"{relative}:{_handler_line(relative, 'search_users')}"

        def evaluate(call_graph, evidence):
            item = {
                "id": f"route:GET:/search_users:{relative}:search_users",
                "kind": "legacy_routes",
                "entry": f"GET /search_users -> {relative}:search_users",
                "status": "已迁移",
                "reason": "Delegates to the feature-owned profile query.",
                "source": {
                    "file": relative,
                    "function": "search_users",
                    "methods": ["GET"],
                    "path": "/search_users",
                },
                "call_graph": call_graph,
                "evidence": evidence,
            }
            scope = {
                "scope_id": "test-scope",
                "source_snapshot": {"fields": fields, "sha256": _snapshot_hash(inventory, fields)},
                "frozen_membership_sha256": _membership_hash([item]),
            }
            return evaluate_phase2_scope(
                scope,
                inventory,
                frozen_items=[item],
                frozen_scope_id="test-scope",
                frozen_source_projection=_source_projection(inventory, fields),
            )

        closed = evaluate(
            [
                f"{location} search_users -> search_users_application -> "
                "PlayerProfileApplication.search_users -> PlayerProfileSqlRepository.search_users"
            ],
            [location],
        )
        self.assertFalse(
            any("Closed legacy Web route" in error for error in closed["integrity_errors"])
        )

        unbound = evaluate(
            ["legacy route -> PlayerProfileApplication.search_users"],
            [location],
        )
        self.assertTrue(
            any("no source-bound handler edge" in error for error in unbound["integrity_errors"])
        )

    def test_new_inventory_entry_is_backlogged_without_expanding_scope(self):
        frozen_inventory = {"commands": [], "legacy_jobs": [], "legacy_routes": []}
        changed_inventory = {**frozen_inventory, "commands": [{"feature": "new", "name": "new command", "aliases": []}]}
        fields = ["commands", "legacy_jobs", "legacy_routes"]
        frozen_items = [
            {"id": "already-closed", "kind": "explicit", "entry": "closed path", "status": "已迁移", "call_graph": ["entry -> application"], "evidence": ["x.py:1"]}
        ]
        scope = {
            "scope_id": "test-scope",
            "source_snapshot": {"fields": fields, "sha256": _snapshot_hash(frozen_inventory, fields)},
            "frozen_membership_sha256": _membership_hash(frozen_items),
            "backlog": [{"id": "candidate", "reason": "review before scope expansion"}],
        }

        report = evaluate_phase2_scope(
            scope,
            changed_inventory,
            frozen_items=frozen_items,
            frozen_scope_id="test-scope",
            frozen_source_projection=_source_projection(frozen_inventory, fields),
            include_items=True,
        )

        self.assertTrue(report["ready"])
        self.assertFalse(report["source_inventory_unchanged"])
        self.assertEqual([item["id"] for item in report["items"]], ["already-closed"])
        self.assertTrue(report["source_inventory_drift_notice"])
        self.assertEqual(report["source_inventory_added"]["commands"], changed_inventory["commands"])
        self.assertEqual(report["backlog"][0], scope["backlog"][0])
        self.assertEqual(
            report["backlog"][1],
            {
                "id": "inventory-drift:commands:new:new command",
                "source_field": "commands",
                "entry": changed_inventory["commands"][0],
                "reason": "Discovered after the Phase 2 scope freeze; review in backlog before any explicit scope-version change.",
            },
        )

    def test_default_legacy_discovery_is_backlogged_without_expanding_scope(self):
        inventory = {"commands": [], "legacy_jobs": [], "legacy_routes": []}
        fields = ["commands", "legacy_jobs", "legacy_routes"]
        frozen_items = [
            {
                "id": "already-closed",
                "kind": "explicit",
                "entry": "closed path",
                "status": "已迁移",
                "call_graph": ["entry -> application"],
                "evidence": ["x.py:1"],
            }
        ]
        scope = {
            "scope_id": "test-scope",
            "source_snapshot": {"fields": fields, "sha256": _snapshot_hash(inventory, fields)},
            "frozen_membership_sha256": _membership_hash(frozen_items),
            "backlog": [],
        }
        discovered = {
            ("back", "我的背包"): (
                {
                    "feature": "back",
                    "name": "我的背包",
                    "file": "legacy/back.py",
                    "line": 10,
                    "matcher_names": ["back_cmd"],
                    "aliases": [],
                    "handlers": [{"name": "back_cmd_", "line": 20}],
                    "suppressed": False,
                },
            )
        }

        report = evaluate_phase2_scope(
            scope,
            inventory,
            frozen_items=frozen_items,
            frozen_scope_id="test-scope",
            frozen_source_projection=_source_projection(inventory, fields),
            default_legacy_commands=discovered,
            include_items=True,
        )

        self.assertTrue(report["ready"])
        self.assertEqual(report["path_count"], 1)
        self.assertEqual(report["default_legacy_command_backlog_count"], 1)
        candidate = report["backlog"][0]
        self.assertEqual(candidate["id"], "default-legacy-command:back:我的背包")
        self.assertTrue(any("back_cmd_" in edge for edge in candidate["call_graph"]))

    def test_invalid_status_and_missing_evidence_fail_closed(self):
        inventory = {"commands": [], "legacy_jobs": [], "legacy_routes": []}
        fields = ["commands", "legacy_jobs", "legacy_routes"]
        frozen_items = [{"id": "bad", "kind": "explicit", "entry": "bad", "status": "maybe", "call_graph": [], "evidence": []}]
        scope = {
            "scope_id": "test-scope",
            "source_snapshot": {"fields": fields, "sha256": _snapshot_hash(inventory, fields)},
            "frozen_membership_sha256": _membership_hash(frozen_items),
        }

        report = evaluate_phase2_scope(
            scope,
            inventory,
            frozen_items=frozen_items,
            frozen_scope_id="test-scope",
            frozen_source_projection=_source_projection(inventory, fields),
        )

        self.assertFalse(report["ready"])
        self.assertGreaterEqual(len(report["integrity_errors"]), 2)

    def test_explicit_path_summary_must_match_frozen_classification(self):
        inventory = {"commands": [], "legacy_jobs": [], "legacy_routes": []}
        fields = ["commands", "legacy_jobs", "legacy_routes"]
        frozen_item = {
            "id": "explicit",
            "kind": "command-effect",
            "entry": "example command effect",
            "status": "已迁移",
            "reason": "The default effect is feature-owned.",
            "call_graph": ["matcher -> application -> repository"],
            "evidence": ["feature/application.py:1"],
        }
        stale_summary = {**frozen_item, "status": "受阻"}
        scope = {
            "scope_id": "test-scope",
            "source_snapshot": {"fields": fields, "sha256": _snapshot_hash(inventory, fields)},
            "frozen_membership_sha256": _membership_hash([frozen_item]),
            "explicit_paths": [stale_summary],
        }

        report = evaluate_phase2_scope(
            scope,
            inventory,
            frozen_items=[frozen_item],
            frozen_scope_id="test-scope",
            frozen_source_projection=_source_projection(inventory, fields),
        )

        self.assertTrue(any("Explicit path summary differs from frozen entry: explicit" in error for error in report["integrity_errors"]))

    def test_reward_center_routes_share_compensation_owner_and_page_is_compatibility(self):
        report = load_phase2_scope_report(include_items=True)
        items = {item["id"]: item for item in report["items"]}
        expected = {
            "route:GET:/api/reward-center/records:nonebot_plugin_xiuxian_2/xiuxian/xiuxian_web/reward_center.py:api_reward_records": (
                "RewardCenterSqlRepository.list_records",
                "tests/test_reward_definition_repository.py",
            ),
            "route:POST:/api/reward-center/records:nonebot_plugin_xiuxian_2/xiuxian/xiuxian_web/reward_center.py:api_save_reward_record": (
                "CompensationApplication.upsert_compensation_definition",
                "tests/test_web_auth.py",
            ),
            "route:DELETE:/api/reward-center/records/<kind>/<record_id>:nonebot_plugin_xiuxian_2/xiuxian/xiuxian_web/reward_center.py:api_delete_reward_record": (
                "delete_reward_definition",
                "tests/test_web_auth.py",
            ),
            "route:POST:/api/reward-center/records/<kind>/clear:nonebot_plugin_xiuxian_2/xiuxian/xiuxian_web/reward_center.py:api_clear_reward_records": (
                "clear_reward_definitions",
                "tests/test_web_auth.py",
            ),
        }
        for item_id, (owner, test_path) in expected.items():
            with self.subTest(route=item_id):
                item = items[item_id]
                graph = " ".join(item["call_graph"])
                self.assertEqual(item["status"], "已迁移")
                self.assertNotIn("unknown_edge", item)
                self.assertIn(owner, graph)
                self.assertNotIn("downstream state effect not closed", graph)
                self.assertTrue(any(test_path in evidence for evidence in item["evidence"]))

        page_id = "route:GET:/reward-center:nonebot_plugin_xiuxian_2/xiuxian/xiuxian_web/reward_center.py:reward_center"
        page = items[page_id]
        self.assertEqual(page["status"], "允许保留的兼容路径")
        self.assertNotIn("unknown_edge", page)
        self.assertIn("authenticated reward_center.html", " ".join(page["call_graph"]))
        self.assertEqual(
            report["status_counts"],
            {"不可达": 19, "允许保留的兼容路径": 135, "受阻": 42, "已迁移": 300},
        )

    def test_legacy_logs_routes_share_file_and_message_owners(self):
        report = load_phase2_scope_report(include_items=True)
        items = {item["id"]: item for item in report["items"]}
        source_file = "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_web/logs.py"
        expected = {
            "route:GET:/api/logs/users": ("api_logs_users", "MessageLogsRepository"),
            "route:GET:/api/logs/user_messages": ("api_logs_user_messages", "MessageLogsRepository"),
            "route:GET:/api/logs/files": ("api_logs_files", "LogFileRepository"),
            "route:GET:/api/logs/read": ("api_logs_read", "LogFileRepository"),
            "route:GET:/api/logs/tail": ("api_logs_tail", "LogFileRepository"),
        }
        for route_prefix, (function, owner) in expected.items():
            item = next(item for key, item in items.items() if key.startswith(route_prefix + ":"))
            graph = " ".join(item["call_graph"])
            with self.subTest(route=route_prefix):
                self.assertEqual(item["status"], "已迁移")
                self.assertNotIn("unknown_edge", item)
                self.assertIn(f"{source_file}:{_handler_line(source_file, function)} {function} ->", graph)
                self.assertIn(owner, graph)
                self.assertTrue(any("tests/test_logs_routes.py" in evidence for evidence in item["evidence"]))

        page = next(item for key, item in items.items() if key.startswith("route:GET:/logs:"))
        self.assertEqual(page["status"], "允许保留的兼容路径")
        self.assertNotIn("unknown_edge", page)
        self.assertIn("render_template(logs.html)", " ".join(page["call_graph"]))
        self.assertEqual(
            report["status_counts"],
            {"不可达": 19, "允许保留的兼容路径": 135, "受阻": 42, "已迁移": 300},
        )

    def test_check_cli_returns_nonzero_for_incomplete_frozen_scope(self):
        report = {
            "scope_id": "test-scope",
            "ready": False,
            "path_count": 1,
            "blocked_count": 1,
            "frozen_membership_valid": True,
            "integrity_errors": [],
            "source_inventory_drift_notice": None,
        }
        output = io.StringIO()
        with patch("scripts.phase2_legacy_path_gate.load_phase2_scope_report", return_value=report):
            with redirect_stdout(output), redirect_stderr(io.StringIO()):
                status = main(["--check"])

        self.assertEqual(status, 1)
        self.assertIn("blocked=1 frozen_membership_valid=True", output.getvalue())


if __name__ == "__main__":
    unittest.main()
