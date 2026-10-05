from __future__ import annotations

import hashlib
import io
import json
import unittest
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


class Phase2LegacyPathGateTests(unittest.TestCase):
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

    def test_frozen_repository_scope_has_call_graphs_and_reports_open_paths(self):
        report = load_phase2_scope_report(include_items=True)

        self.assertFalse(report["ready"])
        self.assertTrue(report["frozen_membership_valid"])
        self.assertEqual(report["path_count"], 496)
        self.assertEqual(
            report["status_counts"],
            {"不可达": 19, "允许保留的兼容路径": 87, "受阻": 266, "已迁移": 124},
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
