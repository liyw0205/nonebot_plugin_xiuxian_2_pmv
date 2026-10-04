from __future__ import annotations

import hashlib
import io
import json
import unittest
from contextlib import redirect_stdout, redirect_stderr
from unittest.mock import patch

from scripts.phase2_legacy_path_gate import evaluate_phase2_scope, load_phase2_scope_report, main


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
    membership = [{key: item.get(key) for key in ("id", "kind", "entry")} for item in items]
    payload = json.dumps(membership, ensure_ascii=False, separators=(",", ":"), sort_keys=True).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _source_projection(inventory: dict, fields: list[str]) -> dict:
    return {field: inventory.get(field) for field in fields}


class Phase2LegacyPathGateTests(unittest.TestCase):
    def test_frozen_repository_scope_has_call_graphs_and_reports_open_paths(self):
        report = load_phase2_scope_report(include_items=True)

        self.assertFalse(report["ready"])
        self.assertTrue(report["frozen_membership_valid"])
        self.assertEqual(report["path_count"], 496)
        self.assertEqual(
            report["status_counts"],
            {"不可达": 19, "允许保留的兼容路径": 41, "受阻": 429, "已迁移": 7},
        )
        self.assertGreater(report["blocked_count"], 0)
        self.assertTrue(all(item["call_graph"] and item["evidence"] for item in report["items"]))
        self.assertIn(
            "admin.player_status.reset.single",
            {item["id"] for item in report["items"]},
        )
        news_query = next(
            item for item in report["items"] if item["id"] == "command:entertainment:60S读世界"
        )
        self.assertEqual(news_query["status"], "允许保留的兼容路径")
        self.assertTrue(any("http_client.get_json" in edge for edge in news_query["call_graph"]))
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
