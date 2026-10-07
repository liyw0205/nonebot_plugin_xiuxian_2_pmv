from __future__ import annotations

import ast
import unittest
from pathlib import Path

from scripts.phase2_legacy_path_gate import load_phase2_scope_report


ROOT = Path(__file__).resolve().parents[1]
SOURCE = "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_fusion/__init__.py"


def _handler_line(handler_name: str) -> int:
    tree = ast.parse((ROOT / SOURCE).read_text(encoding="utf-8"))
    return next(
        node.lineno
        for node in tree.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        and node.name == handler_name
    )


class FusionFrozenCommandOwnerTests(unittest.TestCase):
    def test_frozen_commands_have_reviewed_owner_or_compatibility_edges(self) -> None:
        report = load_phase2_scope_report(include_items=True)
        items = {item["id"]: item for item in report["items"]}
        expected = {
            "command:fusion:合成": (
                "fusion_item_",
                "已迁移",
                "general_fusion -> FusionApplication.apply_result/apply -> FusionRepository -> FusionService",
            ),
            "command:fusion:强行合成": (
                "force_fusion_",
                "已迁移",
                "general_fusion -> FusionApplication.apply_result/apply -> FusionRepository -> FusionService",
            ),
            "command:fusion:合成帮助": (
                "fusion_help_",
                "允许保留的兼容路径",
                "fusion_help_text -> send_help_message",
            ),
            "command:fusion:查看可合成物品": (
                "available_fusion_",
                "允许保留的兼容路径",
                "_items catalog read -> handle_send",
            ),
        }
        for item_id, (handler_name, status, owner_edge) in expected.items():
            with self.subTest(item_id=item_id):
                item = items[item_id]
                self.assertEqual(item["status"], status)
                self.assertNotIn("unknown_edge", item)
                location = f"{SOURCE}:{_handler_line(handler_name)}"
                edges = [
                    str(edge)
                    for edge in item["call_graph"]
                    if str(edge).startswith(f"{location} {handler_name} ->")
                ]
                self.assertEqual(len(edges), 1)
                self.assertIn(owner_edge, edges[0])
                self.assertNotIn("legacy downstream effect not closed", edges[0])
                evidence = {str(value) for value in item["evidence"]}
                self.assertIn(location, evidence)
                self.assertIn("tests/test_fusion_frozen_command_owner.py", evidence)


if __name__ == "__main__":
    unittest.main()
