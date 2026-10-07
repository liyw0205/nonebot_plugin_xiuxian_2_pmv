from __future__ import annotations

import ast
import unittest
from pathlib import Path

from scripts.phase2_legacy_path_gate import load_phase2_scope_report


ROOT = Path(__file__).resolve().parents[1]
SOURCE = "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_Illusion/__init__.py"


def _handler_line(handler_name: str, decorator_marker: str) -> int:
    tree = ast.parse((ROOT / SOURCE).read_text(encoding="utf-8"))
    return next(
        node.lineno
        for node in tree.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        and node.name == handler_name
        and any(
            decorator_marker in ast.get_source_segment(
                (ROOT / SOURCE).read_text(encoding="utf-8"), decorator
            )
            for decorator in node.decorator_list
        )
    )


class IllusionFrozenCommandOwnerTests(unittest.TestCase):
    def test_commands_have_feature_owned_edges(self) -> None:
        report = load_phase2_scope_report(include_items=True)
        items = {item["id"]: item for item in report["items"]}
        expected = {
            "command:illusion:幻境寻心": ("illusion_start.handle", "IllusionApplication.get_state/get_choice"),
            "command:illusion:心境试炼": ("illusion_choice.handle", "IllusionApplication.execute"),
            "command:illusion:清空幻境": ("illusion_clear.handle", "IllusionApplication.clear"),
            "command:illusion:重置幻境": ("illusion_reset.handle", "IllusionApplication.clear"),
        }
        for item_id, (decorator, owner) in expected.items():
            with self.subTest(item_id=item_id):
                item = items[item_id]
                self.assertEqual(item["status"], "已迁移")
                self.assertNotIn("unknown_edge", item)
                location = f"{SOURCE}:{_handler_line('_', decorator)}"
                edges = [
                    str(edge)
                    for edge in item["call_graph"]
                    if str(edge).startswith(f"{location} _ ->")
                ]
                self.assertEqual(len(edges), 1)
                self.assertIn(owner, edges[0])
                self.assertNotIn("legacy downstream effect not closed", edges[0])
                self.assertIn(location, item["evidence"])
                self.assertIn("tests/test_illusion_frozen_command_owner.py", item["evidence"])

    def test_choice_command_has_feature_owned_effect_edge(self) -> None:
        report = load_phase2_scope_report(include_items=True)
        items = {item["id"]: item for item in report["items"]}
        item = items["command:illusion:心境试炼"]

        self.assertEqual(item["status"], "已迁移")
        self.assertNotIn("unknown_edge", item)
        location = f"{SOURCE}:{_handler_line('_', 'illusion_choice.handle')}"
        edges = [
            str(edge)
            for edge in item["call_graph"]
            if str(edge).startswith(f"{location} _ ->")
        ]
        self.assertEqual(len(edges), 1)
        self.assertIn("_run_illusion_action", edges[0])
        self.assertIn("IllusionApplication.execute", edges[0])
        self.assertIn("IllusionRepository", edges[0])
        self.assertNotIn("legacy downstream effect not closed", edges[0])
        source = (ROOT / SOURCE).read_text(encoding="utf-8")
        handler = source[source.index("@illusion_choice.handle"):source.index("@illusion_reset.handle")]
        self.assertIn("period=period_key", handler)
        self.assertNotIn("period_key=period_key", handler)
        self.assertIn(location, item["evidence"])
        self.assertIn("tests/test_illusion_frozen_command_owner.py", item["evidence"])

if __name__ == "__main__":
    unittest.main()
