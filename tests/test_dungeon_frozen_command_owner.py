from __future__ import annotations

import ast
import unittest
from pathlib import Path

from scripts.phase2_legacy_path_gate import load_phase2_scope_report


ROOT = Path(__file__).resolve().parents[1]
SOURCE = "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_dungeon/__init__.py"


def _handler_line(handler_name: str) -> int:
    source_path = ROOT / SOURCE
    tree = ast.parse(source_path.read_text(encoding="utf-8"))
    return next(
        node.lineno
        for node in tree.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        and node.name == handler_name
    )


class DungeonFrozenCommandOwnerTests(unittest.TestCase):
    def test_frozen_commands_bind_current_feature_owners(self) -> None:
        report = load_phase2_scope_report(include_items=True)
        items = {item["id"]: item for item in report["items"]}
        expected = {
            "command:dungeon:副本兑换": (
                "handle_dungeon_purchase",
                (
                    "DungeonApplication.purchase",
                    "DungeonSessionSqlRepository.purchase",
                ),
                (
                    "nonebot_plugin_xiuxian_2/features/dungeon/application.py",
                    "nonebot_plugin_xiuxian_2/features/dungeon/repository.py",
                ),
            ),
            "command:dungeon:探索副本": (
                "handle_explore_dungeon",
                (
                    "DungeonExploreSnapshotApplication",
                    "DungeonApplication.prepare_intent",
                    "DungeonApplication.prepare_resolution",
                    "DungeonApplication.settle",
                ),
                (
                    "nonebot_plugin_xiuxian_2/features/dungeon/application.py",
                    "nonebot_plugin_xiuxian_2/features/dungeon/repository.py",
                ),
            ),
        }
        placeholder_markers = (
            "legacy downstream effect not closed in this frozen item",
            "reviewed downstream call-graph edges",
            "unresolved handler target",
        )

        for item_id, (handler_name, owners, evidence_paths) in expected.items():
            with self.subTest(item_id=item_id):
                item = items[item_id]
                handler_location = f"{SOURCE}:{_handler_line(handler_name)}"
                self.assertEqual(item["status"], "已迁移")
                self.assertNotIn("unknown_edge", item)

                handler_edges = [
                    str(edge)
                    for edge in item["call_graph"]
                    if str(edge).startswith(f"{handler_location} {handler_name} ->")
                ]
                self.assertEqual(len(handler_edges), 1)
                handler_edge = handler_edges[0]
                self.assertFalse(
                    any(marker in handler_edge for marker in placeholder_markers),
                    handler_edge,
                )
                for owner in owners:
                    self.assertIn(owner, handler_edge)

                evidence = {str(value) for value in item["evidence"]}
                self.assertIn(handler_location, evidence)
                for path in evidence_paths:
                    self.assertIn(path, evidence)
                self.assertIn("tests/test_dungeon_frozen_command_owner.py", evidence)


if __name__ == "__main__":
    unittest.main()
