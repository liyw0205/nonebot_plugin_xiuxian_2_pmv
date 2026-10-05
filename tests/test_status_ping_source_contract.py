from __future__ import annotations

import ast
from pathlib import Path
import unittest


SOURCE = (
    Path(__file__).resolve().parents[1]
    / "nonebot_plugin_xiuxian_2"
    / "xiuxian"
    / "xiuxian_status"
    / "__init__.py"
)


class StatusPingSourceContractTests(unittest.TestCase):
    def test_ping_handler_delegates_to_status_application(self) -> None:
        source = SOURCE.read_text(encoding="utf-8")
        tree = ast.parse(source, filename=str(SOURCE))
        functions = {
            node.name: node
            for node in ast.walk(tree)
            if isinstance(node, ast.AsyncFunctionDef)
        }

        handler = functions["handle_ping_test"]
        get_ping_test = functions["get_ping_test"]
        handler_calls = {
            node.func.id
            for node in ast.walk(handler)
            if isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name)
        }
        application_calls = {
            node.func.attr
            for node in ast.walk(get_ping_test)
            if isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and isinstance(node.func.value, ast.Name)
            and node.func.value.id == "status_application"
        }

        self.assertIn("get_ping_test", handler_calls)
        self.assertIn("ping_test", application_calls)
        self.assertNotIn("subprocess", {alias.name for node in ast.walk(get_ping_test) if isinstance(node, ast.Import) for alias in node.names})
        self.assertNotIn("ping_host", {node.name for node in ast.walk(tree) if isinstance(node, ast.AsyncFunctionDef)})


if __name__ == "__main__":
    unittest.main()
