from __future__ import annotations

import ast
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[4]
COMMANDS = ROOT / "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_entertainment/mod/alist_webdav.py"


def _function(source: str, name: str):
    tree = ast.parse(source)
    return next(node for node in tree.body if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == name)


class WebDavCommandContractTests(unittest.TestCase):
    def test_read_only_handlers_delegate_to_entertainment_application(self):
        source = COMMANDS.read_text(encoding="utf-8")
        for name, method in (("webdav_list_bind_", "webdav_bindings"), ("webdav_ls_", "webdav_list"), ("webdav_info_", "webdav_info")):
            node = _function(source, name)
            self.assertTrue(
                any(
                    isinstance(item, ast.Attribute) and item.attr == method
                    for item in ast.walk(node)
                )
            )
            self.assertNotIn("_load_bindings", {call.func.id for call in ast.walk(node) if isinstance(call, ast.Call) and isinstance(call.func, ast.Name)})
            self.assertNotIn("_propfind", {call.func.id for call in ast.walk(node) if isinstance(call, ast.Call) and isinstance(call.func, ast.Name)})


if __name__ == "__main__":
    unittest.main()
