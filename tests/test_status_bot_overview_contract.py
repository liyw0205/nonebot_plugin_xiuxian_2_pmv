from __future__ import annotations

import ast
from pathlib import Path
import unittest


class BotOverviewHandlerContractTests(unittest.TestCase):
    def test_bot_info_uses_status_application_without_legacy_managers(self) -> None:
        path = (
            Path(__file__).resolve().parents[1]
            / "nonebot_plugin_xiuxian_2"
            / "xiuxian"
            / "xiuxian_status"
            / "__init__.py"
        )
        source = path.read_text(encoding="utf-8")
        tree = ast.parse(source, filename=str(path))
        imported_names = {
            alias.name
            for node in ast.walk(tree)
            if isinstance(node, (ast.Import, ast.ImportFrom))
            for alias in node.names
        }
        self.assertNotIn("XiuxianDateManage", imported_names)
        self.assertNotIn("TradeDataManager", imported_names)

        handler = next(
            node
            for node in ast.walk(tree)
            if isinstance(node, ast.AsyncFunctionDef) and node.name == "get_bot_info"
        )
        self.assertTrue(
            any(
                isinstance(node, ast.Call)
                and isinstance(node.func, ast.Attribute)
                and isinstance(node.func.value, ast.Name)
                and node.func.value.id == "status_application"
                and node.func.attr == "bot_overview"
                for node in ast.walk(handler)
            )
        )


if __name__ == "__main__":
    unittest.main()
