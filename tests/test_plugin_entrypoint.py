from __future__ import annotations

import ast
import unittest
from pathlib import Path


ENTRYPOINT = Path(__file__).resolve().parents[1] / "nonebot_plugin_xiuxian_2" / "__init__.py"


class PluginEntrypointTests(unittest.TestCase):
    def test_entrypoint_does_not_load_plugins_from_absolute_directory(self) -> None:
        tree = ast.parse(ENTRYPOINT.read_text(encoding="utf-8"), filename=str(ENTRYPOINT))
        imported_names = {
            alias.asname or alias.name
            for node in tree.body
            if isinstance(node, ast.ImportFrom) and node.module == "nonebot"
            for alias in node.names
        }

        self.assertNotIn(
            "load_plugins",
            imported_names,
            "Directory-based loading breaks when the bot cwd differs from the plugin source directory.",
        )


if __name__ == "__main__":
    unittest.main()
