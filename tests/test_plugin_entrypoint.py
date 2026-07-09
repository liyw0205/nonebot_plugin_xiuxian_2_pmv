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

    def test_internal_packages_are_not_loaded_as_plugins(self) -> None:
        source = ENTRYPOINT.read_text(encoding="utf-8")
        tree = ast.parse(source, filename=str(ENTRYPOINT))
        internal_packages = next(
            node.value
            for node in tree.body
            if isinstance(node, ast.Assign)
            and any(
                isinstance(target, ast.Name) and target.id == "_INTERNAL_PACKAGES"
                for target in node.targets
            )
        )
        values = {element.value for element in internal_packages.elts}
        self.assertEqual(values, {"infrastructure", "messaging", "qq_compat"})
        self.assertIn("module.name not in _INTERNAL_PACKAGES", source)


if __name__ == "__main__":
    unittest.main()
