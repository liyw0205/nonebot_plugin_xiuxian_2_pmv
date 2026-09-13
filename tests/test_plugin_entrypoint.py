from __future__ import annotations

import ast
import importlib
import unittest
from pathlib import Path
from unittest.mock import patch


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

    def test_legacy_runtime_load_is_observable_once_for_non_empty_modules(self) -> None:
        plugin = importlib.import_module("nonebot_plugin_xiuxian_2")
        # The helper resolves the public compatibility function lazily, so
        # patch the module it imports rather than relying on global state.
        with patch(
            "nonebot_plugin_xiuxian_2.compatibility.commands.record_compatibility_hit"
        ) as hit:
            plugin._record_legacy_runtime_load([])
            hit.assert_not_called()
            plugin._record_legacy_runtime_load(["legacy.feature"])
            hit.assert_called_once_with("runtime:legacy_package_load")

    def test_legacy_runtime_load_telemetry_failure_does_not_raise(self) -> None:
        plugin = importlib.import_module("nonebot_plugin_xiuxian_2")
        with patch(
            "nonebot_plugin_xiuxian_2.compatibility.commands.record_compatibility_hit",
            side_effect=OSError("read-only telemetry"),
        ):
            plugin._record_legacy_runtime_load(["legacy.feature"])


if __name__ == "__main__":
    unittest.main()
