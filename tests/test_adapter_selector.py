from __future__ import annotations

import importlib.util
import sys
import unittest
from pathlib import Path
from unittest.mock import patch


MODULE_PATH = (
    Path(__file__).resolve().parents[1]
    / "nonebot_plugin_xiuxian_2"
    / "xiuxian"
    / "xiuxian_adapter"
    / "selector.py"
)
SPEC = importlib.util.spec_from_file_location("xiuxian_adapter_selector", MODULE_PATH)
if SPEC is None or SPEC.loader is None:  # pragma: no cover
    raise RuntimeError(f"Unable to load {MODULE_PATH}")
selector = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = selector
SPEC.loader.exec_module(selector)


class AdapterSelectorTests(unittest.TestCase):
    def test_normalizes_supported_sources(self) -> None:
        self.assertEqual(selector.normalize_adapter_source(None), "vendor")
        self.assertEqual(selector.normalize_adapter_source(" AUTO "), "auto")
        self.assertEqual(selector.normalize_adapter_source("installed"), "installed")

    def test_rejects_unknown_source(self) -> None:
        with self.assertRaises(ValueError):
            selector.normalize_adapter_source("mixed")

    def test_vendor_mode_places_vendor_paths_first(self) -> None:
        import nonebot.adapters as nonebot_adapters

        original = list(nonebot_adapters.__path__)
        try:
            selection = selector.configure_adapter_paths("vendor")
            current = list(map(str, nonebot_adapters.__path__))
            self.assertEqual(selection.requested, "vendor")
            self.assertTrue(current[0].startswith(str(selector.VENDOR_ROOT)))
            self.assertEqual(selection.effective["onebot"], "vendor")
            self.assertEqual(selection.effective["qq"], "vendor")
        finally:
            while len(nonebot_adapters.__path__):
                nonebot_adapters.__path__.pop()
            for path in original:
                nonebot_adapters.__path__.append(path)

    def test_auto_mode_uses_installed_then_vendor_fallback(self) -> None:
        import nonebot.adapters as nonebot_adapters

        original = list(nonebot_adapters.__path__)
        try:
            with patch.object(
                selector,
                "_installed_adapter_available",
                side_effect=lambda name: name == "onebot",
            ):
                selection = selector.configure_adapter_paths("auto")

            self.assertEqual(selection.effective["onebot"], "installed")
            self.assertEqual(selection.effective["qq"], "vendor")
        finally:
            while len(nonebot_adapters.__path__):
                nonebot_adapters.__path__.pop()
            for path in original:
                nonebot_adapters.__path__.append(path)


if __name__ == "__main__":
    unittest.main()
