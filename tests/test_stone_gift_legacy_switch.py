from __future__ import annotations

import importlib
import os
import unittest
from pathlib import Path
from typing import cast
from unittest.mock import patch

import nonebot


class StoneGiftLegacySwitchTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        try:
            nonebot.get_driver()
        except ValueError:
            nonebot.init()

    def test_legacy_handler_is_disabled_by_default(self) -> None:
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("XIUXIAN_STONE_GIFT_LEGACY_HANDLER", None)
            module = importlib.import_module(
                "nonebot_plugin_xiuxian_2.xiuxian.xiuxian_base"
            )
            self.assertFalse(module._legacy_stone_gift_enabled)

    def test_legacy_handler_has_an_explicit_rollback_switch(self) -> None:
        module = importlib.import_module("nonebot_plugin_xiuxian_2.xiuxian.xiuxian_base")
        self.assertIsNotNone(module.__file__)
        source = Path(cast(str, module.__file__)).read_text(encoding="utf-8")
        self.assertIn("XIUXIAN_STONE_GIFT_LEGACY_HANDLER", source)


if __name__ == "__main__":
    unittest.main()
