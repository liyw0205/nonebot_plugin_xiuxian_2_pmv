from __future__ import annotations

import importlib
import os
import unittest
from unittest.mock import patch

import nonebot


class SignInLegacySwitchTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        try:
            nonebot.get_driver()
        except ValueError:
            nonebot.init()

    def test_legacy_sign_in_is_disabled_by_default(self) -> None:
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("XIUXIAN_SIGN_IN_LEGACY_HANDLER", None)
            module = importlib.import_module("nonebot_plugin_xiuxian_2.xiuxian.xiuxian_base")
            self.assertFalse(module._legacy_sign_in_enabled)
