from __future__ import annotations

import asyncio
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import nonebot

from nonebot_plugin_xiuxian_2.bootstrap import build_runtime_context
from nonebot_plugin_xiuxian_2.compatibility.sign_in_effects import LegacySignInEffects
from nonebot_plugin_xiuxian_2.features.sign_in.effects import NullSignInEffects
from nonebot_plugin_xiuxian_2.plugin import build_lifecycle
from tests.bootstrap import copy_static_data


class SignInEffectsWiringTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        try:
            nonebot.get_driver()
        except ValueError:
            nonebot.init()

    def test_legacy_runtime_wires_compatibility_effects(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            data_dir = Path(directory) / "data"
            copy_static_data(Path(__file__).resolve().parents[1] / "data" / "xiuxian", data_dir)
            context = build_runtime_context(data_dir=data_dir, legacy_startup=True)
            lifecycle, _, context = build_lifecycle(context)
            asyncio.run(lifecycle.start())
            try:
                self.assertIsInstance(context.services["sign_in"].effects, LegacySignInEffects)
            finally:
                asyncio.run(lifecycle.shutdown())

    def test_non_legacy_runtime_keeps_effects_noop(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            data_dir = Path(directory) / "data"
            copy_static_data(Path(__file__).resolve().parents[1] / "data" / "xiuxian", data_dir)
            context = build_runtime_context(data_dir=data_dir, legacy_startup=False)
            lifecycle, _, context = build_lifecycle(context)
            asyncio.run(lifecycle.start())
            try:
                self.assertIsInstance(context.services["sign_in"].effects, NullSignInEffects)
            finally:
                asyncio.run(lifecycle.shutdown())


if __name__ == "__main__":
    unittest.main()
