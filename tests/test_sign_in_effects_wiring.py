from __future__ import annotations

import asyncio
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import nonebot

from nonebot_plugin_xiuxian_2.bootstrap import build_runtime_context
from nonebot_plugin_xiuxian_2.features.sign_in.effects import NullSignInEffects
from nonebot_plugin_xiuxian_2.features.sign_in.application_effects import SignInApplicationEffects
from nonebot_plugin_xiuxian_2.plugin import build_lifecycle
from tests.bootstrap import copy_static_data


class SignInEffectsWiringTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        try:
            nonebot.get_driver()
        except ValueError:
            nonebot.init()

    async def _assert_runtime_effects(self, *, legacy_startup: bool, expected_type: type) -> None:
        with tempfile.TemporaryDirectory() as directory:
            data_dir = Path(directory) / "data"
            copy_static_data(Path(__file__).resolve().parents[1] / "data" / "xiuxian", data_dir)
            context = build_runtime_context(data_dir=data_dir, legacy_startup=legacy_startup)
            lifecycle, _, context = build_lifecycle(context)
            await lifecycle.start()
            try:
                self.assertIsInstance(context.services["sign_in"].effects, expected_type)
            finally:
                await lifecycle.shutdown()

    def test_legacy_runtime_wires_feature_effects_with_legacy_lottery_port(self) -> None:
        asyncio.run(
            self._assert_runtime_effects(
                legacy_startup=True,
                expected_type=SignInApplicationEffects,
            )
        )

    def test_non_legacy_runtime_keeps_effects_noop(self) -> None:
        asyncio.run(
            self._assert_runtime_effects(
                legacy_startup=False,
                expected_type=NullSignInEffects,
            )
        )


if __name__ == "__main__":
    unittest.main()
