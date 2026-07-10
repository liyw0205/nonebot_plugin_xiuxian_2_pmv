from __future__ import annotations

import unittest
from types import SimpleNamespace

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.infrastructure.settings import SettingsProvider


class SettingsProviderTests(unittest.TestCase):
    def test_priority_is_driver_then_persisted_then_default(self) -> None:
        driver = [SimpleNamespace(value="driver")]
        persisted = [SimpleNamespace(value="persisted", only_persisted=2)]
        provider = SettingsProvider(lambda: driver[0], lambda: persisted[0])
        self.assertEqual(provider.get("value", "default"), "driver")
        self.assertEqual(provider.get("only_persisted", 0), 2)
        self.assertEqual(provider.get("missing", "default"), "default")

        driver[0] = SimpleNamespace(value=None)
        provider.reload()
        self.assertEqual(provider.get("value"), "persisted")

    def test_typed_accessors_normalize_values(self) -> None:
        provider = SettingsProvider(
            lambda: SimpleNamespace(enabled="yes", count="3.8"),
            lambda: SimpleNamespace(),
        )
        self.assertTrue(provider.get_bool("enabled"))
        self.assertEqual(provider.get_int("count", 0, minimum=5), 5)
        self.assertEqual(provider.get_str("missing", "fallback"), "fallback")


if __name__ == "__main__":
    unittest.main()
