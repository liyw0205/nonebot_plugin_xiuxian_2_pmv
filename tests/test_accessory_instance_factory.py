from __future__ import annotations

import unittest
from datetime import datetime, timezone

from nonebot_plugin_xiuxian_2.features.accessory_package.factory import AccessoryInstanceFactory


class Clock:
    def now(self) -> datetime:
        return datetime(2026, 9, 13, tzinfo=timezone.utc)


class AccessoryFactoryTests(unittest.TestCase):
    def test_factory_uses_injected_dependencies(self) -> None:
        factory = AccessoryInstanceFactory(
            clock=Clock(),
            id_generator=lambda now: "acc-fixed",
            item_lookup=lambda item_id: {"name": "玉佩", "part": "饰品", "set_type": "玄门"},
            affix_roller=lambda quality: [("power", quality)],
        )
        result = factory.create(7, quality=9)
        self.assertEqual(result["uid"], "acc-fixed")
        self.assertEqual(result["quality"], 5)
        self.assertEqual(result["affixes"], [("power", 5)])


if __name__ == "__main__":
    unittest.main()
