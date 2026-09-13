from __future__ import annotations

import unittest

import nonebot


class MigratedMatcherWiringTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        try:
            nonebot.get_driver()
        except ValueError:
            nonebot.init()

    def test_matchers_are_installed_once_and_have_transport_dependencies(self) -> None:
        from nonebot_plugin_xiuxian_2.adapters.nonebot.commands import register_migrated_matchers

        driver = type("Driver", (), {})()
        first = register_migrated_matchers(driver, {"context": None})
        second = register_migrated_matchers(driver, {"context": None})
        self.assertIs(first, second)
        self.assertEqual([matcher.priority for matcher in first], [1, 1, 1])
        self.assertTrue(all(matcher.handlers for matcher in first))


if __name__ == "__main__":
    unittest.main()
