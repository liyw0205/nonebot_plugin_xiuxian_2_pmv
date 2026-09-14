import unittest

import nonebot

nonebot.init()

from ....xiuxian.xiuxian_map import _build_map_enemy


class FixedRandom:
    def uniform(self, start, end):
        return start

    def choice(self, values):
        return values[0]


class MapCombatRandomBoundaryTests(unittest.TestCase):
    def test_enemy_generation_uses_injected_random_source(self):
        enemy = _build_map_enemy(
            {"exp": 10, "power": 1000, "level": "江湖好手"},
            "试炼",
            "试炼台",
            random_source=FixedRandom(),
        )
        self.assertEqual("守关石傀·试炼台", enemy["name"])
        self.assertEqual(1000, enemy["气血"])
        self.assertEqual(200, enemy["攻击"])


if __name__ == "__main__":
    unittest.main()
