import unittest

import nonebot

nonebot.init()

from ....xiuxian.xiuxian_map import _pick_explore_event, _roll_explore_event


class FixedExploreRandom:
    def choices(self, values, *, weights=None, k=1):
        return ["empty"]

    def choice(self, values):
        return values[0]


class MapExploreRandomBoundaryTests(unittest.TestCase):
    def test_event_selection_and_empty_event_use_injected_random(self):
        source = FixedExploreRandom()
        self.assertEqual("empty", _pick_explore_event("遗迹", random_source=source))
        line, rewards, stone, items = _roll_explore_event(
            {"user_id": "u"}, "遗迹", "青山", 1.0, random_source=source
        )
        self.assertIn("青山", line)
        self.assertEqual(([], 0, []), (rewards, stone, items))


if __name__ == "__main__":
    unittest.main()
