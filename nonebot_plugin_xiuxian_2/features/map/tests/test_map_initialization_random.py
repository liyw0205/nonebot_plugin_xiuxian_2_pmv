import unittest

import nonebot

nonebot.init()

from ....xiuxian.xiuxian_map import _init_player_map_status


class FixedRandom:
    def choice(self, values):
        return values[0]


class MapInitializationRandomTests(unittest.TestCase):
    def test_initial_position_uses_injected_random(self):
        data = {"凡界": {"heavens": {"一重天": [{"id": "n1"}]}}}
        result = _init_player_map_status("u", data, random_source=FixedRandom())
        self.assertEqual({"realm": "凡界", "heaven": "一重天", "node_id": "n1", "visited_nodes": ["n1"]}, result)


if __name__ == "__main__": unittest.main()
