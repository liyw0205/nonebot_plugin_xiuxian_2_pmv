import unittest
from unittest.mock import patch

import nonebot

nonebot.init()

from ....xiuxian.xiuxian_map import _init_player_map_status


class FixedRandom:
    def choice(self, values):
        return values[0]


class MapInitializationRandomTests(unittest.TestCase):
    def test_initial_position_uses_injected_random(self):
        data = {"凡界": {"heavens": {"一重天": [{"id": "n1"}]}}}
        with patch(
            "nonebot_plugin_xiuxian_2.xiuxian.xiuxian_map.map_application.save_status",
            side_effect=lambda _user_id, realm, heaven, node_id, visited_nodes: {
                "realm": realm,
                "heaven": heaven,
                "node_id": node_id,
                "visited_nodes": visited_nodes,
            },
        ):
            result = _init_player_map_status("u", data, random_source=FixedRandom())
        self.assertEqual({"realm": "凡界", "heaven": "一重天", "node_id": "n1", "visited_nodes": ["n1"]}, result)


if __name__ == "__main__": unittest.main()
