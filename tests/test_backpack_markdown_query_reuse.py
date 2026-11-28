from __future__ import annotations

import unittest
from unittest.mock import patch

import nonebot

nonebot.init()


class BackpackMarkdownQueryReuseTests(unittest.TestCase):
    def test_equipped_state_uses_the_loaded_inventory_rows(self) -> None:
        from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_back as module
        from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_back import back_util

        class Manager:
            def __init__(self, include_state):
                self.back_reads = 0
                self.back_options = []
                self.include_state = include_state
                self.point_reads = []

            def get_back_msg(self, user_id, *, compact=False):
                self.back_reads += 1
                self.back_options.append(compact)
                rows = [
                    {
                        "user_id": user_id,
                        "goods_id": 101,
                        "goods_name": "青锋剑",
                        "goods_type": "装备",
                        "goods_num": 1,
                        "bind_num": 0,
                        "state": 1,
                    },
                    {
                        "user_id": user_id,
                        "goods_id": 102,
                        "goods_name": "玄铁剑",
                        "goods_type": "装备",
                        "goods_num": 1,
                        "bind_num": 0,
                        "state": 0,
                    },
                ]
                if not self.include_state:
                    for row in rows:
                        row.pop("state")
                return rows

            def get_item_by_good_id_and_user_id(self, user_id, goods_id):
                if self.include_state:
                    raise AssertionError("equipment state must come from the inventory read")
                self.point_reads.append((user_id, goods_id))
                return {"state": 1 if goods_id == 101 else 0}

        item_rows = {
            "101": {"item_type": "法器", "level": "练气"},
            "102": {"item_type": "法器", "level": "练气"},
        }
        for include_state in (True, False):
            manager = Manager(include_state)
            with (
                self.subTest(include_state=include_state),
                patch.object(module, "_sql_message", return_value=manager),
                patch.object(back_util, "_sql_message", return_value=manager),
                patch.object(
                    module.items,
                    "get_data_by_item_id",
                    side_effect=lambda goods_id: item_rows.get(str(goods_id)),
                ),
            ):
                sections = module._build_main_backpack_sections_for_md("u-1")

                manager_back_rows = sections[0][1]
                self.assertEqual(manager.back_reads, 1)
                self.assertEqual(manager.back_options, [True])
                self.assertEqual(
                    [(row["name"], row["is_equipped"]) for row in manager_back_rows],
                    [("青锋剑", True), ("玄铁剑", False)],
                )
                self.assertEqual(
                    [] if include_state else [("u-1", 101), ("u-1", 102)],
                    manager.point_reads,
                )


if __name__ == "__main__":
    unittest.main()
