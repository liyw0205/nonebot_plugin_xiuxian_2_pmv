from types import SimpleNamespace
from unittest.mock import patch

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_compensation import common


class _Economy:
    def grant_stone(self, user_id, amount):
        return SimpleNamespace(succeeded=True, applied=amount)


class _Inventory:
    def __init__(self, result):
        self.result = result
        self.calls = []

    def grant_item(self, *args, **kwargs):
        self.calls.append((args, kwargs))
        return self.result


def test_compensation_rewards_use_inventory_application_and_actual_quantity():
    inventory = _Inventory(
        SimpleNamespace(succeeded=True, applied=2)
    )
    rewards = [
        {"type": "stone", "name": "灵石", "quantity": 10},
        {"type": "功法", "id": 101, "name": "太玄经", "quantity": 5},
    ]

    with patch.object(common, "_economy_application", return_value=_Economy()), patch.object(
        common, "_inventory_application", return_value=inventory
    ), patch.object(common, "_sql_message") as legacy:
        messages = common.send_reward_to_user("u", rewards)

    assert messages == ["获得灵石 10 枚", "获得 太玄经 x2"]
    assert inventory.calls == [
        (
            ("u", 101, "太玄经", "技能", 5),
            {"bind_flag": 1, "max_goods_num": int(common.XiuConfig().max_goods_num)},
        )
    ]
    legacy.assert_not_called()


def test_compensation_rewards_do_not_claim_items_when_inventory_rejects():
    inventory = _Inventory(SimpleNamespace(succeeded=False, applied=0))

    with patch.object(common, "_inventory_application", return_value=inventory):
        messages = common.send_reward_to_user(
            "u",
            [{"type": "道具", "id": 7, "name": "令牌", "quantity": 1}],
        )

    assert messages == []
