import unittest

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.features.map.schemas import (
    MapExploreSettlementResult,
    MapInteractiveActionResult,
    MapMissionClaimResult,
)
from nonebot_plugin_xiuxian_2.xiuxian.command_guard import result_failure
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_map import _interactive_start_message


class MapReasonContractTests(unittest.TestCase):
    def test_interactive_consumer_uses_reason_instead_of_generic_state_message(self):
        result = MapInteractiveActionResult(
            "state_changed", stamina=8, reason="daily_changed", subject="actor"
        )
        message = _interactive_start_message(result, "采集")
        self.assertIn("今日行动计数与发起时不一致", message)
        self.assertNotIn("节点资源状态已更新", message)

    def test_operation_conflict_and_reward_rejections_are_player_safe(self):
        conflict = result_failure(
            "资源结算",
            MapInteractiveActionResult(
                "state_changed", reason="operation_conflict", subject="actor"
            ),
        )
        full = result_failure(
            "探索结算",
            MapExploreSettlementResult(
                "inventory_full", 0, (), reason="inventory_full", subject="actor"
            ),
        )
        incomplete = result_failure(
            "领取地图委托",
            MapMissionClaimResult(
                "not_completed", 0, (), reason="not_completed", subject="actor"
            ),
        )
        self.assertIn("操作号已用于不同内容", conflict)
        self.assertIn("背包容量不足", full)
        self.assertIn("尚未完成", incomplete)
        for message in (conflict, full, incomplete):
            self.assertNotIn("SQL", message)
            self.assertNotIn("traceback", message.lower())


if __name__ == "__main__":
    unittest.main()
