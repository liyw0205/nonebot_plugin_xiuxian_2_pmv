from __future__ import annotations

import unittest

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_dungeon.team_command_service import (
    build_team_view_message,
    build_transfer_team_not_member_message,
    build_transfer_team_self_message,
    build_transfer_team_success_message,
)


class DungeonTeamCommandServiceTests(unittest.TestCase):
    def test_build_team_view_message_marks_leader_and_unknown_member(self) -> None:
        team_info = {
            "team_name": "试炼小队",
            "team_id": "team-1",
            "create_time": "2026-07-12 12:00:00",
            "members": ["1001", "1002", "1003"],
            "leader": "1002",
            "max_members": 4,
        }

        message = build_team_view_message(team_info, ["甲", "乙"])

        self.assertIn("队伍名：试炼小队", message)
        self.assertIn("成员：3/4", message)
        self.assertIn("👤 甲", message)
        self.assertIn("👑 乙", message)
        self.assertNotIn("未知用户", message)

    def test_transfer_team_messages_are_centralized(self) -> None:
        self.assertEqual(build_transfer_team_success_message("韩立"), "👑 队长已成功转移给 韩立！")
        self.assertEqual(build_transfer_team_self_message(), "你已经是队长了，无需转移给自己。")
        self.assertEqual(build_transfer_team_not_member_message(), "只能将队长转移给当前队伍内的成员！")


if __name__ == "__main__":
    unittest.main()
