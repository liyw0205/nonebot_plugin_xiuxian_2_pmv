from __future__ import annotations

import unittest

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_dungeon.team_command_service import (
    build_kick_team_message,
    build_kick_team_result,
    build_leave_team_message,
    build_leave_team_result,
    build_team_view,
    build_team_view_message,
    build_transfer_team_not_member_message,
    build_transfer_team_self_message,
    build_transfer_team_success_message,
    resolve_kick_target,
    resolve_transfer_target,
)


class DungeonTeamCommandServiceTests(unittest.TestCase):
    def test_build_team_view_collects_members_and_marks_leader(self) -> None:
        team_info = {
            "team_name": "试炼小队",
            "team_id": "team-1",
            "create_time": "2026-07-12 12:00:00",
            "members": ["1001", "1002", "1003"],
            "leader": "1002",
            "max_members": 4,
        }

        result = build_team_view(
            team_info,
            lambda user_id: {"1001": "甲", "1002": "乙"}.get(user_id, f"未知用户({user_id})"),
        )

        self.assertEqual(result.status, "ok")
        self.assertEqual(len(result.members), 3)
        self.assertEqual(result.members[0].user_name, "甲")
        self.assertFalse(result.members[0].is_leader)
        self.assertEqual(result.members[1].user_name, "乙")
        self.assertTrue(result.members[1].is_leader)
        self.assertEqual(result.members[2].user_name, "未知用户(1003)")

    def test_build_team_view_message_marks_leader_and_unknown_member(self) -> None:
        team_info = {
            "team_name": "试炼小队",
            "team_id": "team-1",
            "create_time": "2026-07-12 12:00:00",
            "members": ["1001", "1002", "1003"],
            "leader": "1002",
            "max_members": 4,
        }

        result = build_team_view(
            team_info,
            lambda user_id: {"1001": "甲", "1002": "乙"}.get(user_id, f"未知用户({user_id})"),
        )

        message = build_team_view_message(result)

        self.assertIn("队伍名：试炼小队", message)
        self.assertIn("成员：3/4", message)
        self.assertIn("👤 甲", message)
        self.assertIn("👑 乙", message)
        self.assertIn("👤 未知用户(1003)", message)

    def test_resolve_transfer_target_validates_target_states(self) -> None:
        team_info = {"members": ["1001", "1002"]}

        missing = resolve_transfer_target(
            actor_user_id="1001",
            team_info=team_info,
            at_target_user_id=None,
            arg_target_user_id=None,
            lookup_user_name=lambda user_id: None,
        )
        self.assertEqual(missing.status, "target_not_found")

        self_target = resolve_transfer_target(
            actor_user_id="1001",
            team_info=team_info,
            at_target_user_id="1001",
            arg_target_user_id=None,
            lookup_user_name=lambda user_id: "甲",
        )
        self.assertEqual(self_target.status, "self_target")

        outsider = resolve_transfer_target(
            actor_user_id="1001",
            team_info=team_info,
            at_target_user_id=None,
            arg_target_user_id="1003",
            lookup_user_name=lambda user_id: "丙",
        )
        self.assertEqual(outsider.status, "target_not_member")

        no_info = resolve_transfer_target(
            actor_user_id="1001",
            team_info=team_info,
            at_target_user_id=None,
            arg_target_user_id="1002",
            lookup_user_name=lambda user_id: None,
        )
        self.assertEqual(no_info.status, "target_info_missing")

        success = resolve_transfer_target(
            actor_user_id="1001",
            team_info=team_info,
            at_target_user_id=None,
            arg_target_user_id="1002",
            lookup_user_name=lambda user_id: "乙",
        )
        self.assertEqual(success.status, "ok")
        self.assertEqual(success.target_user_id, "1002")
        self.assertEqual(success.target_user_name, "乙")

    def test_build_leave_team_result_covers_member_transfer_and_disband(self) -> None:
        team_info = {"team_name": "试炼小队", "leader": "1001"}

        member_left = build_leave_team_result(
            team_info=team_info,
            leaver_user_id="1002",
            success=True,
            cooldown_hours=3,
            new_leader_name=None,
        )
        self.assertEqual(member_left.status, "member_left")
        self.assertIn("你已离开队伍【试炼小队}】".replace("}", ""), build_leave_team_message(member_left))

        leader_transferred = build_leave_team_result(
            team_info=team_info,
            leaver_user_id="1001",
            success=True,
            cooldown_hours=3,
            new_leader_name="韩立",
        )
        self.assertEqual(leader_transferred.status, "leader_left_transferred")
        self.assertIn("队长已转让给韩立", build_leave_team_message(leader_transferred))

        leader_disbanded = build_leave_team_result(
            team_info=team_info,
            leaver_user_id="1001",
            success=True,
            cooldown_hours=3,
            new_leader_name=None,
        )
        self.assertEqual(leader_disbanded.status, "leader_left_disbanded")
        self.assertIn("队伍已解散", build_leave_team_message(leader_disbanded))

        failed = build_leave_team_result(
            team_info=team_info,
            leaver_user_id="1002",
            success=False,
            cooldown_hours=3,
            new_leader_name=None,
        )
        self.assertEqual(build_leave_team_message(failed), "离开队伍失败！")

    def test_resolve_kick_target_and_message_cover_validation_and_success(self) -> None:
        team_info = {"members": ["1001", "1002"]}

        self.assertEqual(
            resolve_kick_target(
                actor_user_id="1001",
                team_info=team_info,
                at_target_user_id=None,
                arg_target_user_id=None,
                lookup_user_name=lambda user_id: None,
            ).status,
            "target_not_found",
        )
        self.assertEqual(
            resolve_kick_target(
                actor_user_id="1001",
                team_info=team_info,
                at_target_user_id="1001",
                arg_target_user_id=None,
                lookup_user_name=lambda user_id: "甲",
            ).status,
            "self_target",
        )
        self.assertEqual(
            resolve_kick_target(
                actor_user_id="1001",
                team_info=team_info,
                at_target_user_id=None,
                arg_target_user_id="1003",
                lookup_user_name=lambda user_id: "丙",
            ).status,
            "target_not_member",
        )
        self.assertEqual(
            resolve_kick_target(
                actor_user_id="1001",
                team_info=team_info,
                at_target_user_id=None,
                arg_target_user_id="1002",
                lookup_user_name=lambda user_id: None,
            ).status,
            "target_info_missing",
        )

        success = resolve_kick_target(
            actor_user_id="1001",
            team_info=team_info,
            at_target_user_id=None,
            arg_target_user_id="1002",
            lookup_user_name=lambda user_id: "乙",
        )
        self.assertEqual(success.status, "ok")

        kick_message = build_kick_team_message(
            build_kick_team_result(
                target_user_id="1002",
                target_user_name="乙",
                success=True,
                cooldown_hours=3,
            )
        )
        self.assertIn("已将成员乙踢出队伍", kick_message)
        self.assertIn("3小时组队冷却", kick_message)
        self.assertEqual(
            build_kick_team_message(
                build_kick_team_result(
                    target_user_id="1002",
                    target_user_name="乙",
                    success=False,
                    cooldown_hours=3,
                )
            ),
            "踢出成员失败！",
        )

    def test_transfer_team_messages_are_centralized(self) -> None:
        self.assertEqual(build_transfer_team_success_message("韩立"), "👑 队长已成功转移给 韩立！")
        self.assertEqual(build_transfer_team_self_message(), "你已经是队长了，无需转移给自己。")
        self.assertEqual(build_transfer_team_not_member_message(), "只能将队长转移给当前队伍内的成员！")


if __name__ == "__main__":
    unittest.main()
