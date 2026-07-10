from __future__ import annotations

import unittest
from unittest.mock import patch

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_dungeon import team_manager


class DungeonTeamStateTests(unittest.TestCase):
    def test_load_teams_recovers_invalid_members_json(self) -> None:
        records = [
            {
                "user_id": "team-1",
                "members": "{broken",
                "leader": None,
                "max_members": "invalid",
            }
        ]
        with patch.object(team_manager.player_data, "get_all_records", return_value=records):
            teams = team_manager.load_teams()

        self.assertEqual(teams["team-1"]["members"], [])
        self.assertEqual(teams["team-1"]["leader"], "")
        self.assertEqual(teams["team-1"]["max_members"], 4)

    def test_get_team_info_normalizes_legacy_member_values(self) -> None:
        record = {
            "user_id": "team-2",
            "members": '[1001, "1002", null, {"bad": true}]',
            "leader": {"bad": True},
            "max_members": "6",
        }
        with patch.object(team_manager.player_data, "get_fields", return_value=record):
            team = team_manager.get_team_info("team-2")

        self.assertEqual(team["members"], ["1001", "1002"])
        self.assertEqual(team["leader"], "1001")
        self.assertEqual(team["max_members"], 6)

    def test_root_type_mismatch_does_not_become_member_list(self) -> None:
        team = team_manager._normalize_team_record(
            {"members": '{"1001": true}', "leader": "1001"}
        )
        self.assertEqual(team["members"], [])


if __name__ == "__main__":
    unittest.main()
