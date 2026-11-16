from __future__ import annotations

import unittest
from unittest.mock import Mock, patch


def _user_info_module():
    import nonebot

    try:
        nonebot.get_driver()
    except ValueError:
        nonebot.init(command_start={"", "/"}, superusers={"900003"}, log_level="ERROR")
    from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_info import user_info

    return user_info


class InfoProfileReuseTests(unittest.IsolatedAsyncioTestCase):
    async def test_projection_reuses_matching_checked_profile(self):
        user_info = _user_info_module()

        profile = {
            "user_id": "u-1",
            "user_name": "测试道友",
            "id": 7,
            "root_type": "天灵根",
            "level": "江湖好手",
            "sect_id": None,
            "sect_position": None,
            "exp": 100,
            "stone": 200,
            "root": "金",
            "atk": 30,
            "level_up_rate": 0,
            "atkpractice": 1,
            "hppractice": 2,
            "mppractice": 3,
        }
        manager = Mock()
        manager.get_exp_rank.return_value = (1,)
        manager.get_stone_rank.return_value = (2,)
        manager.get_root_rate.return_value = 1
        manager.get_level_power.return_value = 1000

        levels = Mock(level=["江湖好手", "筑基"])
        buff = Mock()
        buff.get_user_main_buff_data.return_value = None
        buff.get_user_sub_buff_data.return_value = None
        buff.get_user_sec_buff_data.return_value = None
        buff.get_user_effect1_buff_data.return_value = None
        buff.get_user_effect2_buff_data.return_value = None
        buff.get_user_weapon_data.return_value = None
        buff.get_user_armor_buff_data.return_value = None

        jsondata = user_info.jsondata
        with (
            patch.object(user_info, "get_user_profile") as get_profile,
            patch.object(user_info, "_sql_message", return_value=manager),
            patch.object(
                user_info, "get_player_attributes", return_value={"final_atk": 30}
            ) as player_attributes,
            patch.object(user_info, "OtherSet", return_value=levels),
            patch.object(user_info, "UserBuffDate", return_value=buff),
            patch.object(user_info, "load_partner", return_value={}),
            patch.object(user_info, "load_mentor", return_value={"mentor_id": None, "apprentice_ids": []}),
            patch.object(user_info, "update_last_check_info_time"),
            patch.object(user_info, "NatalTreasure") as natal,
            patch.object(
                user_info, "get_equipped_title_display", return_value=""
            ) as title_display,
            patch.object(jsondata, "level_data", return_value={"江湖好手": {"spend": 1}}),
            patch.object(jsondata, "sect_config_data", return_value={}),
            patch.object(jsondata, "level_rate_data", return_value={"江湖好手": 0}),
        ):
            natal.return_value.exists.return_value = False
            detail, text = await user_info.get_user_xiuxian_info(
                "u-1", user_info=profile, equipped_title_id="title-1"
            )

        get_profile.assert_not_called()
        title_display.assert_called_once_with("u-1", title_id="title-1")
        player_attributes.assert_called_once_with("u-1", buff_data_provider=buff)
        self.assertEqual(detail["道号"], "测试道友")
        self.assertIn("修为排行: 第1位", text)

    async def test_projection_reads_when_profile_belongs_to_another_user(self):
        user_info = _user_info_module()

        with patch.object(user_info, "get_user_profile", return_value=None) as get_profile:
            with self.assertRaises(TypeError):
                await user_info.get_user_xiuxian_info("u-1", user_info={"user_id": "u-2"})

        get_profile.assert_called_once_with("u-1")


class BuffProjectionReuseTests(unittest.TestCase):
    def test_buff_info_is_loaded_once_per_projection_instance(self):
        from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils import buff_data

        handle = Mock()
        handle.get_user_buff.return_value = {
            "main_buff": 7,
            "sub_buff": 0,
            "sec_buff": 0,
            "effect1_buff": 0,
            "effect2_buff": 0,
            "faqi_buff": 0,
            "armor_buff": 0,
        }
        item = Mock()
        item.get_data_by_item_id.return_value = {"name": "主功法", "level": 1}
        with patch.object(buff_data, "_handle_module", return_value=handle), patch.object(
            handle, "items", item, create=True
        ):
            projection = buff_data.UserBuffDate("u-1")
            self.assertEqual("主功法", projection.get_user_main_buff_data()["name"])
            self.assertIsNone(projection.get_user_sub_buff_data())
            self.assertEqual("主功法", projection.get_user_main_buff_data()["name"])

        handle.get_user_buff.assert_called_once_with("u-1")

    def test_new_projection_instance_reads_latest_buff_snapshot(self):
        from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils import buff_data

        handle = Mock()
        handle.get_user_buff.side_effect = [
            {"main_buff": 1},
            {"main_buff": 2},
        ]
        with patch.object(buff_data, "_handle_module", return_value=handle):
            first = buff_data.UserBuffDate("u-1")
            second = buff_data.UserBuffDate("u-1")
            self.assertEqual({"main_buff": 1}, first.BuffInfo)
            self.assertEqual({"main_buff": 1}, first.BuffInfo)
            self.assertEqual({"main_buff": 2}, second.BuffInfo)

        self.assertEqual(2, handle.get_user_buff.call_count)


class TitleProjectionReuseTests(unittest.TestCase):
    def test_explicit_title_snapshot_skips_state_query(self):
        from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_title import title_data

        with (
            patch.object(title_data, "get_user_equipped_title") as state_reader,
            patch.object(title_data, "get_title_by_id", return_value={"name": "快照称号"}),
        ):
            self.assertEqual(
                "快照称号",
                title_data.get_equipped_title_display("u-1", title_id="301"),
            )

        state_reader.assert_not_called()


if __name__ == "__main__":
    unittest.main()
