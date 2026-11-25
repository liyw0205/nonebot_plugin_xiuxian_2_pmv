from __future__ import annotations

import asyncio
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch


ROOT = Path(__file__).resolve().parents[1]


class MindStateHotpathTests(unittest.TestCase):
    def test_state_handler_reuses_checked_profile_and_buff_snapshot(self) -> None:
        source = (
            ROOT
            / "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_buff/__init__.py"
        ).read_text(encoding="utf-8")
        handler = source[source.index("async def mind_state_(") : source.index("async def my_exp_")]
        self.assertIn("user_info = user_msg", handler)
        self.assertIn("get_base_attributes(user_id, user_info=user_info)", handler)
        self.assertIn("base_provider=lambda _user_id: base_attr", handler)
        self.assertIn("buff_data_provider=user_buff_data", handler)
        self.assertEqual(handler.count("user_buff_data = UserBuffDate(user_id)"), 1)
        self.assertNotIn("get_user_impart_info_with_id", handler)
        self.assertNotIn("_sql_message().get_user_info_with_id(user_id)", handler)
        self.assertNotIn("_sql_message().get_player_data(user_id)", handler)
        self.assertIn('"气血": final_attr["current_hp"]', handler)
        self.assertIn('"真元": final_attr["current_mp"]', handler)

    def test_my_exp_reuses_live_profile_and_buff_snapshot(self) -> None:
        import nonebot

        try:
            nonebot.get_driver()
        except ValueError:
            nonebot.init(command_start={"", "/"}, log_level="ERROR")

        from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_buff as module

        profile = {
            "user_id": "u-1",
            "level": "筑基",
            "level_up_rate": 4,
            "root_type": "天灵根",
            "exp": 100,
        }

        class BuffSnapshot:
            buff_info_reads = 0

            @property
            def BuffInfo(self):
                self.buff_info_reads += 1
                return {"blessed_spot": 2}

            def get_user_main_buff_data(self):
                return None

        manager = Mock()
        manager.get_root_rate.return_value = 1
        buff = BuffSnapshot()
        bot = SimpleNamespace(self_id="bot")

        with (
            patch.object(module, "assign_bot", AsyncMock(return_value=(bot, None))),
            patch.object(module, "check_user", return_value=(True, profile, "")),
            patch.object(module, "_sql_message", return_value=manager),
            patch.object(module, "UserBuffDate", return_value=buff) as buff_factory,
            patch.object(module, "OtherSet", return_value=SimpleNamespace(level=["筑基"])),
            patch.object(module, "XiuConfig", return_value=SimpleNamespace(closing_exp=10)),
            patch.object(module.jsondata, "level_data", return_value={"筑基": {"spend": 1}}),
            patch.object(module.jsondata, "level_rate_data", return_value={"筑基": 5}),
            patch.object(module, "handle_send", AsyncMock()) as handle_send,
            patch.object(module.my_exp, "finish", AsyncMock()),
        ):
            asyncio.run(module.my_exp.handlers[0].call(bot, SimpleNamespace()))

        manager.get_user_info_with_id.assert_not_called()
        manager.get_root_rate.assert_called_once_with("天灵根", "u-1")
        buff_factory.assert_called_once_with("u-1")
        self.assertEqual(buff.buff_info_reads, 1)
        self.assertIn("筑基", handle_send.await_args.args[2])

    def test_base_attributes_accepts_matching_profile_without_sql_read(self) -> None:
        from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils import xiuxian2_handle

        profile = {
            "user_id": "u-1",
            "user_name": "测试道友",
            "level": "江湖好手",
            "exp": "10000000000000000000000000000000000000000000000000",
            "stone": 20,
            "hp": 50,
            "mp": 100,
            "atk": 10,
            "atkpractice": 1,
            "hppractice": 2,
            "mppractice": 3,
        }
        manager = Mock()
        with patch.object(xiuxian2_handle, "sql_message", manager):
            result = xiuxian2_handle.get_base_attributes("u-1", user_info=profile)

        manager.get_user_info_with_id.assert_not_called()
        self.assertEqual(result["user_id"], "u-1")
        self.assertEqual(result["exp"], int(profile["exp"]))
        self.assertEqual(result["base_atk"], 10)

    def test_base_attributes_rejects_mismatched_snapshot_and_reads_requested_user(self) -> None:
        from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils import xiuxian2_handle

        manager = Mock()
        manager.get_user_info_with_id.return_value = {
            "user_id": "u-2",
            "user_name": "另一个道友",
            "level": "江湖好手",
            "exp": 1,
            "stone": 2,
            "hp": 3,
            "mp": 4,
            "atk": 5,
        }
        with patch.object(xiuxian2_handle, "sql_message", manager):
            result = xiuxian2_handle.get_base_attributes(
                "u-2", user_info={"user_id": "u-1", "user_name": "错配"}
            )

        manager.get_user_info_with_id.assert_called_once_with("u-2")
        self.assertEqual(result["user_id"], "u-2")


if __name__ == "__main__":
    unittest.main()
