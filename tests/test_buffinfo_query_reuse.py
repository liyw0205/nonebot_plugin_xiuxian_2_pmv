from __future__ import annotations

import asyncio
import importlib
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, patch

from nonebot.exception import FinishedException


class BuffInfoQueryReuseTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        import nonebot

        try:
            nonebot.get_driver()
        except ValueError:
            nonebot.init()
        cls.buff = importlib.import_module("nonebot_plugin_xiuxian_2.xiuxian.xiuxian_buff")
        cls.handle = importlib.import_module("nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils.xiuxian2_handle")

    def test_all_slots_share_one_live_read_and_next_request_refreshes(self):
        slots = ("main_buff", "sub_buff", "effect1_buff", "effect2_buff", "sec_buff")
        original = {slot: index for index, slot in enumerate(slots, 1)}
        current = {**original, "main_buff": 6}
        data = {index: {"name": f"skill-{index}"} for index in range(1, 7)}
        bot = SimpleNamespace(self_id="bot")
        event = SimpleNamespace()
        send = AsyncMock()
        with (
            patch.object(self.buff, "assign_bot", new=AsyncMock(return_value=(bot, None))),
            patch.object(self.buff, "check_user", return_value=(True, {"user_id": "u"}, "")),
            patch.object(self.handle, "get_user_buff", side_effect=[original, current]) as reads,
            patch.object(self.buff, "get_user_buff", side_effect=AssertionError("duplicate slot read")),
            patch.object(self.handle, "items", SimpleNamespace(get_data_by_item_id=data.get)),
            patch.object(self.buff, "get_main_info_msg", side_effect=lambda key: (True, f"main-{key}")),
            patch.object(self.buff, "get_sub_info_msg", side_effect=lambda key: (True, f"sub-{key}")),
            patch.object(self.buff, "get_effect_info_msg", side_effect=lambda key: (True, f"effect-{key}")),
            patch.object(self.buff, "get_sec_msg", side_effect=lambda row: row["name"] if row else "无"),
            patch.object(self.buff, "handle_send", new=send),
        ):
            for expected_calls in (1, 2):
                with self.assertRaises(FinishedException):
                    asyncio.run(self.buff.buffinfo_(bot, event))
                self.assertEqual(expected_calls, reads.call_count)
        first, second = [call.args[2] for call in send.await_args_list]
        self.assertIn("主功法：skill-1", first)
        self.assertIn("主功法：skill-6", second)
        for text in (first, second):
            for expected in ("sub-2", "effect-3", "effect-4", "神通：skill-5"):
                self.assertIn(expected, text)

    def test_empty_slots_and_read_error_do_not_add_queries_or_send_false_reply(self):
        bot, event = SimpleNamespace(self_id="bot"), SimpleNamespace()
        send = AsyncMock()
        with (
            patch.object(self.buff, "assign_bot", new=AsyncMock(return_value=(bot, None))),
            patch.object(self.buff, "check_user", return_value=(True, {"user_id": "u"}, "")),
            patch.object(self.handle, "get_user_buff", side_effect=[{}, RuntimeError("buff read denied")]) as reads,
            patch.object(self.buff, "get_sec_msg", return_value="无"),
            patch.object(self.buff, "handle_send", new=send),
        ):
            with self.assertRaises(FinishedException):
                asyncio.run(self.buff.buffinfo_(bot, event))
            self.assertEqual(5, send.await_args.args[2].count("：无"))
            with self.assertRaisesRegex(RuntimeError, "buff read denied"):
                asyncio.run(self.buff.buffinfo_(bot, event))
        self.assertEqual(2, reads.call_count)
        send.assert_awaited_once()
