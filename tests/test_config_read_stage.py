"""Configuration snapshots are local to one command, including interactive replies."""
from __future__ import annotations

import asyncio
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, Mock, patch

from tests.test_sect_read_stage import Finished, load


ROOT = Path(__file__).parents[1] / "nonebot_plugin_xiuxian_2/xiuxian"


def setUpModule():
    import nonebot
    try:
        nonebot.get_driver()
    except ValueError:
        nonebot.init()


async def finish():
    raise Finished


class RootConfigReadStageTests(unittest.TestCase):
    def test_root_generation_keeps_random_sequence_and_default_calls_read_fresh(self):
        from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils import xiuxian_json_config as module

        data = {"root": {"type_rate": 1, "type_flag": [1, 2], "type_list": ["a", "b", "c"]}}
        generator = module.XiuxianJsonDate()
        initial = module.random.getstate()
        self.addCleanup(module.random.setstate, initial)
        with patch.object(module, "OtherSet", return_value=SimpleNamespace(calculated=lambda rates: "root")), \
                patch.object(module.jsondata, "root_data", return_value=data) as reader:
            module.random.seed(20261128)
            legacy = [generator.linggen_get() for _ in range(10)]
            legacy_state = module.random.getstate()
            self.assertEqual(10, reader.call_count)
            module.random.seed(20261128)
            explicit = [generator.linggen_get(root_data=data) for _ in range(10)]
            self.assertEqual(legacy, explicit)
            self.assertEqual(legacy_state, module.random.getstate())
            self.assertEqual(10, reader.call_count)
            reader.return_value = {"root": dict(data["root"], type_flag=[], type_list=["changed"])}
            self.assertEqual(("changed", "root"), generator.linggen_get())

    def test_reroll_and_interactive_comparison_read_one_new_config_each_time(self):
        for handler in ("restart_", "handle_user_choice"):
            with self.subTest(handler=handler):
                roots = [{"A": {"type_speeds": 2}, "B": {"type_speeds": 1}},
                         {"A": {"type_speeds": 1}, "B": {"type_speeds": 3}}]
                reader = Mock(side_effect=roots)
                generator = SimpleNamespace(linggen_get=Mock(side_effect=[("a", "A"), ("b", "B")] * 10))
                app = SimpleNamespace(get_root_reroll_result=Mock(return_value=None),
                                      reroll_root=Mock(return_value=SimpleNamespace(succeeded=False, status="stone_insufficient")))
                profile = {"user_id": "u", "stone": 100, "root_type": "ordinary", "level": "realm"}
                namespace = {
                    "assign_bot": AsyncMock(return_value=("bot", None)),
                    "check_user": lambda event: (True, profile, ""),
                    "_root_reroll_operation_id": lambda event, user: "operation",
                    "_root_reroll_snapshot": lambda user: dict(user),
                    "base_application": app, "handle_send": AsyncMock(),
                    "restart": SimpleNamespace(finish=finish),
                    "XiuConfig": lambda: SimpleNamespace(remake=1),
                    "JsonConfig": lambda: SimpleNamespace(is_auto_root_selection_enabled=lambda: True),
                    "XiuxianJsonDate": lambda: generator,
                    "jsondata": SimpleNamespace(root_data=reader, level_data=lambda: {"realm": {"spend": 1}}),
                    "_sql_message": lambda: SimpleNamespace(get_root_rate=lambda *args: 1),
                }
                load(ROOT / "xiuxian_base/__init__.py", {handler}, namespace)
                event = SimpleNamespace(get_user_id=lambda: "sender", get_plaintext=lambda: "最好")
                state = {"user_id": "u", "sender_id": "sender", "root_reroll_operation_id": "operation",
                         "root_reroll_snapshot": profile, "linggen_options": [("a", "A"), ("b", "B")] * 5}
                for index, selected in enumerate(("A", "B")):
                    if handler == "restart_":
                        with self.assertRaises(Finished):
                            asyncio.run(namespace[handler]("bot", event, state))
                        calls = generator.linggen_get.call_args_list[index * 10:(index + 1) * 10]
                        self.assertTrue(all(call.kwargs["root_data"] is roots[index] for call in calls))
                    else:
                        asyncio.run(namespace[handler]("bot", event, state))
                    self.assertEqual(index + 1, reader.call_count)
                    self.assertEqual(selected, app.reroll_root.call_args.kwargs["root_type"])


class SectPositionConfigReadStageTests(unittest.TestCase):
    def test_member_text_and_markdown_reuse_positions_and_reload_next_request(self):
        from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_sect import sect_member_utils as helpers

        for markdown in (False, True):
            with self.subTest(markdown=markdown):
                reader = Mock(side_effect=[{"0": {"title": "宗主"}, "2": {"title": "长老"},
                                           "4": {"title": title, "max_count": 20}}
                                          for title in ("Member", "Updated")])
                rows = [{"user_id": str(i), "user_name": f"User{i}", "sect_position": pos,
                         "level": "realm", "sect_contribution": 10**80 + i}
                        for i, pos in enumerate((0, 2, 4))]
                sender, reply = AsyncMock(), AsyncMock()
                namespace = {
                    "CommandArg": lambda: None,
                    "assign_bot": AsyncMock(return_value=(SimpleNamespace(self_id="bot"), None)),
                    "check_user": lambda event: (True, {"sect_id": 1, "sect_position": 0}, ""),
                    "sect_application": SimpleNamespace(get_sect_info=lambda sid: {"sect_name": "Sect"},
                                                        list_sect_members=lambda sid: rows),
                    "jsondata": SimpleNamespace(sect_config_data=reader),
                    "parse_page_arg": lambda arg: {"page": 1, "keyword": ""},
                    "filter_sect_member_rows": helpers.filter_sect_member_rows,
                    "count_sect_positions": helpers.count_sect_positions,
                    "XiuConfig": lambda: SimpleNamespace(markdown_status=markdown),
                    "is_channel_event": lambda event: False, "number_to": str,
                    "_md_cmd_link": lambda title, command: title,
                    "next_page_command": lambda *args: "next",
                    "MessageSegment": SimpleNamespace(markdown=lambda bot, text, **kwargs: text),
                    "delivery_service": SimpleNamespace(reply=reply),
                    "send_msg_handler": sender, "sect_users": SimpleNamespace(finish=finish),
                }
                load(ROOT / "xiuxian_sect/__init__.py", {"sect_users_"}, namespace)
                for index, title in enumerate(("Member", "Updated")):
                    with self.assertRaises(Finished):
                        asyncio.run(namespace["sect_users_"](None, None, None))
                    self.assertEqual(index + 1, reader.call_count)
                    text = reply.call_args.args[2] if markdown else str(sender.call_args)
                    self.assertIn(title, text)
                    self.assertIn("1/20", text)
