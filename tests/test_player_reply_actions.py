"""Final QQ payload contracts for clickable player replies."""

from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch
from urllib.parse import parse_qs

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian import adapter_compat
from nonebot_plugin_xiuxian_2.xiuxian.messaging import delivery as delivery_module
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils import utils
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils.message_markdown import (
    build_md_command_link,
)


_LINK_RE = re.compile(r"\[[^\]\r\n]+\]\(mqqapi://aio/inlinecmd\?([^)]*)\)")


def _actions_from_payload(message):
    segments = list(message) if hasattr(message, "__iter__") else [message]
    markdown = next(
        segment.data["markdown"].content
        for segment in segments
        if segment.type == "markdown"
    )
    links = [
        parse_qs(query, keep_blank_values=True).get("command", [""])[0]
        for query in _LINK_RE.findall(markdown)
    ]
    buttons = []
    for segment in segments:
        if segment.type != "keyboard":
            continue
        rows = segment.data["keyboard"].content.rows
        buttons.extend(
            (button.render_data.label, button.action.data)
            for row in rows
            for button in row.buttons
        )
    return markdown, links, buttons


async def _send_as_qq(operation):
    sent = []
    bot = SimpleNamespace(
        self_id="10000",
        adapter=SimpleNamespace(get_name=lambda: "QQ"),
    )
    event = SimpleNamespace(
        scene="group",
        group_id="900100",
        user_id="900001",
        message_id="payload-test",
        get_user_id=lambda: "900001",
    )

    class _XiuConfig:
        def __init__(self, *args, **kwargs):
            self.markdown_status = True
            self.markdown_id = ""
            self.markdown_id2 = ""
            self.markdown_button_status = True
            self.button_id2 = ""
            self.message_optimization = False
            self.at_sender = False
            self.img = False

    async def capture_group_send(_bot, *, group_id, message, **kwargs):
        sent.append(message)
        return {"message_id": "captured"}

    with (
        patch.object(utils, "XiuConfig", _XiuConfig),
        patch.object(utils, "get_chat_scene", return_value="group"),
        patch.object(utils, "get_real_id_async", AsyncMock(return_value="")),
        patch.object(delivery_module, "get_chat_scene", return_value="group"),
        patch.object(delivery_module, "get_group_id", return_value="900100"),
        patch.object(delivery_module, "is_qq_bot", return_value=True),
        patch.object(delivery_module, "send_group_message", capture_group_send),
        patch.object(
            adapter_compat.CompatMessageSegment,
            "_is_qq_bot",
            new=staticmethod(lambda _bot: True),
        ),
    ):
        await operation(bot, event)

    if len(sent) != 1:
        raise AssertionError(f"expected one outgoing QQ payload, got {len(sent)}")
    return sent[0]


class PlayerReplyActionPayloadTests(unittest.IsolatedAsyncioTestCase):
    async def test_help_entry_link_wins_over_same_target_quick_button(self):
        async def send(bot, event):
            await utils.send_help_message(
                bot,
                event,
                "**修仙帮助**\n- 我的修仙信息\n> 查看角色资料",
                k1="存档",
                v1="我的修仙信息",
                k2="状态",
                v2="我的状态",
            )

        payload = await _send_as_qq(send)
        markdown, links, buttons = _actions_from_payload(payload)

        self.assertIn("[我的修仙信息](mqqapi://aio/inlinecmd?", markdown)
        self.assertIn("我的修仙信息", links)
        self.assertEqual([("状态", "我的状态")], buttons)
        self.assertEqual(1, (links + [command for _, command in buttons]).count("我的修仙信息"))

    async def test_dungeon_subhelp_keeps_context_links_without_duplicate_shortcuts(self):
        from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_dungeon import __dungeon_help__

        async def send(bot, event):
            await utils.send_help_message(
                bot,
                event,
                __dungeon_help__,
                k1="副本信息",
                v1="副本信息",
                k2="探索副本",
                v2="探索副本",
                k3="副本状态",
                v3="我的副本状态",
            )

        payload = await _send_as_qq(send)
        _markdown, links, buttons = _actions_from_payload(payload)
        commands = links + [command for _, command in buttons]

        self.assertTrue({"副本信息", "探索副本", "我的副本状态"}.issubset(commands))
        self.assertEqual(len(commands), len(set(commands)))
        self.assertEqual([], buttons)

    async def test_dungeon_result_sends_useful_actions_once(self):
        async def send(bot, event):
            await utils.handle_send(
                bot,
                event,
                "队伍已经集结，可以开始探索。",
                md_type="team",
                k1="探索副本",
                v1="探索副本",
                k2="查看队伍",
                v2="查看队伍",
                k3="帮助",
                v3="队伍帮助",
            )

        payload = await _send_as_qq(send)
        _markdown, links, buttons = _actions_from_payload(payload)
        commands = links + [command for _, command in buttons]

        self.assertEqual([], links)
        self.assertEqual(
            ["探索副本", "查看队伍", "队伍帮助"],
            [command for _, command in buttons],
        )
        self.assertEqual(len(commands), len(set(commands)))

    async def test_activity_center_actions_reach_the_final_qq_payload_once(self):
        from tests.test_activity_center_delivery_contract import _center_text
        from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils.message_markdown import (
            strip_md_command_links,
        )

        text = _center_text()

        async def send(bot, event):
            await utils.handle_send(
                bot,
                event,
                text,
                native_markdown=True,
                fallback_msg=strip_md_command_links(text),
            )

        payload = await _send_as_qq(send)
        _markdown, links, buttons = _actions_from_payload(payload)
        commands = links + [command for _, command in buttons]

        self.assertTrue(buttons)
        self.assertEqual(len(commands), len(set(commands)))

    async def test_activity_text_without_a_next_step_sends_no_keyboard(self):
        async def send(bot, event):
            await utils.handle_send(
                bot,
                event,
                "活動獎勵目錄\r每日簽到獎勵：靈石",
                native_markdown=True,
                fallback_msg="活動獎勵目錄\n每日簽到獎勵：靈石",
            )

        payload = await _send_as_qq(send)
        segments = list(payload) if hasattr(payload, "__iter__") else [payload]
        self.assertEqual(["markdown"], [segment.type for segment in segments])

    async def test_profile_item_link_deduplicates_footer_button_but_keeps_other_action(self):
        async def send(bot, event):
            text = (
                f"修为: {build_md_command_link('123', '我的修为')}\r\r---\r\r"
                f"{build_md_command_link('我的修为', '我的修为')} | "
                f"{build_md_command_link('我的状态', '我的状态')}"
            )
            await utils.handle_send(
                bot,
                event,
                text,
                native_markdown=True,
                fallback_msg="修为: 123\n\n图片版 | 我的修为 | 我的状态",
            )

        payload = await _send_as_qq(send)
        markdown, links, buttons = _actions_from_payload(payload)

        self.assertIn("修为: [123]", markdown)
        self.assertEqual(["我的修为"], links)
        self.assertEqual([("我的状态", "我的状态")], buttons)

    async def test_page_buttons_keep_arguments_and_hide_no_op_last_page(self):
        async def send(bot, event):
            await utils.handle_send_markdown(
                bot,
                event,
                "背包内容",
                page=["翻页", "我的背包 灵石 2", "查看", "查看效果", "1/2"],
            )

        payload = await _send_as_qq(send)
        _markdown, _links, buttons = _actions_from_payload(payload)
        self.assertIn(("翻页", "我的背包 灵石 2"), buttons)

        async def send_last_page(bot, event):
            await utils.handle_send_markdown(
                bot,
                event,
                "背包内容",
                page=["翻页", "我的背包 灵石 1", "查看", "查看效果", "1/1"],
            )

        last_page_payload = await _send_as_qq(send_last_page)
        _last_markdown, _last_links, last_page_buttons = _actions_from_payload(last_page_payload)
        self.assertNotIn("我的背包 灵石 1", [command for _, command in last_page_buttons])
        self.assertIn("查看效果", [command for _, command in last_page_buttons])

        single_page_rows, _ = utils.build_native_page_rows(
            ["翻页", "我的背包 灵石 1", "查看", "查看效果", "1/1"]
        )
        single_page_commands = [command for row in single_page_rows for _, command in row]
        self.assertNotIn("我的背包 灵石 1", single_page_commands)
        self.assertIn("查看效果", single_page_commands)
        self.assertNotIn("我的背包", utils.build_native_page_text(
            ["翻页", "我的背包 灵石 1", "查看", "查看效果", "1/1"]
        ))
        self.assertNotIn(
            "我的背包",
            str(utils.generate_page_param(
                ["翻页", "我的背包 灵石 1", "查看", "查看效果", "1/1"]
            )),
        )

    async def test_repeated_inline_target_unlinks_later_copy_without_merging_item_targets(self):
        async def send(bot, event):
            text = (
                f"资料：{build_md_command_link('修为', '我的修为')}\r"
                f"重复入口：{build_md_command_link('再看修为', '我的修为')}\r"
                f"{build_md_command_link('第一页', '我的背包 1')} | "
                f"{build_md_command_link('第二页', '我的背包 2')}"
            )
            await utils.handle_send(bot, event, text, native_markdown=True)

        payload = await _send_as_qq(send)
        markdown, links, buttons = _actions_from_payload(payload)
        commands = links + [command for _, command in buttons]

        self.assertEqual(1, commands.count("我的修为"))
        self.assertIn("再看修为", markdown)
        self.assertIn(("第一页", "我的背包 1"), buttons)
        self.assertIn(("第二页", "我的背包 2"), buttons)

    async def test_keyboard_targets_are_unique_even_when_labels_differ(self):
        async def send(bot, event):
            await utils.handle_send(
                bot,
                event,
                "选择后续操作",
                native_markdown=True,
                keyboard_rows=[
                    [("存档", "我的修仙信息"), ("个人资料", "我的修仙信息")]
                ],
            )

        payload = await _send_as_qq(send)
        _markdown, links, buttons = _actions_from_payload(payload)

        self.assertEqual([], links)
        self.assertEqual([("存档", "我的修仙信息")], buttons)

    def test_representative_click_targets_select_the_live_command_matcher(self):
        result = subprocess.run(
            [sys.executable, "-m", "tests.player_reply_route_probe"],
            cwd=Path(__file__).resolve().parents[1],
            text=True,
            capture_output=True,
            timeout=120,
        )
        self.assertEqual(0, result.returncode, result.stdout + result.stderr)
        self.assertIn('"status": "pass"', result.stdout)
        self.assertIn('"input": "我的背包 灵石 2"', result.stdout)
        self.assertIn('"input": "活动购买 灵石补给"', result.stdout)


if __name__ == "__main__":
    unittest.main()
