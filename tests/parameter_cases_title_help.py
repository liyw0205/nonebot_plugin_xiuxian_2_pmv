"""Verify the title help command through its registered matcher."""

from tests.parameter_cases_team_leave import integrity
from tests.parameter_cases_team_view import database_projection


def register(case):
    @case("title-help-live-readonly", "称号帮助", "真实帮助内容/群聊私聊/只读",
          "真实matcher/DI/handler、隔离game/player SQLite与OneBot transport mock",
          "标题、指令说明和快捷入口可见；群聊/私聊内容一致且两库零写",
          group="title-help-parameters")
    async def help_live_readonly(probe):
        from types import SimpleNamespace
        from unittest.mock import patch

        from nonebot.adapters.onebot.v11 import Message, PrivateMessageEvent
        from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils import utils
        from tests import command_probe

        before = database_projection(probe)

        def assert_help(reply):
            content = Message(reply).extract_plain_text().replace("\r", "\n").strip()
            for phrase in (
                "称号帮助", "我的称号：查看第一页", "装备称号 + 名称",
                "检查称号：手动检测新称号", "达成条件后自动解锁",
            ):
                assert phrase in content, (phrase, content)
            assert "检查成就：手动检测新成就" in content
            assert database_projection(probe) == before
            return content

        probe.clear_cooldowns()
        group_reply = await probe.command("称号帮助", "称号帮助", actor="900001", number=999830,
                                          timeout=30)
        group_content = assert_help(group_reply)

        make_group = command_probe.make_event

        def make_private(text, actor, number):
            data = make_group(text, actor, number).model_dump()
            data.pop("group_id")
            data.update(message_type="private", sub_type="friend")
            return PrivateMessageEvent.model_validate(data)

        probe.clear_cooldowns()
        qq_config = SimpleNamespace(
            markdown_status=True,
            markdown_id="",
            markdown_id2="",
            markdown_button_status=True,
            message_optimization=False,
            img=False,
            at_sender=False,
        )
        qq_payload = {}

        async def capture_qq_payload(bot, event, markdown, **kwargs):
            qq_payload.update(markdown=markdown, rows=kwargs.get("rows"))
            await utils.handle_send2(bot, event, utils.strip_md_command_links(markdown))

        with patch.object(command_probe, "make_event", side_effect=make_private):
            with (patch.object(utils, "_is_onebot_v11_bot", return_value=False),
                  patch.object(utils, "XiuConfig", return_value=qq_config),
                  patch.object(utils, "_allow_native_markdown", return_value=True),
                  patch.object(utils, "_send_markdown_or_keyboard", side_effect=capture_qq_payload)):
                private_reply = await probe.command(
                    "称号帮助", "称号帮助", actor="900001", number=999831, timeout=30,
                )
        private_content = assert_help(private_reply)
        assert private_content == group_content
        assert qq_payload["rows"] == [
            [("我的称号", "我的称号"), ("检查称号", "检查称号"), ("关系", "关系帮助")],
            [("存档", "我的修仙信息")],
        ], qq_payload
        registered_commands = {
            name
            for entry in probe.entries
            for name in (entry["command"], *entry["aliases"])
        }
        assert all(command in registered_commands
                   for row in qq_payload["rows"] for _, command in row)
        integrity(probe)
        probe.evidence.update(group_reply=group_reply, private_reply=private_reply,
                              same_body_for_group_and_private=True, full_database_zero_writes=True,
                              qq_markdown=qq_payload["markdown"], qq_keyboard_rows=qq_payload["rows"],
                              integrity_check="ok")
