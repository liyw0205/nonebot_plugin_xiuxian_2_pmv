"""Exercise help menu and keyword search through the registered matcher."""

from tests.parameter_cases_team_leave import integrity
from tests.parameter_cases_team_view import database_projection


def register(case):
    @case("help-search-live-contracts", "修仙帮助", "菜单/关键词/别名/边界",
          "真实matcher/DI/handler、隔离game/player SQLite与OneBot transport mock",
          "菜单、查询排序/上限/错误提示正常且群聊私聊只读",
          group="help-search-parameters")
    async def help_search_contracts(probe):
        from nonebot.adapters.onebot.v11 import Message, PrivateMessageEvent
        from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_base as base
        from tests import command_probe
        from unittest.mock import patch

        before = database_projection(probe)
        base._HELP_SEARCH_ENTRIES = None

        def assert_reply(reply, *phrases):
            content = Message(reply).extract_plain_text().replace("\r", "\n").strip()
            for phrase in phrases:
                assert phrase in content, (phrase, content)
            assert database_projection(probe) == before
            return content

        probe.clear_cooldowns()
        menu = await probe.command("修仙帮助", "修仙帮助", number=999840, timeout=30)
        assert_reply(menu, "修仙帮助", "基础指令", "我的修仙信息", "修仙签到")

        probe.clear_cooldowns()
        whitespace_menu = await probe.command("修仙帮助", "修仙帮助   ", number=999841, timeout=30)
        assert_reply(whitespace_menu, "基础指令", "我的修仙信息")

        probe.clear_cooldowns()
        keyword = await probe.command("修仙帮助", "修仙帮助   签到  ", number=999842, timeout=30)
        keyword_content = assert_reply(keyword, "帮助检索", "修仙签到")

        probe.clear_cooldowns()
        alias = await probe.command("修仙菜单", "修仙菜单 签到统计", number=999843, timeout=30)
        assert_reply(alias, "帮助检索", "我的签到统计", "签到统计")

        probe.clear_cooldowns()
        casefold = await probe.command("修仙帮助", "修仙帮助 活动boss", number=999844, timeout=30)
        assert_reply(casefold, "帮助检索", "活动首领", "活动BOSS")

        probe.clear_cooldowns()
        missing = await probe.command("修仙帮助", "修仙帮助 完全不存在的诊断词", number=999845, timeout=30)
        assert_reply(missing, "没有找到", "修仙帮助")

        probe.clear_cooldowns()
        truncated = await probe.command("修仙帮助", "修仙帮助 帮助", number=999846, timeout=30)
        truncated_content = assert_reply(truncated, "帮助检索", "仅显示前 10 条")
        assert sum(line.startswith("-") for line in truncated_content.splitlines()) == 10

        make_group = command_probe.make_event

        def make_private(text, actor, number):
            data = make_group(text, actor, number).model_dump()
            data.pop("group_id")
            data.update(message_type="private", sub_type="friend")
            return PrivateMessageEvent.model_validate(data)

        probe.clear_cooldowns()
        with patch.object(command_probe, "make_event", side_effect=make_private):
            private = await probe.command(
                "修仙帮助", "修仙帮助   签到  ", actor="900001", number=999847, timeout=30,
            )
        private_content = assert_reply(private, "帮助检索", "修仙签到")
        assert private_content == keyword_content

        integrity(probe)
        probe.evidence.update(
            menu=menu,
            whitespace_menu=whitespace_menu,
            keyword=keyword,
            alias=alias,
            casefold=casefold,
            no_match=missing,
            truncated=truncated,
            private=private,
            full_database_zero_writes=True,
            integrity_check="ok",
        )
