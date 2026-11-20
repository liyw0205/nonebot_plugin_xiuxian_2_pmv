"""Query actual title definitions through live matchers and isolated SQLite."""

import json
from unittest.mock import patch

from tests.parameter_cases_team_leave import integrity
from tests.parameter_cases_team_view import database_projection


def register(case):
    @case("title-info-definitions-readonly", "称号详情", "真实名称/ID、空白/错误参数、私聊和重复查询",
          "实际称号JSON、已装备/未解锁/未注册玩家和隔离双库", "名称/描述/获取条件逐项一致，不解锁称号，完整双库和回执零写",
          group="title-info-parameters")
    async def definitions_readonly(probe):
        from nonebot.adapters.onebot.v11 import Message, PrivateMessageEvent
        from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_title import title_data
        from tests import command_probe

        definitions = json.loads(title_data.TITLE_JSONPATH.read_text(encoding="utf-8"))
        selected = ["30001", "30101", next(key for key, value in definitions.items() if ";" in value["condition"])]
        player = probe.context.paths.player_db
        probe.seed(player, "DELETE FROM title WHERE user_id IN ('900001','900002')")
        probe.seed(player, "INSERT INTO title VALUES('900001','[\"30001\"]','30001')")
        probe.seed(player, "INSERT INTO title VALUES('900002','[]','')")
        probe.managers[1].update_or_write_data("900002", "statistics", "修仙签到", 30)
        before = database_projection(probe)

        def check(reply, expected):
            content = Message(reply).extract_plain_text().replace("\r", "\n").removeprefix("\n")
            assert content == expected, (expected, reply)
            assert database_projection(probe) == before

        for index, title_id in enumerate(selected):
            definition = definitions[title_id]
            expected = (f"【称号详情】\n名称：{definition['name']}\n描述：{definition['desc']}\n"
                        f"获取条件：{definition['condition']}")
            for offset, actor in enumerate(("900001", "900002", "900099")):
                number = 999000 + index * 10 + offset
                reply = await probe.command("称号详情", f"称号详情 {title_id}", actor=actor, number=number)
                check(reply, expected)
                check(await probe.command("称号详情", f"称号详情 {title_id}", actor=actor, number=number), expected)
            check(await probe.command("称号详情", f"称号详情   {definition['name']}   ", number=999050 + index), expected)

        for index, argument in enumerate(("", "   ", "不存在的诊断称号", "999999999", "30001 多余参数")):
            expected = ("请输入要查看的称号名称或ID！\n例如：称号详情 持之以恒" if not argument.strip() else "未找到该称号！")
            check(await probe.command("称号详情", f"称号详情 {argument}", number=999060 + index), expected)

        make_group = command_probe.make_event

        def make_private(text, actor, number):
            data = make_group(text, actor, number).model_dump()
            data.pop("group_id")
            data.update(message_type="private", sub_type="friend")
            return PrivateMessageEvent.model_validate(data)

        definition = definitions["30001"]
        with patch.object(command_probe, "make_event", side_effect=make_private):
            reply = await probe.command("称号详情", f"称号详情 {definition['name']}", actor="900099", number=999070)
        check(reply, f"【称号详情】\n名称：{definition['name']}\n描述：{definition['desc']}\n获取条件：{definition['condition']}")
        integrity(probe)
        probe.evidence.update(title_ids=selected, actual_definitions=True, private=True, public_query=True,
                              eligible_player_not_auto_unlocked=True, repeat_full_database_zero_writes=True, integrity_check="ok")
