"""Read the real shop catalogue without mutating isolated player data."""

from unittest.mock import patch

from tests.parameter_cases_team_leave import integrity
from tests.parameter_cases_team_view import database_projection


def register(case):
    @case("dungeon-shop-catalogue-readonly", "副本商店", "注册/未注册玩家、私聊、空白和重复查询",
          "真实matcher/handler、实际商品配置与隔离双库", "名称匹配实际物品、价格匹配兑换配置，完整双库资产/回执零写",
          group="dungeon-shop-parameters")
    async def catalogue_readonly(probe):
        from nonebot.adapters.onebot.v11 import PrivateMessageEvent
        from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_dungeon as module
        from tests import command_probe

        expected = []
        for item_id, item in module.DUNGEON_SHOP.items():
            definition = module.items.get_data_by_item_id(item_id)
            assert definition is not None, item_id
            assert item["cost"] > 0
            expected.append(f"{item_id} | {definition['name']} | {module.number_to(item['cost'])}灵石")
        assert expected
        before = database_projection(probe)

        def check(reply):
            lines = reply.replace("\r", "\n").splitlines()
            assert "【副本商店】" in lines and "使用：副本兑换 物品ID 数量" in lines, reply
            assert [line for line in lines if " | " in line] == expected, (expected, reply)
            assert database_projection(probe) == before

        for index, actor in enumerate(("900001", "900002", "900099")):
            number = 997000 + index
            reply = await probe.command("副本商店", actor=actor, number=number)
            check(reply)
            assert await probe.command("副本商店", actor=actor, number=number) == reply
            assert database_projection(probe) == before
        check(await probe.command("副本商店", "副本商店   ", number=997010))
        make_group = command_probe.make_event

        def make_private(text, actor, number):
            data = make_group(text, actor, number).model_dump()
            data.pop("group_id")
            data.update(message_type="private", sub_type="friend")
            return PrivateMessageEvent.model_validate(data)

        with patch.object(command_probe, "make_event", side_effect=make_private):
            check(await probe.command("副本商店", number=997011))
        integrity(probe)
        probe.evidence.update(catalogue=expected, private=True, unregistered_public_query=True,
                              repeat_full_database_zero_writes=True, integrity_check="ok")
