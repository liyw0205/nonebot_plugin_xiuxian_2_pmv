"""Check real achievement counts and unlock receipts through live commands."""

import json
from unittest.mock import patch

from tests.parameter_cases_team_leave import integrity
from tests.parameter_cases_team_view import database_projection
from tests.parameter_cases_title_list import content
from tests.parameter_cases_title_unlock import eligible_player


def register(case):
    @case("achievement-check-counts-unlock", "检查成就", "无新资格、系统赠送/旧ID、实际解锁、别名/私聊/重复",
          "真实资格/配置、隔离双库和合法玩家", "完成总数只含有条件称号，实际解锁与回执一致，无新资格完整双库零写",
          group="achievement-check-parameters")
    async def counts_unlock(probe):
        from nonebot.adapters.onebot.v11 import PrivateMessageEvent
        from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_title as module
        from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_title import title_data
        from tests import command_probe

        definitions = json.loads(title_data.TITLE_JSONPATH.read_text(encoding="utf-8"))
        total = sum(bool(value["condition"]) for value in definitions.values())
        eligible_player(probe, module, count=9)
        player = probe.context.paths.player_db
        probe.seed(player, "UPDATE title SET unlocked='[\"30101\",\"legacy-missing\"]' WHERE user_id='900001'")
        before = database_projection(probe)
        for index, name in enumerate(("检查成就", "检测成就")):
            probe.clear_cooldowns()
            reply = content(await probe.command(name, number=999500 + index))
            assert reply == f"检查完成，当前已完成0/{total}个成就，暂无新成就解锁。", reply
            assert database_projection(probe) == before

        probe.managers[1].update_or_write_data("900001", "statistics", "修仙签到", 30)
        make_group = command_probe.make_event

        def make_private(text, actor, number):
            data = make_group(text, actor, number).model_dump()
            data.pop("group_id")
            data.update(message_type="private", sub_type="friend")
            return PrivateMessageEvent.model_validate(data)

        probe.clear_cooldowns()
        with patch.object(command_probe, "make_event", side_effect=make_private):
            reply = content(await probe.command("检测成就", number=999502))
        expected = ["检查完成，解锁新成就：", *[f"【{definitions[title_id]['name']}】{definitions[title_id]['desc']}"
                                                       for title_id in ("30001", "30002")]]
        assert reply.splitlines() == expected, reply
        state = module.title_application.get_state("900001")
        assert set(json.loads(state["unlocked"])) == {"30001", "30002", "30101", "legacy-missing"}
        receipt = probe.rows(player, "SELECT result_status,title_id FROM title_transaction_operations WHERE operation_id=?",
                             ("title-unlock:999502:900001",))
        assert receipt == [("applied", "30001,30002")], receipt
        committed = database_projection(probe)
        for index, name in enumerate(("检查成就", "检测成就")):
            probe.clear_cooldowns()
            repeated = content(await probe.command(name, number=999502 + index))
            assert repeated == f"检查完成，当前已完成2/{total}个成就，暂无新成就解锁。", repeated
            assert database_projection(probe) == committed
        probe.clear_cooldowns()
        other = content(await probe.command("检查成就", actor="900002", number=999504))
        assert other == f"检查完成，当前已完成0/{total}个成就，暂无新成就解锁。", other
        assert database_projection(probe) == committed
        probe.clear_cooldowns()
        missing = content(await probe.command("检查成就", actor="900099", number=999505))
        assert "我要修仙" in missing and "检查完成" not in missing, missing
        assert database_projection(probe) == committed
        integrity(probe)
        probe.evidence.update(total=total, completed=[0, 2], gift_and_missing_not_counted=True, actual_receipt=receipt,
                              private=True, alias=True, repeat_full_database_zero_writes=True, integrity_check="ok")
