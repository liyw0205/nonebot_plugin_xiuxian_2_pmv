"""Verify title list pages and actual automatic unlocks through live commands."""

import json
import re
from unittest.mock import patch

from tests.parameter_cases_team_leave import integrity
from tests.parameter_cases_team_view import database_projection
from tests.parameter_cases_title_unlock import eligible_player


def content(reply):
    from nonebot.adapters.onebot.v11 import Message

    return Message(reply).extract_plain_text().replace("\r", "\n").removeprefix("\n")


def register(case):
    @case("title-list-pages-readonly", "我的称号", "十二个实际称号分页、装备标识、本人隔离/别名/私聊",
          "真实已解锁档案/实际资格引擎与隔离双库", "正常页码归一/越界、名称描述和已装备标识正确，重复完整双库零写",
          group="title-list-parameters")
    async def pages_readonly(probe):
        from nonebot.adapters.onebot.v11 import PrivateMessageEvent
        from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_title as module
        from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_title import title_data
        from tests import command_probe

        definitions = json.loads(title_data.TITLE_JSONPATH.read_text(encoding="utf-8"))
        title_ids = list(definitions)[:12]
        equipped = title_ids[10]
        player = probe.context.paths.player_db
        probe.seed(player, "DELETE FROM title WHERE user_id IN ('900001','900002')")
        probe.seed(player, "INSERT INTO title VALUES('900001',?,?)", (json.dumps([*title_ids, "legacy-missing"]), equipped))
        probe.seed(player, "INSERT INTO title VALUES('900002','[]','')")
        for actor, unlocked in (("900001", [*title_ids, "legacy-missing"]), ("900002", [])):
            snapshot = module.get_title_condition_snapshot(actor)
            assert module.find_unlockable_titles(actor, snapshot=snapshot, unlocked_title_ids=unlocked) == []
        before = database_projection(probe)

        def check_page(reply, page):
            text = content(reply)
            assert f"【我的称号】共12个（第{page}/2页）" in text, text
            actual = re.findall(r"^(\d+)\. 【([^\n]+)】( ⭐已装备)?\n   ([^\n]*)$", text, re.MULTILINE)
            start = (page - 1) * 10
            expected = [(str(index + 1), definitions[title_id]["name"], " ⭐已装备" if title_id == equipped else "",
                         definitions[title_id]["desc"]) for index, title_id in enumerate(title_ids) if start <= index < start + 10]
            assert actual == expected, (expected, actual, text)
            assert "恭喜解锁" not in text and "legacy-missing" not in text
            assert database_projection(probe) == before

        for index, (argument, page) in enumerate((("", 1), ("1", 1), (" 2 ", 2), ("999", 2),
                                                 ("0", 1), ("-1", 1), ("页码", 1), ("2 多余", 1))):
            probe.clear_cooldowns()
            reply = await probe.command("我的称号", f"我的称号 {argument}", number=999200 + index)
            check_page(reply, page)
            probe.clear_cooldowns()
            assert await probe.command("我的称号", f"我的称号 {argument}", number=999200 + index) == reply
            assert database_projection(probe) == before
        for index, alias in enumerate(("称号列表", "查看称号")):
            probe.clear_cooldowns()
            check_page(await probe.command(alias, f"{alias} 2", number=999220 + index), 2)
        probe.clear_cooldowns()
        empty = await probe.command("我的称号", actor="900002", number=999222)
        assert "你还没有解锁任何称号" in empty and all(definitions[key]["name"] not in empty for key in title_ids), empty
        assert database_projection(probe) == before
        probe.clear_cooldowns()
        unregistered = await probe.command("我的称号", actor="900099", number=999223)
        assert "我要修仙" in unregistered and "【我的称号】" not in unregistered, unregistered
        assert database_projection(probe) == before
        make_group = command_probe.make_event

        def make_private(text, actor, number):
            data = make_group(text, actor, number).model_dump()
            data.pop("group_id")
            data.update(message_type="private", sub_type="friend")
            return PrivateMessageEvent.model_validate(data)

        probe.clear_cooldowns()
        with patch.object(command_probe, "make_event", side_effect=make_private):
            check_page(await probe.command("查看称号", "查看称号 2", number=999224), 2)
        integrity(probe)
        probe.evidence.update(title_ids=title_ids, equipped=equipped, pages=[1, 2], private=True, aliases=True,
                              legacy_missing_not_listed=True, full_database_zero_writes=True, integrity_check="ok")

    @case("title-list-real-unlock", "我的称号", "真实签到30、实际解锁/显示/重复查询",
          "真实称号资格与应用事务、不替换条件或应用", "新称号提示和持久化一致，成功后重复查询完整双库零写",
          group="title-list-parameters")
    async def actual_unlock(probe):
        from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_title as module

        titles = eligible_player(probe, module)
        probe.clear_cooldowns()
        reply = content(await probe.command("我的称号", "我的称号 1", number=999250))
        assert "恭喜解锁新称号" in reply and "【我的称号】共2个（第1/1页）" in reply, reply
        for title in titles:
            assert title["name"] in reply and title["desc"] in reply
        expected = {title["id"] for title in titles}
        assert set(json.loads(module.title_application.get_state("900001")["unlocked"])) == expected
        operation = "title-unlock:999250:900001"
        receipt = probe.rows(probe.context.paths.player_db,
                             "SELECT result_status,title_id FROM title_transaction_operations WHERE operation_id=?", (operation,))
        assert receipt == [("applied", ",".join(sorted(expected)))], receipt
        committed = database_projection(probe)
        for index, alias in enumerate(("我的称号", "称号列表", "查看称号")):
            probe.clear_cooldowns()
            repeated = content(await probe.command(alias, f"{alias} 1", number=999250 + index))
            assert "恭喜解锁" not in repeated and "【我的称号】共2个（第1/1页）" in repeated, repeated
            assert database_projection(probe) == committed
        integrity(probe)
        probe.evidence.update(unlocked=sorted(expected), actual_receipt=receipt, repeat_zero_writes=True, integrity_check="ok")
