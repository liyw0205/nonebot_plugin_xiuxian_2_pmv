"""Exercise actual achievement progress, filters, and source-read failures."""

import json
import re
from unittest.mock import patch

from tests.parameter_cases_team_leave import integrity
from tests.parameter_cases_team_view import database_projection
from tests.parameter_cases_title_list import content
from tests.parameter_cases_title_unlock import eligible_player


def register(case):
    @case("achievement-real-signin-progress", "我的成就", "真实签到9/10/30和名称筛选",
          "实际资格快照/称号JSON和隔离双库", "未完成90%、实际解锁与完成100%一致，无新资格查询完整双库零写",
          group="achievement-parameters")
    async def signin_progress(probe):
        from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_title as module

        eligible_player(probe, module, count=9)
        before = database_projection(probe)
        probe.clear_cooldowns()
        reply = content(await probe.command("我的成就", "我的成就 初心不改", number=999400))
        assert "【初心不改】未完成 90%" in reply and "修仙签到：9 / 10" in reply, reply
        assert "本次新解锁" not in reply and database_projection(probe) == before
        for index, (count, title_name, expected_ids) in enumerate(((10, "初心不改", ["30001"]),
                                                               (30, "持之以恒", ["30001", "30002"]))):
            probe.managers[1].update_or_write_data("900001", "statistics", "修仙签到", count)
            probe.clear_cooldowns()
            number = 999401 + index
            reply = content(await probe.command("我的成就", f"我的成就 {title_name}", number=number))
            assert "本次新解锁" in reply and f"【{title_name}】已完成 100%" in reply, reply
            assert f"修仙签到：{count} / {count}" in reply, reply
            assert json.loads(module.title_application.get_state("900001")["unlocked"]) == expected_ids
            rows = probe.rows(probe.context.paths.player_db,
                              "SELECT result_status,title_id FROM title_transaction_operations WHERE operation_id=?",
                              (f"title-unlock:{number}:900001",))
            assert rows == [("applied", expected_ids[-1])], rows
            committed = database_projection(probe)
            probe.clear_cooldowns()
            repeated = content(await probe.command("我的成就", f"我的成就 {title_name}", number=number))
            assert "本次新解锁" not in repeated and f"【{title_name}】已完成 100%" in repeated, repeated
            assert database_projection(probe) == committed
        integrity(probe)
        probe.evidence.update(signin=[9, 10, 30], actual_unlock_receipts=True, repeat_zero_writes=True, integrity_check="ok")

    @case("achievement-source-read-recovery", "我的成就", "有效签到数据但统计视图读取失败",
          "合法玩家、真实SQLite错误、不替换资格读取", "读取异常不能当进度0，原错误日志/定位号、双库零写，修复数据源同消息恢复",
          group="achievement-parameters")
    async def source_read_recovery(probe):
        from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_title as module

        eligible_player(probe, module, count=9)
        player = probe.context.paths.player_db
        probe.seed(player, "ALTER TABLE statistics RENAME TO isolated_achievement_statistics")
        probe.seed(player, "CREATE VIEW statistics AS SELECT * FROM isolated_missing_statistics_source")
        before = database_projection(probe)
        try:
            probe.clear_cooldowns()
            reply = content(await probe.command("我的成就", "我的成就 初心不改", number=999410, allow_exception=True))
            probe.evidence.update(reply=reply, original_signin=probe.rows(player,
                                  'SELECT "修仙签到" FROM isolated_achievement_statistics WHERE user_id=?', ("900001",)))
            assert "定位号：" in reply and "内部异常" in reply and "【初心不改】" not in reply, reply
            assert any("isolated_missing_statistics_source" in row["exception"] for row in probe.calls[-1]["logs"])
            assert database_projection(probe) == before
        finally:
            probe.seed(player, "DROP VIEW statistics")
            probe.seed(player, "ALTER TABLE isolated_achievement_statistics RENAME TO statistics")
        restored = database_projection(probe)
        probe.clear_cooldowns()
        recovered = content(await probe.command("我的成就", "我的成就 初心不改", number=999410))
        assert "【初心不改】未完成 90%" in recovered and "修仙签到：9 / 10" in recovered, recovered
        assert database_projection(probe) == restored
        integrity(probe)
        probe.evidence.update(recovered=True, full_database_zero_writes=True, integrity_check="ok")

    @case("achievement-filters-pages-readonly", "我的成就", "已完成/未完成/类别/条件关键词与分页边界",
          "实际签到9、历史已解锁称号和真实配置", "六项分页/排序/本人隔离正常，别名/私聊/重复完整双库零写",
          group="achievement-parameters")
    async def filters_pages(probe):
        from nonebot.adapters.onebot.v11 import PrivateMessageEvent
        from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_title as module
        from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_title import title_data
        from tests import command_probe

        eligible_player(probe, module, count=9)
        probe.seed(probe.context.paths.player_db, "UPDATE title SET unlocked='[\"30001\"]' WHERE user_id='900001'")
        definitions = json.loads(title_data.TITLE_JSONPATH.read_text(encoding="utf-8"))
        total = sum(bool(value["condition"]) for value in definitions.values())
        before = database_projection(probe)
        keyword_pages = [["30002", "30003", "30004", "30005", "30112", "30051"], ["30052", "30053", "30001"]]

        async def query(argument, *, name="我的成就", actor="900001", number=999420):
            probe.clear_cooldowns()
            reply = content(await probe.command(name, f"{name} {argument}", actor=actor, number=number))
            assert database_projection(probe) == before
            assert "本次新解锁" not in reply
            return reply

        def check(reply, ids, *, page, pages, completed=1):
            assert f"【我的成就】{completed}/{total}" in reply and f"第{page}/{pages}页" in reply, reply
            actual = re.findall(r"^【([^\n]+)】(?:已完成|未完成|可解锁) \d+%$", reply, re.MULTILINE)
            assert actual == [definitions[title_id]["name"] for title_id in ids], (ids, actual, reply)
            for title_id in ids:
                assert definitions[title_id]["desc"] in reply

        for index, (argument, page) in enumerate((("修仙签到", 1), ("2 修仙签到", 2), ("修仙签到 999", 2),
                                                ("0 修仙签到", 1), ("修仙签到 2", 2))):
            reply = await query(argument, number=999420 + index)
            check(reply, keyword_pages[page - 1], page=page, pages=2)
            assert "筛选：修仙签到" in reply
            if page == 2:
                assert "【初心不改】已完成 100%" in reply and "修仙签到：9 / 10" in reply
            assert await query(argument, number=999420 + index) == reply
        completed = await query("已完成", number=999430)
        check(completed, ["30001"], page=1, pages=1)
        incomplete = await query("未完成 修仙签到", number=999431)
        check(incomplete, keyword_pages[0], page=1, pages=2)
        assert "筛选：修仙签到" in incomplete
        # An argument has one filter token; the last nonnumeric token is used.
        incomplete = await query("修仙签到 未完成", number=999432)
        assert "筛选：未完成" in incomplete and "【初心不改】" not in incomplete
        assert len(re.findall(r"^【[^\n]+】未完成 \d+%$", incomplete, re.MULTILINE)) == 6
        realm_ids = ["30093", "30094", "30095", "30096", "30097", "30098", "30099", "30100", "30107", "30108",
                     "30112", "30122", "30123", "30124", "30125"]
        for page in (1, 2, 3):
            reply = await query(f"境界突破 {page}", number=999433 + page)
            check(reply, realm_ids[(page - 1) * 6:page * 6], page=page, pages=3)
        no_match = await query("不存在的诊断成就", number=999437)
        check(no_match, [], page=1, pages=1)
        assert "暂无符合条件的成就" in no_match
        for index, name in enumerate(("成就列表", "查看成就")):
            check(await query("修仙签到 2", name=name, number=999438 + index), keyword_pages[1], page=2, pages=2)
        other = await query("初心不改", actor="900002", number=999440)
        assert "【初心不改】未完成 0%" in other and "修仙签到：0 / 10" in other
        assert f"【我的成就】0/{total}" in other
        unregistered = await query("", actor="900099", number=999441)
        assert "我要修仙" in unregistered and "【我的成就】" not in unregistered
        make_group = command_probe.make_event

        def make_private(text, actor, number):
            data = make_group(text, actor, number).model_dump()
            data.pop("group_id")
            data.update(message_type="private", sub_type="friend")
            return PrivateMessageEvent.model_validate(data)

        with patch.object(command_probe, "make_event", side_effect=make_private):
            check(await query("修仙签到 2", name="成就列表", number=999442), keyword_pages[1], page=2, pages=2)
        integrity(probe)
        probe.evidence.update(filters=["已完成", "未完成", "境界突破", "修仙签到", "不存在的诊断成就"],
                              pages=[1, 2, 3], private=True, aliases=True, full_database_zero_writes=True, integrity_check="ok")

    @case("achievement-compound-progress", "我的成就", "全能修士五条件99%到实际解锁100%",
          "实际统计/真实复合条件和应用事务", "按最少进度显示，未满足不得解锁，达到全部条件后实际保存和回执一致",
          group="achievement-parameters")
    async def compound_progress(probe):
        from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_title as module

        eligible_player(probe, module, count=99)
        values = {"历练次数": 499, "双修次数": 100, "炼丹次数": 50, "讨伐世界BOSS": 500}
        for key, value in values.items():
            probe.managers[1].update_or_write_data("900001", "statistics", key, value)
        probe.clear_cooldowns()
        reply = content(await probe.command("我的成就", "我的成就 全能修士", number=999460))
        assert "【全能修士】未完成 99%" in reply, reply
        for phrase in ("修仙签到：99 / 100", "历练次数：499 / 500", "双修次数：100 / 100", "炼丹次数：50 / 50", "讨伐世界BOSS：500 / 500"):
            assert phrase in reply
        before_ids = set(json.loads(module.title_application.get_state("900001")["unlocked"]))
        assert "30051" not in before_ids
        committed = database_projection(probe)
        probe.clear_cooldowns()
        repeated = content(await probe.command("我的成就", "我的成就 全能修士", number=999460))
        assert "【全能修士】未完成 99%" in repeated and "本次新解锁" not in repeated
        assert database_projection(probe) == committed
        probe.managers[1].update_or_write_data("900001", "statistics", "修仙签到", 100)
        probe.managers[1].update_or_write_data("900001", "statistics", "历练次数", 500)
        probe.clear_cooldowns()
        reply = content(await probe.command("我的成就", "我的成就 全能修士", number=999461))
        assert "本次新解锁" in reply and "【全能修士】已完成 100%" in reply, reply
        after_ids = set(json.loads(module.title_application.get_state("900001")["unlocked"]))
        assert after_ids - before_ids == {"30003", "30008", "30051"}, (before_ids, after_ids)
        receipt = probe.rows(probe.context.paths.player_db,
                             "SELECT result_status,title_id FROM title_transaction_operations WHERE operation_id=?",
                             ("title-unlock:999461:900001",))
        assert receipt == [("applied", "30003,30008,30051")], receipt
        committed = database_projection(probe)
        probe.clear_cooldowns()
        repeated = content(await probe.command("我的成就", "我的成就 全能修士", number=999461))
        assert "【全能修士】已完成 100%" in repeated and "本次新解锁" not in repeated
        assert database_projection(probe) == committed
        integrity(probe)
        probe.evidence.update(compound_progress=[99, 100], actual_unlock_delta=sorted(after_ids - before_ids),
                              actual_receipt=receipt, repeat_zero_writes=True, integrity_check="ok")
