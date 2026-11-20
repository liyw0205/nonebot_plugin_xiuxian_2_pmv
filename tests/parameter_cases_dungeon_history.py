"""Personal dungeon history queried from actual settlement rows and isolated data."""

import json
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import patch

from tests.parameter_cases_dungeon_exit import live_manager
from tests.parameter_cases_team_invite import clock
from tests.parameter_cases_team_leave import integrity
from tests.parameter_cases_team_view import database_projection


def add_history(probe, operation, *, user="900001", stamp="2026-11-19 12:00:00", layer=2,
                phase="completed", result="applied", state="exploring", message="历史探索", battles=0, response=None):
    identity = json.dumps({"action": "explore", "user_id": user}, ensure_ascii=True, sort_keys=True)
    response = response if response is not None else json.dumps({"message": message, "battle_messages": ["battle"] * battles})
    probe.seed(probe.context.paths.game_db,
               "INSERT INTO dungeon_explore_operations(operation_id,request_identity,phase,prepared_json,result_status,"
               "result_json,current_layer,dungeon_status,created_at,updated_at) VALUES(?,?,?,'{}',?,?,?,?,?,?)",
               (operation, identity, phase, result, response, layer, state, stamp, stamp))


def register(case):
    @case("dungeon-history-actual-settlement", "我的副本记录", "真实应用结算回执、原生时间格式和查询",
          "隔离双库，真实intent/prepare/settle，无战斗或奖励替身", "回执实际落库且查询显示本次结果，重复/别名完整两库零写",
          group="dungeon-history-parameters")
    async def actual_settlement(probe):
        from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_dungeon as module

        with live_manager() as (_, manager):
            status = manager.get_player_status("900001")
            user = probe.rows(probe.context.paths.game_db, "SELECT hp,mp,stone,exp FROM user_xiuxian WHERE user_id='900001'")[0]
            probe.seed(probe.context.paths.game_db, "UPDATE user_cd SET type=0 WHERE user_id='900001'")
            operation = "history-actual-settlement"
            app = module.dungeon_application
            intent = app.prepare_intent(operation_id=operation, user_id="900001", intent={"source": "history-acceptance"})
            assert intent["phase"] == "intent", intent
            plan = {"expected_status": status, "team": None,
                    "members": [{"user_id": "900001", "expected": {**dict(zip(("hp", "mp", "stone", "exp"), user)), "cd_type": 0},
                                 "final_hp": user[0], "final_mp": user[1], "stone_delta": 0, "exp_delta": 0, "items": []}],
                    "advance": True, "complete": False, "response": {"message": "真实结算记录", "battle_messages": []}}
            prepared = app.prepare_resolution(operation_id=operation, user_id="900001", plan=plan)
            assert prepared["phase"] == "prepared", prepared
            settled = app.settle(operation_id=operation, user_id="900001", max_goods_num=999999)
            assert settled["result_status"] == "applied", settled
        row = probe.rows(probe.context.paths.game_db,
                         "SELECT phase,result_status,result_json,updated_at FROM dungeon_explore_operations WHERE operation_id=?", (operation,))[0]
        assert row[:2] == ("completed", "applied"), row
        assert len(row[3]) == 19 and row[3][10] == " ", row
        moment = datetime.fromisoformat(row[3]).replace(tzinfo=timezone.utc)
        with patch.object(module.dungeon_explore_history_application, "_clock", SimpleNamespace(now=lambda: moment)):
            before = database_projection(probe)
            for index, name in enumerate(("我的副本记录", "副本记录", "副本探索记录")):
                reply = await probe.command(name, number=979800 + index)
                for phrase in ("探索：1 次（通关 0）", "最高层：1 层", "战斗 0 场", "真实结算记录", row[3]):
                    assert phrase in reply, (phrase, reply)
                assert database_projection(probe) == before
        integrity(probe)
        probe.evidence.update(actual_receipt=row, utc_current_timestamp=True, readonly=True, aliases=True, integrity_check="ok")

    @case("dungeon-history-window-summary", "我的副本记录", "7天精确边界、最近10条、本人隔离、通关/战斗摘要及私聊",
          "隔离真实schema的已完成/准备中/拒绝/坏JSON历史，固定UTC业务钟", "只计本人有效成功记录，摘要总数与最近列表正确，完整双库零写",
          group="dungeon-history-parameters")
    async def window_summary(probe):
        from nonebot.adapters.onebot.v11 import PrivateMessageEvent
        from tests import command_probe
        from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_dungeon as module

        for index in range(11):
            add_history(probe, f"valid-{index}", stamp=f"2026-11-19 12:00:{index:02d}", layer=index + 1,
                        state="completed" if index == 10 else "exploring", message=f"有效结果{index}", battles=1)
        add_history(probe, "exact-boundary", stamp="2026-11-13 12:00:00", layer=12, message="精确边界", battles=2)
        for operation, changes in (
            ("expired", {"stamp": "2026-11-13 11:59:59", "message": "已过期"}),
            ("other-player", {"user": "900002", "message": "他人结果", "layer": 99}),
            ("prepared", {"phase": "prepared", "result": "", "message": "未结算"}),
            ("rejected", {"result": "state_changed", "message": "拒绝结果"}),
            ("bad-json", {"response": "not-json"}),
            ("list-json", {"response": "[]"}),
        ):
            add_history(probe, operation, **changes)
        with patch.object(module.dungeon_explore_history_application, "_clock", clock()):
            before = database_projection(probe)
            first = await probe.command("我的副本记录", number=979830)
            for phrase in ("探索：12 次（通关 1）", "最高层：12 层", "战斗 13 场", "有效结果10", "有效结果1"):
                assert phrase in first, (phrase, first)
            assert first.count("第") == 10, first
            for phrase in ("有效结果0", "精确边界", "已过期", "他人结果", "未结算", "拒绝结果"):
                assert phrase not in first, (phrase, first)
            recent_messages = [line.rsplit("：", 1)[-1] for line in first.splitlines() if line.startswith("- ")]
            assert recent_messages == [f"有效结果{index}" for index in range(10, 0, -1)], first
            assert database_projection(probe) == before
            assert await probe.command("副本记录", number=979830) == first
            assert database_projection(probe) == before
            other = await probe.command("我的副本记录", actor="900002", number=979831)
            assert "他人结果" in other and "探索：1 次" in other and "有效结果" not in other, other
            assert database_projection(probe) == before
            empty = await probe.command("我的副本记录", actor="900003", number=979832)
            assert "近7天没有副本探索记录" in empty, empty
            assert database_projection(probe) == before
            make_group = command_probe.make_event

            def make_private(text, actor, number):
                data = make_group(text, actor, number).model_dump()
                data.pop("group_id")
                data.update(message_type="private", sub_type="friend")
                return PrivateMessageEvent.model_validate(data)

            with patch.object(command_probe, "make_event", side_effect=make_private):
                private = await probe.command("副本探索记录", number=979833)
            assert "探索：12 次（通关 1）" in private and "有效结果10" in private, private
            assert database_projection(probe) == before
        integrity(probe)
        probe.evidence.update(exact_seven_days=True, total=12, recent=10, ownership=True, private=True,
                              corrupt_json_skipped=True, readonly=True, integrity_check="ok")

    @case("dungeon-history-invalid-time", "我的副本记录", "实际历史表中的非法时间不能作为近7天成功记录",
          "隔离schema保留合法及坏时间的成功历史行", "坏时间不计入/不展示，合法记录仍可见，历史和两库均不改写",
          group="dungeon-history-parameters")
    async def invalid_time(probe):
        from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_dungeon as module

        add_history(probe, "valid-time", message="合法时间结果", layer=2, battles=1)
        add_history(probe, "corrupt-time", stamp="not-a-time", message="非法时间结果", layer=999)
        before = database_projection(probe)
        with patch.object(module.dungeon_explore_history_application, "_clock", clock()):
            reply = await probe.command("我的副本记录", number=979860)
        probe.evidence.update(reply=reply, actual_rows=probe.rows(probe.context.paths.game_db,
                              "SELECT operation_id,updated_at,current_layer FROM dungeon_explore_operations ORDER BY operation_id"))
        assert "探索：1 次" in reply and "最高层：2 层" in reply and "合法时间结果" in reply, reply
        assert "非法时间结果" not in reply and "not-a-time" not in reply and "999" not in reply, reply
        assert database_projection(probe) == before
        integrity(probe)
        probe.evidence.update(invalid_time_skipped=True, readonly=True, integrity_check="ok")
