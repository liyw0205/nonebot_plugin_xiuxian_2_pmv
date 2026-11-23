"""Player dungeon status: real queries, initialization and isolated write faults."""

from unittest.mock import AsyncMock, patch

from tests.parameter_cases_dungeon_exit import live_manager
from tests.parameter_cases_team_leave import integrity
from tests.parameter_cases_team_view import database_projection


def register(case):
    @case("dungeon-status-phases-readonly", "我的副本状态", "四阶段/层数/百分比、别名/私聊、重复查询",
          "已发布的真实副本，隔离双库和固定业务钟", "实际状态一致，初始化后查询完整双库零写",
          group="dungeon-status-parameters")
    async def phases_readonly(probe):
        from nonebot.adapters.onebot.v11 import PrivateMessageEvent
        from tests import command_probe

        player = probe.context.paths.player_db
        with live_manager() as (module, manager):
            status = manager.get_player_status("900001")
            for index, (phase, layer, label) in enumerate((("not_started", 0, "未开始"), ("exploring", 2, "探索中"),
                                                         ("exited", 2, "已退出"), ("completed", status["total_layers"], "已完成"))):
                probe.seed(player, "UPDATE player_dungeon_status SET dungeon_status=?,current_layer=? WHERE user_id='900001'", (phase, layer))
                before = database_projection(probe)
                name = ("我的副本状态", "副本状态", "我的副本信息")[index % 3]
                reply = await probe.command(name, number=979500 + index)
                for phrase in (f"副本：{status['dungeon_name']}", f"状态：{label}",
                               f"层数：{layer}/{status['total_layers']}层", f"进度：{layer / status['total_layers'] * 100:.1f}%"):
                    assert phrase in reply, (phrase, reply)
                assert database_projection(probe) == before
                assert await probe.command(name, number=979500 + index) == reply
                assert database_projection(probe) == before
            make_group = command_probe.make_event

            def make_private(text, actor, number):
                data = make_group(text, actor, number).model_dump()
                data.pop("group_id")
                data.update(message_type="private", sub_type="friend")
                return PrivateMessageEvent.model_validate(data)

            before = database_projection(probe)
            with patch.object(command_probe, "make_event", side_effect=make_private):
                private = await probe.command("副本状态", number=979510)
            assert "状态：已完成" in private and "进度：100.0%" in private, private
            assert database_projection(probe) == before
        integrity(probe)
        probe.evidence.update(phases_and_progress=True, aliases_and_private=True, readonly=True, integrity_check="ok")

    @case("dungeon-status-initialization-recovery", "我的副本状态", "无记录/旧代际的合法查询遇到初始化拒写或改写",
          "真实manager和隔离玩家库trigger，已存在今日完整发布", "说明具体保存原因、整笔回滚，解除故障原消息恢复，再查询零写",
          group="dungeon-status-parameters")
    async def initialization_recovery(probe):
        player = probe.context.paths.player_db
        with live_manager() as (module, manager):
            status = manager.get_player_status("900001")
            faults = (
                ("missing", "BEFORE INSERT", "SELECT RAISE(IGNORE)", "副本状态初始化保存失败"),
                ("missing", "AFTER INSERT", "UPDATE player_dungeon_status SET dungeon_name='changed' WHERE user_id=NEW.user_id", "副本状态初始化结果与当前副本不一致"),
                ("stale", "BEFORE UPDATE", "SELECT RAISE(IGNORE)", "副本状态初始化保存失败"),
                ("stale", "AFTER UPDATE", "UPDATE player_dungeon_status SET current_layer=99 WHERE user_id=NEW.user_id", "副本状态初始化结果与当前副本不一致"),
            )
            for index, (kind, timing, body, phrase) in enumerate(faults):
                if kind == "missing":
                    probe.seed(player, "DELETE FROM player_dungeon_status WHERE user_id='900001'")
                else:
                    manager.get_player_status("900001")
                    probe.seed(player, "UPDATE player_dungeon_status SET dungeon_status='exploring',current_layer=2,reset_generation=0 WHERE user_id='900001'")
                before = database_projection(probe)
                probe.seed(player, f"CREATE TRIGGER status_fault {timing} ON player_dungeon_status WHEN NEW.user_id='900001' BEGIN "
                           f"INSERT OR IGNORE INTO team_cd(user_id,join_cd_until) VALUES('side-effect','touched'); {body}; END")
                try:
                    failed = await probe.command("我的副本状态", number=979550 + index)
                    probe.evidence[str(index)] = {"reply": failed, "rows": probe.rows(player, "SELECT * FROM player_dungeon_status WHERE user_id='900001'")}
                    assert phrase in failed and "【副本信息】" not in failed, failed
                    assert database_projection(probe) == before
                finally:
                    probe.seed(player, "DROP TRIGGER status_fault")
                recovered = await probe.command("我的副本状态", number=979550 + index)
                assert "状态：未开始" in recovered and f"层数：0/{status['total_layers']}层" in recovered, recovered
                actual = manager.get_player_status("900001")
                assert actual == status, (status, actual)
                committed = database_projection(probe)
                assert await probe.command("我的副本状态", number=979550 + index) == recovered
                assert database_projection(probe) == committed
                probe.evidence[str(index)].update(rollback=True, recovered=True, repeat_zero_writes=True)
        integrity(probe)
        probe.evidence.update(integrity_check="ok")

    @case("dungeon-status-abort-reply-failure", "我的副本状态", "新建/旧代际初始化ABORT及初始化提交后的回复失败",
          "隔离双库真实指令，mock发送故障", "ABORT原异常/定位号、完整回滚和恢复；发送失败保持初始化已提交",
          group="dungeon-status-parameters")
    async def abort_reply_failure(probe):
        player = probe.context.paths.player_db
        with live_manager() as (module, manager):
            initial = manager.get_player_status("900001")
            for index, (missing, timing) in enumerate(((True, "BEFORE INSERT"), (False, "BEFORE UPDATE"))):
                if missing:
                    probe.seed(player, "DELETE FROM player_dungeon_status WHERE user_id='900001'")
                else:
                    probe.seed(player, "UPDATE player_dungeon_status SET reset_generation=0 WHERE user_id='900001'")
                before = database_projection(probe)
                probe.seed(player, f"CREATE TRIGGER status_abort {timing} ON player_dungeon_status WHEN NEW.user_id='900001' BEGIN "
                           "INSERT OR IGNORE INTO team_cd(user_id,join_cd_until) VALUES('side-effect','touched'); "
                           "SELECT RAISE(ABORT,'isolated status initialization abort'); END")
                try:
                    failed = await probe.command("我的副本状态", number=979580 + index, allow_exception=True)
                    assert "定位号：" in failed and "内部异常" in failed and "【副本信息】" not in failed, failed
                    assert any("isolated status initialization abort" in row["exception"] for row in probe.calls[-1]["logs"])
                    assert database_projection(probe) == before
                finally:
                    probe.seed(player, "DROP TRIGGER status_abort")
                recovered = await probe.command("我的副本状态", number=979580 + index)
                assert "状态：未开始" in recovered, recovered
                assert manager.get_player_status("900001") == initial
            probe.seed(player, "DELETE FROM player_dungeon_status WHERE user_id='900001'")
            with patch.object(module, "handle_send", AsyncMock(side_effect=RuntimeError("isolated committed status query reply failure"))):
                failed = await probe.command("我的副本状态", number=979590, allow_exception=True)
            assert "定位号：" in failed and "内部异常" in failed, failed
            assert any("isolated committed status query reply failure" in row["exception"] for row in probe.calls[-1]["logs"])
            assert manager.get_player_status("900001") == initial
            committed = database_projection(probe)
            assert "状态：未开始" in await probe.command("副本状态", number=979590)
            assert database_projection(probe) == committed
        integrity(probe)
        probe.evidence.update(abort_rollback_recovery=True, reply_failure_committed=True,
                              repeat_readonly=True, integrity_check="ok")
