"""Exit real dungeon sessions using the live manager and isolated SQLite."""

from contextlib import contextmanager
from unittest.mock import AsyncMock, patch

from tests.parameter_cases_team_invite import clock
from tests.parameter_cases_team_leave import integrity
from tests.parameter_cases_team_view import database_projection


@contextmanager
def live_manager():
    from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_dungeon as module

    with patch.object(module, "runtime_clock", clock()):
        manager = module._dungeon_manager()
        with patch.object(manager, "clock", clock()):
            yield module, manager


def exploring(probe, manager):
    initial = manager.get_player_status("900001")
    assert initial["total_layers"] > 2, initial
    probe.seed(probe.context.paths.player_db,
               "UPDATE player_dungeon_status SET dungeon_status='exploring',current_layer=2 WHERE user_id='900001'")
    return probe.rows(probe.context.paths.player_db, "SELECT * FROM player_dungeon_status WHERE user_id='900001'")[0]


def unrelated(state):
    return {name: [table for table in tables if table[0]["name"] not in {"player_dungeon_status", "dungeon_session_operations"}]
            for name, tables in state.items()}


def register(case):
    @case("dungeon-exit-write-recovery", "退出副本", "合法探索状态，状态/回执拒写或改写",
          "真实DungeonManager/matcher和隔离双库，固定业务钟", "具体原因/玩家subject、整笔回滚、原消息恢复/replay",
          group="dungeon-exit-parameters")
    async def write_recovery(probe):
        player = probe.context.paths.player_db
        faults = (
            ("BEFORE UPDATE ON player_dungeon_status", "NEW.user_id='900001'", "SELECT RAISE(IGNORE)", "state_write_rejected", "退出副本状态保存失败"),
            ("AFTER UPDATE ON player_dungeon_status", "NEW.user_id='900001'", "UPDATE player_dungeon_status SET dungeon_name='changed' WHERE user_id=NEW.user_id", "state_projection_mismatch", "退出副本后的状态与原进度不一致"),
            ("BEFORE INSERT ON dungeon_session_operations", "1", "SELECT RAISE(IGNORE)", "receipt_write_rejected", "退出副本回执保存失败"),
            ("AFTER INSERT ON dungeon_session_operations", "1", "UPDATE dungeon_session_operations SET payload='{}' WHERE operation_id=NEW.operation_id", "receipt_projection_mismatch", "退出副本回执与请求不一致"),
            ("AFTER INSERT ON dungeon_session_operations", "1", "UPDATE player_dungeon_status SET current_layer=4 WHERE user_id='900001'", "state_projection_mismatch", "退出副本后的状态与原进度不一致"),
        )
        with live_manager() as (module, manager):
            for index, (timing, condition, body, reason, phrase) in enumerate(faults):
                original = exploring(probe, manager)
                before = database_projection(probe)
                number = 979100 + index
                operation = f"dungeon-exit:{number}:900001"
                probe.seed(player, f"CREATE TRIGGER departure_fault {timing} WHEN {condition} BEGIN "
                           f"INSERT OR IGNORE INTO team_cd(user_id,join_cd_until) VALUES('side-effect','touched'); {body}; END")
                try:
                    failed = await probe.command("退出副本", number=number)
                    assert phrase in failed and "玩家" in failed and "已退出当前副本" not in failed, failed
                    assert database_projection(probe) == before
                    assert module.dungeon_application.session_operation(operation_id=operation, user_id="900001", action="exit") is None
                finally:
                    probe.seed(player, "DROP TRIGGER departure_fault")
                applied = await probe.command("退出副本", number=number)
                assert "已退出当前副本，进度将保留" in applied, applied
                row = probe.rows(player, "SELECT * FROM player_dungeon_status WHERE user_id='900001'")[0]
                assert row == (*original[:3], "exited", *original[4:]), (original, row)
                assert unrelated(database_projection(probe)) == unrelated(before)
                receipt = probe.rows(player, "SELECT result_status,dungeon_status FROM dungeon_session_operations WHERE operation_id=?", (operation,))
                assert receipt == [("applied", "exited")], receipt
                committed = database_projection(probe)
                replay = await probe.command("离开副本", number=number)
                assert replay == applied and database_projection(probe) == committed, replay
                probe.evidence[str(index)] = dict(reason="dungeon_exit_" + reason, reply=failed,
                                                 rollback=True, recovered=True, replay_zero_writes=True)
        integrity(probe)
        probe.evidence.update(integrity_check="ok")

    @case("dungeon-exit-phase-boundaries", "退出副本", "未探索/探索/已退出/已完成、别名/私聊、跨代际成功重放",
          "隔离真实副本发布和玩家进度，固定业务钟", "只探索状态可退出，同代际进度不清零，历史回执零写",
          group="dungeon-exit-parameters")
    async def phase_boundaries(probe):
        from nonebot.adapters.onebot.v11 import PrivateMessageEvent
        from tests import command_probe

        player = probe.context.paths.player_db
        with live_manager() as (module, manager):
            manager.get_player_status("900001")
            for index, (phase, layer, phrase) in enumerate((
                ("not_started", 0, "当前未在副本探索中"), ("exploring", 2, "已退出当前副本"),
                ("exited", 2, "当前未在副本探索中"), ("completed", 5, "今日副本已完成"),
            )):
                probe.seed(player, "UPDATE player_dungeon_status SET dungeon_status=?,current_layer=? WHERE user_id='900001'", (phase, layer))
                original = probe.rows(player, "SELECT * FROM player_dungeon_status WHERE user_id='900001'")[0]
                before = database_projection(probe)
                reply = await probe.command("离开副本", number=979200 + index)
                assert phrase in reply, reply
                expected_phase = "exited" if phase == "exploring" else phase
                assert probe.rows(player, "SELECT * FROM player_dungeon_status WHERE user_id='900001'") == [(*original[:3], expected_phase, *original[4:])]
                assert unrelated(database_projection(probe)) == unrelated(before)
                committed = database_projection(probe)
                assert await probe.command("退出副本", number=979200 + index) == reply
                assert database_projection(probe) == committed

            original = exploring(probe, manager)
            make_group = command_probe.make_event

            def make_private(text, actor, number):
                data = make_group(text, actor, number).model_dump()
                data.pop("group_id")
                data.update(message_type="private", sub_type="friend")
                return PrivateMessageEvent.model_validate(data)

            with patch.object(command_probe, "make_event", side_effect=make_private):
                private = await probe.command("离开副本", number=979210)
            assert "已退出当前副本" in private, private
            assert probe.rows(player, "SELECT current_layer FROM player_dungeon_status WHERE user_id='900001'") == [(2,)]
            manager.reset_dungeon("exit-next-generation", source="manual")
            manager.get_player_status("900001")
            committed = database_projection(probe)
            with patch.object(manager, "get_player_status", side_effect=AssertionError("replay must not read live state")):
                replay = await probe.command("退出副本", number=979210)
            assert "已退出当前副本" in replay and database_projection(probe) == committed, replay
            assert probe.rows(player, "SELECT dungeon_status,current_layer,reset_generation FROM player_dungeon_status WHERE user_id='900001'") == [("not_started", 0, original[7] + 1)]
        integrity(probe)
        probe.evidence.update(phases=True, progress_preserved=True, private_and_alias=True,
                              old_success_replay_after_reset=True, integrity_check="ok")

    @case("dungeon-exit-snapshot-boundary", "退出副本", "读快照后同模板/进度的真实刷新代际、拒绝回放、同日重新退出",
          "真实manager发布第二代际并恢复探索形成ABA快照", "具体刷新原因/subject，旧拒绝稳定回放，新消息可退出",
          group="dungeon-exit-parameters")
    async def snapshot_boundary(probe):
        with live_manager() as (module, manager):
            exploring(probe, manager)
            transition = module.dungeon_application.session_transition

            def reset_before_transition(**kwargs):
                with patch.object(manager.random, "choice", return_value=manager.current_dungeon):
                    manager.reset_dungeon("exit-racing-reset", source="manual")
                exploring(probe, manager)
                return transition(**kwargs)

            with patch.object(module.dungeon_application, "session_transition", side_effect=reset_before_transition):
                denied = await probe.command("退出副本", number=979250)
            assert "副本刷新记录与发起时不一致" in denied and "玩家" in denied and "已退出当前副本" not in denied, denied
            result = module.dungeon_application.session_operation(operation_id="dungeon-exit:979250:900001", user_id="900001", action="exit")
            assert (result["status"], result["reason"], result["subject"]) == ("state_changed", "dungeon_reset_changed", "900001"), result
            exploring(probe, manager)
            before = database_projection(probe)
            assert await probe.command("离开副本", number=979250) == denied
            assert database_projection(probe) == before
            success = await probe.command("退出副本", number=979251)
            assert "已退出当前副本" in success, success
        integrity(probe)
        probe.evidence.update(real_reset_race=True, rejection_subject="900001", rejection_replay=True,
                              new_message_recovery=True, integrity_check="ok")

    @case("dungeon-exit-abort-and-reply-failure", "退出副本", "两个写点ABORT、业务提交后发送失败",
          "隔离双库trigger和mock发送故障", "原异常/定位号、完整回滚、原消息恢复；发送失败保持已提交语义",
          group="dungeon-exit-parameters")
    async def abort_and_reply_failure(probe):
        player = probe.context.paths.player_db
        with live_manager() as (module, manager):
            for index, (timing, condition) in enumerate((("BEFORE UPDATE ON player_dungeon_status", "NEW.user_id='900001'"),
                                                        ("BEFORE INSERT ON dungeon_session_operations", "1"))):
                exploring(probe, manager)
                before = database_projection(probe)
                number = 979280 + index
                probe.seed(player, f"CREATE TRIGGER departure_abort {timing} WHEN {condition} BEGIN "
                           "INSERT OR IGNORE INTO team_cd(user_id,join_cd_until) VALUES('side-effect','touched'); "
                           "SELECT RAISE(ABORT,'isolated dungeon departure abort'); END")
                try:
                    failed = await probe.command("退出副本", number=number, allow_exception=True)
                    assert "定位号：" in failed and "内部异常" in failed and "已退出当前副本" not in failed, failed
                    assert any("isolated dungeon departure abort" in row["exception"] for row in probe.calls[-1]["logs"])
                    assert database_projection(probe) == before
                finally:
                    probe.seed(player, "DROP TRIGGER departure_abort")
                recovered = await probe.command("退出副本", number=number)
                assert "已退出当前副本" in recovered, recovered
            exploring(probe, manager)
            with patch.object(module, "handle_send", AsyncMock(side_effect=RuntimeError("isolated committed dungeon departure reply failure"))):
                failed = await probe.command("退出副本", number=979290, allow_exception=True)
            assert "定位号：" in failed and "内部异常" in failed, failed
            assert any("isolated committed dungeon departure reply failure" in row["exception"] for row in probe.calls[-1]["logs"])
            assert probe.rows(player, "SELECT dungeon_status,current_layer FROM player_dungeon_status WHERE user_id='900001'") == [("exited", 2)]
            assert module.dungeon_application.session_operation(operation_id="dungeon-exit:979290:900001", user_id="900001", action="exit")["status"] == "duplicate"
            committed = database_projection(probe)
            assert "已退出当前副本" in await probe.command("离开副本", number=979290)
            assert database_projection(probe) == committed
        integrity(probe)
        probe.evidence.update(abort_rollback_and_recovery=True, reply_failure_committed=True,
                              replay_zero_writes=True, integrity_check="ok")
