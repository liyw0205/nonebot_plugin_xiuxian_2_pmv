"""Bounded departure commands against real matchers and isolated SQLite."""

import json
from unittest.mock import AsyncMock, patch

from tests.parameter_cases_assets import balances, backpack
from tests.parameter_cases_team_invite import clock, projection as team_projection


def projection(probe):
    return {**team_projection(probe), "dungeon_team_exit_operations": probe.rows(
        probe.context.paths.player_db, "SELECT * FROM dungeon_team_exit_operations ORDER BY rowid")}


def integrity(probe):
    from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork

    for database in (probe.context.paths.player_db, probe.context.paths.game_db):
        with DatabaseUnitOfWork(database, read_only=True) as uow:
            assert uow.query_one("PRAGMA integrity_check") == {"integrity_check": "ok"}


def register(case):
    @case("dungeon-team-leave-write-recovery", "离开队伍", "合法成员离队、五阶段拒写/改写与ABORT",
          "真实创建/邀请/加入和隔离双库，固定业务时间", "十类原因/subject、整笔回滚、同消息恢复和永久重放",
          group="dungeon-team-leave-parameters")
    async def leave_write_recovery(probe):
        from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_dungeon as module

        player = probe.context.paths.player_db
        assets = balances(probe), backpack(probe)
        faults = (
            ("BEFORE UPDATE ON teams", "1", "SELECT RAISE(IGNORE)", "离队状态保存失败", "team"),
            ("AFTER UPDATE ON teams", "1", "UPDATE teams SET description='changed' WHERE user_id=NEW.user_id", "离队状态与请求不一致", "team"),
            ("BEFORE DELETE ON dungeon_team_members", "1", "SELECT RAISE(IGNORE)", "离队成员索引保存失败", "team"),
            ("BEFORE INSERT ON dungeon_team_members", "1", "SELECT RAISE(IGNORE)", "离队成员索引保存失败", "team"),
            ("AFTER INSERT ON dungeon_team_members", "1", "UPDATE dungeon_team_members SET version=99 WHERE team_id=NEW.team_id", "离队成员索引与请求不一致", "team"),
            ("BEFORE INSERT ON team_cd", "NEW.user_id='900002'", "SELECT RAISE(IGNORE)", "离队冷却保存失败", "player"),
            ("AFTER UPDATE ON team_cd", "NEW.user_id='900002'", "UPDATE team_cd SET had_first_join=0 WHERE user_id=NEW.user_id", "离队冷却与原记录不一致", "player"),
            ("BEFORE UPDATE ON dungeon_team_invites", "OLD.status='pending'", "SELECT RAISE(IGNORE)", "离队关联邀请保存失败", "invite"),
            ("AFTER UPDATE ON dungeon_team_invites", "NEW.status='team_changed'", "UPDATE dungeon_team_invites SET bot_id='changed' WHERE invite_id=NEW.invite_id", "离队关联邀请与原记录不一致", "invite"),
            ("BEFORE INSERT ON dungeon_team_exit_operations", "1", "SELECT RAISE(IGNORE)", "离队操作回执保存失败", "team"),
            ("AFTER INSERT ON dungeon_team_exit_operations", "1", "UPDATE dungeon_team_exit_operations SET result_json='{}' WHERE operation_id=NEW.operation_id", "离队操作回执与请求不一致", "team"),
            ("AFTER INSERT ON dungeon_team_exit_operations", "1", "UPDATE team_cd SET join_cd_until='changed' WHERE user_id='900002'", "离队冷却与原记录不一致", "player"),
            ("BEFORE UPDATE ON teams", "1", "SELECT RAISE(ABORT,'isolated command departure abort')", None, "team"),
            ("BEFORE DELETE ON dungeon_team_members", "1", "SELECT RAISE(ABORT,'isolated command departure abort')", None, "team"),
            ("BEFORE INSERT ON dungeon_team_members", "1", "SELECT RAISE(ABORT,'isolated command departure abort')", None, "team"),
            ("BEFORE INSERT ON team_cd", "NEW.user_id='900002'", "SELECT RAISE(ABORT,'isolated command departure abort')", None, "player"),
            ("BEFORE UPDATE ON dungeon_team_invites", "OLD.status='pending'", "SELECT RAISE(ABORT,'isolated command departure abort')", None, "invite"),
            ("BEFORE INSERT ON dungeon_team_exit_operations", "1", "SELECT RAISE(ABORT,'isolated command departure abort')", None, "team"),
        )
        with patch.object(module, "runtime_clock", clock()):
            await probe.command("创建队伍", "创建队伍 离队故障队", number=975100)
            team_id = module.dungeon_team_application.team_id_for_user("900001")
            for index, (timing, condition, body, phrase, subject) in enumerate(faults):
                invite_number, join_number, pending_number, number = 975110 + index, 975150 + index, 975190 + index, 975230 + index
                probe.seed(player, "UPDATE team_cd SET join_cd_until='' WHERE user_id='900002'")
                invited = await probe.command("邀请组队", "邀请组队 诊断对手", number=invite_number)
                assert "已向诊断对手发送组队邀请" in invited, invited
                joined = await probe.command("同意组队", actor="900002", number=join_number)
                assert "成功加入队伍" in joined, joined
                await probe.command("邀请组队", "邀请组队 诊断管理", number=pending_number)
                pending_id = f"dungeon-team-invite:{pending_number}:900001:900003"
                before = projection(probe)
                probe.seed(player, f"CREATE TRIGGER departure_fault {timing} WHEN {condition} BEGIN INSERT OR IGNORE INTO team_cd(user_id,join_cd_until) VALUES('side-effect','touched'); {body}; END")
                try:
                    failed = await probe.command("离开队伍", actor="900002", number=number, allow_exception=phrase is None)
                    assert "你已离开队伍" not in failed, failed
                    if phrase:
                        label = {"team": "队伍【离队故障队】", "player": "离队玩家", "invite": f"邀请【{pending_id}】"}[subject]
                        assert phrase in failed and label in failed, failed
                    else:
                        assert "定位号：" in failed and "内部异常" in failed, failed
                        assert any("isolated command departure abort" in row["exception"] for row in probe.calls[-1]["logs"])
                    assert projection(probe) == before
                    probe.evidence[str(index)] = {"reply": failed, "logs": probe.calls[-1]["logs"], "rollback": True}
                finally:
                    probe.seed(player, "DROP TRIGGER departure_fault")
                recovered = await probe.command("离开队伍", actor="900002", number=number)
                assert "你已离开队伍【离队故障队】" in recovered and "2026-11-20 15:00:00" in recovered, recovered
                assert "队长已转让" not in recovered, recovered
                committed = projection(probe)
                replay = await probe.command("退出队伍", actor="900002", number=number)
                assert recovered == replay and projection(probe) == committed, replay
                assert probe.rows(player, "SELECT members,leader FROM teams WHERE user_id=?", (team_id,)) == [(json.dumps(["900001"]), "900001")]
                version = probe.rows(player, "SELECT version FROM teams WHERE user_id=?", (team_id,))[0][0]
                assert probe.rows(player, "SELECT member_id,version FROM dungeon_team_members WHERE team_id=?", (team_id,)) == [("900001", version)]
                assert probe.rows(player, "SELECT join_cd_until,had_first_join FROM team_cd WHERE user_id='900002'") == [("2026-11-20 15:00:00", 1)]
                assert probe.rows(player, "SELECT status,resolved_operation_id,bot_id FROM dungeon_team_invites WHERE invite_id=?", (pending_id,)) == [("team_changed", f"dungeon-team-leave:{number}:900002", "900000")]
                assert len(probe.rows(player, "SELECT result_json FROM dungeon_team_exit_operations WHERE operation_id=?", (f"dungeon-team-leave:{number}:900002",))) == 1
                probe.evidence[str(index)].update(recovered=True, replay_zero_writes=True, version=version)
        assert (balances(probe), backpack(probe)) == assets
        integrity(probe)
        probe.evidence.update(assets_unchanged=True, integrity_check="ok")

    @case("dungeon-team-leave-normal", "离开队伍", "单人、成员、队长/活动会话/私聊与原消息重放",
          "固定时钟，隔离真实队伍与冷却记录", "原离队/转让/解散规则不变，回复与实际冷却一致且旧请求稳定",
          group="dungeon-team-leave-parameters")
    async def leave_normal(probe):
        from nonebot.adapters.onebot.v11 import PrivateMessageEvent
        from tests import command_probe
        from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_dungeon as module

        player = probe.context.paths.player_db
        assets = balances(probe), backpack(probe)
        with patch.object(module, "runtime_clock", clock()):
            missing = await probe.command("离开队伍", number=975300)
            assert "你不在任何队伍中" in missing, missing
            await probe.command("创建队伍", "创建队伍 初次单人队", number=975301)
            solo = await probe.command("退出队伍", number=975302)
            assert "队伍已解散" in solo and "冷却" not in solo, solo
            assert module.dungeon_team_application.snapshot("900100_975301") is None
            assert probe.rows(player, "SELECT * FROM team_cd WHERE user_id='900001'") == []
            committed_solo = projection(probe)
            assert await probe.command("离开队伍", number=975302) == solo
            assert projection(probe) == committed_solo
            await probe.command("创建队伍", "创建队伍 队长离开队", number=975303)
            assert await probe.command("离开队伍", number=975300) == missing
            assert module.dungeon_team_application.snapshot("900100_975303") is not None
            await probe.command("邀请组队", "邀请组队 诊断对手", number=975304)
            await probe.command("同意组队", actor="900002", number=975305)
            for index, actor in enumerate(("900001", "900002")):
                probe.seed(player, "INSERT INTO player_dungeon_status(user_id,dungeon_status) VALUES(?,'exploring') "
                           "ON CONFLICT(user_id) DO UPDATE SET dungeon_status='exploring'", (actor,))
                before = projection(probe)
                active = await probe.command("离开队伍", number=975306 + index)
                assert "副本探索会话进行中" in active, active
                for table in before.keys() - {"dungeon_team_exit_operations"}:
                    assert projection(probe)[table] == before[table], table
                probe.seed(player, "UPDATE player_dungeon_status SET dungeon_status='not_started' WHERE user_id=?", (actor,))
            rejected_state = projection(probe)
            assert "副本探索会话进行中" in await probe.command("离开队伍", number=975306)
            assert projection(probe) == rejected_state
            make_group = command_probe.make_event

            def make_private(text, actor, number):
                data = make_group(text, actor, number).model_dump()
                data.pop("group_id")
                data.update(message_type="private", sub_type="friend")
                return PrivateMessageEvent.model_validate(data)

            with patch.object(command_probe, "make_event", side_effect=make_private):
                leader = await probe.command("退出队伍", number=975308)
            assert "你已离开队伍【队长离开队】" in leader and "当前队长为诊断对手" in leader, leader
            assert "冷却" not in leader, leader
            assert module.dungeon_team_application.snapshot("900100_975303").leader_id == "900002"
            assert module.dungeon_team_application.snapshot("900100_975303").members == ("900002",)
            member_solo = await probe.command("离开队伍", actor="900002", number=975309)
            assert "队伍已解散" in member_solo and "2026-11-20 15:00:00" in member_solo, member_solo
            assert module.dungeon_team_application.snapshot("900100_975303") is None
            committed = projection(probe)
        with patch.object(module, "runtime_clock", clock(1795177000)):
            repeated = await probe.command("退出队伍", actor="900002", number=975309)
            assert repeated == member_solo and projection(probe) == committed, repeated
        assert (balances(probe), backpack(probe)) == assets
        integrity(probe)
        probe.evidence.update(solo_without_cooldown=True, actual_leader_transferred=True,
                              member_solo_with_cooldown=True, alias=True, private=True,
                              session_rejection_stable=True, replay_original_deadline=True, integrity_check="ok")

    @case("dungeon-team-leave-reply-failure", "离开队伍", "成员离队完成后的回复失败",
          "隔离库业务先提交，再注入发送异常", "定位号/原日志保留，实际已离队且原消息可零写重放",
          group="dungeon-team-leave-parameters")
    async def leave_reply_failure(probe):
        from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_dungeon as module

        with patch.object(module, "runtime_clock", clock()):
            await probe.command("创建队伍", "创建队伍 离队回复队", number=975400)
            await probe.command("邀请组队", "邀请组队 诊断对手", number=975401)
            await probe.command("同意组队", actor="900002", number=975402)
            with patch.object(module, "handle_send", AsyncMock(side_effect=RuntimeError("isolated committed departure reply failure"))):
                failed = await probe.command("离开队伍", actor="900002", number=975403, allow_exception=True)
        assert "定位号：" in failed and "内部异常" in failed, failed
        assert any("isolated committed departure reply failure" in row["exception"] for row in probe.calls[-1]["logs"])
        assert module.dungeon_team_application.snapshot("900100_975400").members == ("900001",)
        assert module.dungeon_team_application.exit_operation_result("dungeon-team-leave:975403:900002", "leave", "900002").status == "duplicate"
        committed = projection(probe)
        replay = await probe.command("离开队伍", actor="900002", number=975403)
        assert "你已离开队伍【离队回复队】" in replay and projection(probe) == committed, replay
        probe.evidence.update(reply_failure_committed=True, replay_zero_writes=True)
