"""Bounded member removal through real matchers and isolated SQLite."""

import json
from unittest.mock import AsyncMock, patch

from tests.parameter_cases_assets import balances, backpack
from tests.parameter_cases_team_invite import clock
from tests.parameter_cases_team_leave import integrity, projection


def register(case):
    @case("dungeon-team-kick-write-recovery", "踢出队伍", "合法成员、五阶段拒写/改写与ABORT",
          "真实创建/邀请/加入及隔离双库，固定业务时钟", "具体原因/subject、完整回滚、原消息恢复/replay",
          group="dungeon-team-kick-parameters")
    async def kick_write_recovery(probe):
        from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_dungeon as module

        player = probe.context.paths.player_db
        assets = balances(probe), backpack(probe)
        faults = (
            ("BEFORE UPDATE ON teams", "1", "SELECT RAISE(IGNORE)", "移除队员状态保存失败", "team"),
            ("AFTER UPDATE ON teams", "1", "UPDATE teams SET description='changed' WHERE user_id=NEW.user_id", "移除队员状态与请求不一致", "team"),
            ("BEFORE DELETE ON dungeon_team_members", "1", "SELECT RAISE(IGNORE)", "移除队员索引保存失败", "team"),
            ("BEFORE INSERT ON dungeon_team_members", "1", "SELECT RAISE(IGNORE)", "移除队员索引保存失败", "team"),
            ("AFTER INSERT ON dungeon_team_members", "1", "UPDATE dungeon_team_members SET version=99 WHERE team_id=NEW.team_id", "移除队员索引与请求不一致", "team"),
            ("BEFORE INSERT ON team_cd", "NEW.user_id='900002'", "SELECT RAISE(IGNORE)", "被移除队员冷却保存失败", "player"),
            ("AFTER UPDATE ON team_cd", "NEW.user_id='900002'", "UPDATE team_cd SET had_first_join=0 WHERE user_id=NEW.user_id", "被移除队员冷却与原记录不一致", "player"),
            ("BEFORE UPDATE ON dungeon_team_invites", "OLD.status='pending'", "SELECT RAISE(IGNORE)", "移除队员关联邀请保存失败", "invite"),
            ("AFTER UPDATE ON dungeon_team_invites", "NEW.status='team_changed'", "UPDATE dungeon_team_invites SET bot_id='changed' WHERE invite_id=NEW.invite_id", "移除队员关联邀请与原记录不一致", "invite"),
            ("BEFORE INSERT ON dungeon_team_exit_operations", "1", "SELECT RAISE(IGNORE)", "移除队员操作回执保存失败", "team"),
            ("AFTER INSERT ON dungeon_team_exit_operations", "1", "UPDATE dungeon_team_exit_operations SET result_json='{}' WHERE operation_id=NEW.operation_id", "移除队员操作回执与请求不一致", "team"),
            ("AFTER INSERT ON dungeon_team_exit_operations", "1", "UPDATE team_cd SET join_cd_until='changed' WHERE user_id='900002'", "被移除队员冷却与原记录不一致", "player"),
            ("BEFORE UPDATE ON teams", "1", "SELECT RAISE(ABORT,'isolated command removal abort')", None, "team"),
            ("BEFORE DELETE ON dungeon_team_members", "1", "SELECT RAISE(ABORT,'isolated command removal abort')", None, "team"),
            ("BEFORE INSERT ON dungeon_team_members", "1", "SELECT RAISE(ABORT,'isolated command removal abort')", None, "team"),
            ("BEFORE INSERT ON team_cd", "NEW.user_id='900002'", "SELECT RAISE(ABORT,'isolated command removal abort')", None, "player"),
            ("BEFORE UPDATE ON dungeon_team_invites", "OLD.status='pending'", "SELECT RAISE(ABORT,'isolated command removal abort')", None, "invite"),
            ("BEFORE INSERT ON dungeon_team_exit_operations", "1", "SELECT RAISE(ABORT,'isolated command removal abort')", None, "team"),
        )
        with patch.object(module, "runtime_clock", clock()):
            await probe.command("创建队伍", "创建队伍 移除故障队", number=976100)
            team_id = module.dungeon_team_application.team_id_for_user("900001")
            for index, (timing, condition, body, phrase, subject) in enumerate(faults):
                invite_number, join_number, pending_number, number = 976110 + index, 976150 + index, 976190 + index, 976230 + index
                probe.seed(player, "UPDATE team_cd SET join_cd_until='' WHERE user_id='900002'")
                invited = await probe.command("邀请组队", "邀请组队 诊断对手", number=invite_number)
                assert "已向诊断对手发送组队邀请" in invited, invited
                joined = await probe.command("同意组队", actor="900002", number=join_number)
                assert "成功加入队伍" in joined, joined
                await probe.command("邀请组队", "邀请组队 诊断管理", number=pending_number)
                pending_id = f"dungeon-team-invite:{pending_number}:900001:900003"
                before = projection(probe)
                probe.seed(player, f"CREATE TRIGGER removal_fault {timing} WHEN {condition} BEGIN INSERT OR IGNORE INTO team_cd(user_id,join_cd_until) VALUES('side-effect','touched'); {body}; END")
                try:
                    failed = await probe.command("踢出队伍", "踢出队伍 诊断对手", number=number, allow_exception=phrase is None)
                    assert "已将成员" not in failed, failed
                    if phrase:
                        label = {"team": "队伍【移除故障队】", "player": "被移除队员", "invite": f"邀请【{pending_id}】"}[subject]
                        assert phrase in failed and label in failed, failed
                    else:
                        assert "定位号：" in failed and "内部异常" in failed, failed
                        assert any("isolated command removal abort" in row["exception"] for row in probe.calls[-1]["logs"])
                    assert projection(probe) == before
                    probe.evidence[str(index)] = {"reply": failed, "logs": probe.calls[-1]["logs"], "rollback": True}
                finally:
                    probe.seed(player, "DROP TRIGGER removal_fault")
                recovered = await probe.command("踢出队伍", "踢出队伍 诊断对手", number=number)
                assert "已将成员诊断对手踢出队伍" in recovered and "2026-11-20 15:00:00" in recovered, recovered
                committed = projection(probe)
                replay = await probe.command("踢出队伍", "移除队员 [CQ:at,qq=900002]", number=number)
                assert recovered == replay and projection(probe) == committed, replay
                assert probe.rows(player, "SELECT members,leader FROM teams WHERE user_id=?", (team_id,)) == [(json.dumps(["900001"]), "900001")]
                version = probe.rows(player, "SELECT version FROM teams WHERE user_id=?", (team_id,))[0][0]
                assert probe.rows(player, "SELECT member_id,version FROM dungeon_team_members WHERE team_id=?", (team_id,)) == [("900001", version)]
                assert probe.rows(player, "SELECT join_cd_until,had_first_join FROM team_cd WHERE user_id='900002'") == [("2026-11-20 15:00:00", 1)]
                assert probe.rows(player, "SELECT status,resolved_operation_id,bot_id FROM dungeon_team_invites WHERE invite_id=?", (pending_id,)) == [("team_changed", f"dungeon-team-kick:{number}:900001", "900000")]
                assert len(probe.rows(player, "SELECT result_json FROM dungeon_team_exit_operations WHERE operation_id=?", (f"dungeon-team-kick:{number}:900001",))) == 1
                probe.evidence[str(index)].update(recovered=True, replay_zero_writes=True, version=version)
        assert (balances(probe), backpack(probe)) == assets
        integrity(probe)
        probe.evidence.update(assets_unchanged=True, integrity_check="ok")

    @case("dungeon-team-kick-normal", "踢出队伍", "权限、目标、活动会话及实际冷却",
          "隔离真实成员和固定业务时钟", "保留正常拒绝，移除原创建者不虚报冷却，私聊别名可用",
          group="dungeon-team-kick-parameters")
    async def kick_normal(probe):
        from nonebot.adapters.onebot.v11 import PrivateMessageEvent
        from tests import command_probe
        from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_dungeon as module

        player = probe.context.paths.player_db
        assets = balances(probe), backpack(probe)
        with patch.object(module, "runtime_clock", clock()):
            missing = await probe.command("踢出队伍", "踢出队伍 诊断对手", number=976300)
            assert "你不在任何队伍中" in missing, missing
            await probe.command("创建队伍", "创建队伍 移除边界队", number=976301)
            await probe.command("邀请组队", "邀请组队 诊断对手", number=976302)
            await probe.command("同意组队", actor="900002", number=976303)
            for index, (actor, target, phrase) in enumerate((
                ("900001", "", "该成员不在你的队伍中"),
                ("900001", "不存在道号", "该成员不在你的队伍中"),
                ("900001", "诊断道友", "不能踢出自己"),
                ("900001", "诊断管理", "该成员不在你的队伍中"),
                ("900002", "诊断道友", "只有队长"),
                ("900003", "诊断对手", "你不在任何队伍中"),
            )):
                before = projection(probe)
                rejected = await probe.command("踢出队伍", "踢出队伍 " + target, actor=actor, number=976310 + index)
                assert phrase in rejected, rejected
                for table in before.keys() - {"dungeon_team_exit_operations"}:
                    assert projection(probe)[table] == before[table], table
            for index, actor in enumerate(("900001", "900002")):
                probe.seed(player, "INSERT INTO player_dungeon_status(user_id,dungeon_status) VALUES(?,'exploring') "
                           "ON CONFLICT(user_id) DO UPDATE SET dungeon_status='exploring'", (actor,))
                rejected = await probe.command("踢出队伍", "踢出队伍 诊断对手", number=976320 + index)
                assert "副本探索会话进行中" in rejected, rejected
                probe.seed(player, "UPDATE player_dungeon_status SET dungeon_status='not_started' WHERE user_id=?", (actor,))
            before = projection(probe)
            assert "副本探索会话进行中" in await probe.command("踢出队伍", "踢出队伍 诊断对手", number=976320)
            assert projection(probe) == before
            assert await probe.command("踢出队伍", "踢出队伍 诊断对手", number=976300) == missing
            assert module.dungeon_team_application.snapshot("900100_976301").members == ("900001", "900002")
            await probe.command("转移队长", "转移队长 诊断对手", number=976325)
            make_group = command_probe.make_event

            def make_private(text, actor, number):
                data = make_group(text, actor, number).model_dump()
                data.pop("group_id")
                data.update(message_type="private", sub_type="friend")
                return PrivateMessageEvent.model_validate(data)

            with patch.object(command_probe, "make_event", side_effect=make_private):
                removed = await probe.command("踢出队伍", "移除队员 [CQ:at,qq=900001]", actor="900002", number=976326)
            assert "已将成员诊断道友踢出队伍" in removed and "冷却" not in removed, removed
            assert probe.rows(player, "SELECT * FROM team_cd WHERE user_id='900001'") == []
            assert module.dungeon_team_application.snapshot("900100_976301").members == ("900002",)
        assert (balances(probe), backpack(probe)) == assets
        integrity(probe)
        probe.evidence.update(normal_rejections=True, rejection_replay=True, both_member_sessions=True,
                              alias_at_private=True, founding_leader_has_no_cooldown=True, integrity_check="ok")

    @case("dungeon-team-kick-replay", "踢出队伍", "合法目标等价重放、改目标冲突与晚时钟",
          "真实三人队伍和持久回执", "同目标/跨群稳定重放、改合法目标明确冲突，原期限不变",
          group="dungeon-team-kick-parameters")
    async def kick_replay(probe):
        from tests import command_probe
        from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_dungeon as module

        with patch.object(module, "runtime_clock", clock()):
            await probe.command("创建队伍", "创建队伍 移除重放队", number=976350)
            for name, actor, number in (("诊断对手", "900002", 976351), ("诊断管理", "900003", 976353)):
                await probe.command("邀请组队", "邀请组队 " + name, number=number)
                await probe.command("同意组队", actor=actor, number=number + 1)
            original = await probe.command("踢出队伍", "踢出队伍 诊断对手", number=976355)
        assert "已将成员诊断对手踢出队伍" in original and "2026-11-20 15:00:00" in original, original
        committed = projection(probe)
        with patch.object(module, "runtime_clock", clock(1795177000)):
            replay = await probe.command("踢出队伍", "移除队员 [CQ:at,qq=900002]", number=976355)
            assert replay == original and projection(probe) == committed, replay
            conflict = await probe.command("踢出队伍", "踢出队伍 诊断管理", number=976355)
            assert "不同内容的请求" in conflict and "已将成员" not in conflict, conflict
            make_group = command_probe.make_event

            def another_group(text, actor, number):
                return make_group(text, actor, number).model_copy(update={"group_id": 900101})

            with patch.object(command_probe, "make_event", side_effect=another_group):
                assert await probe.command("踢出队伍", "踢出队伍 诊断对手", number=976355) == original
        assert projection(probe) == committed
        assert module.dungeon_team_application.snapshot("900100_976350").members == ("900001", "900003")
        integrity(probe)
        probe.evidence.update(changed_target_conflict=conflict, original_deadline=True, cross_group_replay=True, zero_writes=True)

    @case("dungeon-team-kick-reply-failure", "踢出队伍", "移除提交后的回复异常",
          "隔离实际业务已提交，mock 发送异常", "定位号/原异常保留、实际已移除且原消息重放零写",
          group="dungeon-team-kick-parameters")
    async def kick_reply_failure(probe):
        from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_dungeon as module

        with patch.object(module, "runtime_clock", clock()):
            await probe.command("创建队伍", "创建队伍 移除回复队", number=976400)
            await probe.command("邀请组队", "邀请组队 诊断对手", number=976401)
            await probe.command("同意组队", actor="900002", number=976402)
            with patch.object(module, "handle_send", AsyncMock(side_effect=RuntimeError("isolated committed removal reply failure"))):
                failed = await probe.command("踢出队伍", "踢出队伍 诊断对手", number=976403, allow_exception=True)
        assert "定位号：" in failed and "内部异常" in failed, failed
        assert any("isolated committed removal reply failure" in row["exception"] for row in probe.calls[-1]["logs"])
        assert module.dungeon_team_application.snapshot("900100_976400").members == ("900001",)
        assert module.dungeon_team_application.exit_operation_result("dungeon-team-kick:976403:900001", "kick", "900001", "900002").status == "duplicate"
        committed = projection(probe)
        replay = await probe.command("踢出队伍", "踢出队伍 诊断对手", number=976403)
        assert "已将成员诊断对手踢出队伍" in replay and projection(probe) == committed, replay
        probe.evidence.update(reply_failure_committed=True, replay_zero_writes=True)
