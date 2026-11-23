"""Real join commands, finite SQLite failure injection, and boundary evidence."""

import json
from unittest.mock import AsyncMock, patch

from tests.parameter_cases_assets import balances, backpack
from tests.parameter_cases_team_invite import clock, projection


def register(case):
    @case("dungeon-team-join-replay", "同意组队", "原消息重放、跨群改参和私聊",
          "真实创建/邀请/接受和隔离 SQLite", "接受重放零写，换发起群不能误播报原成功",
          group="dungeon-team-join-parameters")
    async def join_replay(probe):
        from tests import command_probe
        from nonebot.adapters.onebot.v11 import PrivateMessageEvent
        from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_dungeon as module

        with patch.object(module, "runtime_clock", clock()):
            await probe.command("创建队伍", "创建队伍 接受核验队", number=973000)
            await probe.command("邀请组队", "邀请组队 诊断对手", number=973001)
            joined = await probe.command("同意组队", actor="900002", number=973002)
        assert "成功加入队伍【接受核验队】" in joined, joined
        committed = projection(probe)
        make_group = command_probe.make_event

        def another_group(text, actor, number):
            return make_group(text, actor, number).model_copy(update={"group_id": 900101})

        with patch.object(command_probe, "make_event", side_effect=another_group):
            changed = await probe.command("同意组队", actor="900002", number=973002)
        probe.evidence["changed_group_reply"] = changed
        assert "不同内容的请求" in changed and "成功加入" not in changed, changed
        with patch.object(module, "runtime_clock", clock(1795177000)):
            replay = await probe.command("接受组队", actor="900002", number=973002)
        assert "成功加入队伍【接受核验队】" in replay and projection(probe) == committed, replay

        def make_private(text, actor, number):
            data = make_group(text, actor, number).model_dump()
            data.pop("group_id")
            data.update(message_type="private", sub_type="friend")
            return PrivateMessageEvent.model_validate(data)

        with patch.object(command_probe, "make_event", side_effect=make_private):
            private = await probe.command("同意组队", actor="900002", number=973002)
        assert "成功加入队伍【接受核验队】" in private and projection(probe) == committed, private
        player = probe.context.paths.player_db
        legacy = json.loads(probe.rows(player, "SELECT result_json FROM dungeon_team_operations WHERE operation_id='dungeon-team-join:973002:900002'")[0][0])
        legacy.pop("group_id")
        probe.seed(player, "UPDATE dungeon_team_operations SET result_json=? WHERE operation_id='dungeon-team-join:973002:900002'", (json.dumps(legacy),))
        legacy_state = projection(probe)
        legacy_reply = await probe.command("同意组队", actor="900002", number=973002)
        assert "成功加入队伍【接受核验队】" in legacy_reply and projection(probe) == legacy_state, legacy_reply
        probe.evidence.update(replay_zero_writes=True, private_replay=True, changed_group_conflict=True, legacy_receipt_unchanged=True)

    @case("dungeon-team-join-write-recovery", "同意组队", "合法邀请、五步拒写/改写/ABORT与同消息恢复",
          "真实 matcher/DI/handler、固定时钟和隔离两库", "具体原因和subject、整事务回滚、故障解除后恢复且replay零写",
          group="dungeon-team-join-parameters")
    async def join_write_recovery(probe):
        from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork
        from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_dungeon as module

        player = probe.context.paths.player_db
        assets = balances(probe), backpack(probe)
        with patch.object(module, "runtime_clock", clock()):
            await probe.command("创建队伍", "创建队伍 加入故障队", number=973010)
            team_id = module.dungeon_team_application.team_id_for_user("900001")
            faults = (
                ("BEFORE UPDATE ON teams", "1", "SELECT RAISE(IGNORE)", "队伍入队状态保存失败", "team"),
                ("AFTER UPDATE ON teams", "1", "UPDATE teams SET leader='changed' WHERE user_id=NEW.user_id", "队伍入队状态与请求不一致", "team"),
                ("BEFORE INSERT ON dungeon_team_members", "NEW.member_id='900002'", "SELECT RAISE(IGNORE)", "入队成员索引保存失败", "team"),
                ("AFTER INSERT ON dungeon_team_members", "NEW.member_id='900002'", "UPDATE dungeon_team_members SET version=99 WHERE team_id=NEW.team_id", "入队成员索引与请求不一致", "team"),
                ("BEFORE INSERT ON team_cd", "NEW.user_id='900002'", "SELECT RAISE(IGNORE)", "入队首次加入标记保存失败", "player"),
                ("AFTER INSERT ON team_cd", "NEW.user_id='900002'", "UPDATE team_cd SET join_cd_until='changed' WHERE user_id=NEW.user_id", "入队首次加入记录与请求不一致", "player"),
                ("BEFORE UPDATE ON dungeon_team_invites", "1", "SELECT RAISE(IGNORE)", "入队邀请消费标记保存失败", "invite"),
                ("AFTER UPDATE ON dungeon_team_invites", "1", "UPDATE dungeon_team_invites SET bot_id='changed' WHERE invite_id=NEW.invite_id", "入队邀请记录与请求不一致", "invite"),
                ("BEFORE INSERT ON dungeon_team_operations", "NEW.action='join'", "SELECT RAISE(IGNORE)", "入队操作回执保存失败", "team"),
                ("AFTER INSERT ON dungeon_team_operations", "NEW.action='join'", "UPDATE dungeon_team_operations SET result_json='{}' WHERE operation_id=NEW.operation_id", "入队操作回执与请求不一致", "team"),
                ("BEFORE UPDATE ON dungeon_team_invites", "1", "SELECT RAISE(ABORT,'isolated command join abort')", None, "invite"),
            )
            for index, (timing, condition, body, phrase, subject) in enumerate(faults):
                invite_number, number = 973100 + index, 973200 + index
                await probe.command("邀请组队", "邀请组队 诊断对手", number=invite_number)
                invite_id = f"dungeon-team-invite:{invite_number}:900001:900002"
                before = projection(probe)
                probe.seed(player, f"CREATE TRIGGER fail_join {timing} WHEN {condition} BEGIN INSERT INTO team_cd(user_id,join_cd_until) VALUES('side-effect','touched'); {body}; END")
                try:
                    reply = await probe.command("同意组队", actor="900002", number=number, allow_exception=phrase is None)
                    probe.evidence[str(index)] = {"reply": reply, "logs": probe.calls[-1]["logs"]}
                    assert "成功加入" not in reply, reply
                    if phrase is None:
                        assert "定位号：" in reply and "内部异常" in reply, reply
                        assert any("isolated command join abort" in row["exception"] for row in probe.calls[-1]["logs"])
                    else:
                        label = {"team": "队伍【加入故障队】", "player": "入队玩家【诊断对手】", "invite": f"邀请【{invite_id}】"}[subject]
                        assert phrase in reply and label in reply, reply
                    assert projection(probe) == before
                finally:
                    probe.seed(player, "DROP TRIGGER fail_join")
                recovered = await probe.command("同意组队", actor="900002", number=number)
                assert "成功加入队伍【加入故障队】" in recovered, recovered
                committed = projection(probe)
                replay = await probe.command("同意组队", actor="900002", number=number)
                assert "成功加入队伍【加入故障队】" in replay and projection(probe) == committed, replay
                assert probe.rows(player, "SELECT members,version FROM teams WHERE user_id=?", (team_id,)) == [(json.dumps(["900001", "900002"]), 1)]
                assert probe.rows(player, "SELECT member_id,version FROM dungeon_team_members WHERE team_id=? ORDER BY member_id", (team_id,)) == [("900001", 1), ("900002", 1)]
                assert probe.rows(player, "SELECT had_first_join FROM team_cd WHERE user_id='900002'") == [(1,)]
                assert probe.rows(player, "SELECT status,resolved_operation_id FROM dungeon_team_invites WHERE invite_id=?", (invite_id,)) == [("joined", f"dungeon-team-join:{number}:900002")]
                assert probe.rows(player, "SELECT result_status FROM dungeon_team_operations WHERE operation_id=?", (f"dungeon-team-join:{number}:900002",)) == [("applied",)]
                probe.evidence[str(index)].update(rollback=True, recovered=True, replay_zero_writes=True)
                probe.seed(player, "DELETE FROM dungeon_team_invites")
                probe.seed(player, "DELETE FROM dungeon_team_operations WHERE action IN ('invite','join')")
                probe.seed(player, "DELETE FROM team_cd WHERE user_id='900002'")
                probe.seed(player, "DELETE FROM dungeon_team_members WHERE team_id=?", (team_id,))
                probe.seed(player, "INSERT INTO dungeon_team_members VALUES(?,'900001',0)", (team_id,))
                probe.seed(player, "UPDATE teams SET members=?,version=0 WHERE user_id=?", (json.dumps(["900001"]), team_id))
        assert (balances(probe), backpack(probe)) == assets
        for database in (player, probe.context.paths.game_db):
            with DatabaseUnitOfWork(database, read_only=True) as uow:
                assert uow.query_one("PRAGMA integrity_check") == {"integrity_check": "ok"}
        probe.evidence.update(assets_unchanged=True, integrity_check="ok")

    @case("dungeon-team-join-parameters", "同意组队", "三别名、私聊、群/目标/容量/会话与60秒边界",
          "真实持久邀请、隔离SQLite和可控业务时间", "有效邀请恰好一次入队，正常拒绝不改变业务状态",
          group="dungeon-team-join-parameters")
    async def join_parameters(probe):
        from nonebot.adapters.onebot.v11 import PrivateMessageEvent
        from tests import command_probe
        from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_dungeon as module

        player = probe.context.paths.player_db
        assets = balances(probe), backpack(probe)
        with patch.object(module, "runtime_clock", clock()):
            missing = await probe.command("同意组队", actor="900002", number=973300)
            assert "未找到这条组队邀请" in missing, missing
            await probe.command("创建队伍", "创建队伍 边界队伍", number=973301)
            await probe.command("邀请组队", "邀请组队 诊断对手", number=973302)
            team_id = module.dungeon_team_application.team_id_for_user("900001")
            before = projection(probe)

            async def rejected(command, number, phrase, **kwargs):
                reply = await probe.command(command, actor="900002", number=number, **kwargs)
                assert phrase in reply and "成功加入" not in reply, reply
                after = projection(probe)
                for table in before.keys() - {"dungeon_team_operations"}:
                    assert before[table] == after[table], (table, before[table], after[table])
                return reply

            make_group = command_probe.make_event

            def wrong_group(text, actor, number):
                return make_group(text, actor, number).model_copy(update={"group_id": 900101})

            with patch.object(command_probe, "make_event", side_effect=wrong_group):
                await rejected("同意组队", 973303, "邀请中的发起人或受邀人与本次请求不符")
            untouched = await probe.command("同意组队", actor="900003", number=973304)
            assert "未找到这条组队邀请" in untouched, untouched
            probe.seed(player, "UPDATE teams SET max_members=1 WHERE user_id=?", (team_id,))
            before = projection(probe)
            await rejected("接受组队", 973305, "队伍已满员")
            probe.seed(player, "UPDATE teams SET max_members=4 WHERE user_id=?", (team_id,))
            for index, uid in enumerate(("900001", "900002")):
                probe.seed(player, "INSERT INTO player_dungeon_status(user_id,dungeon_status) VALUES(?,'exploring') ON CONFLICT(user_id) DO UPDATE SET dungeon_status='exploring'", (uid,))
                before = projection(probe)
                await rejected("加入队伍", 973306 + index, "探索会话进行中")
                probe.seed(player, "UPDATE player_dungeon_status SET dungeon_status='not_started' WHERE user_id=?", (uid,))
            before = projection(probe)
        with patch.object(module, "runtime_clock", clock(1795176060)):
            await rejected("同意组队", 973308, "未找到这条组队邀请")
        with patch.object(module, "runtime_clock", clock(1795176059.999)):
            await rejected("同意组队", 973308, "未找到这条组队邀请")

            def make_private(text, actor, number):
                data = make_group(text, actor, number).model_dump()
                data.pop("group_id")
                data.update(message_type="private", sub_type="friend")
                return PrivateMessageEvent.model_validate(data)

            with patch.object(command_probe, "make_event", side_effect=make_private):
                private = await probe.command("组队同意", actor="900002", number=973309)
            assert "成功加入队伍【边界队伍】" in private, private
        assert module.dungeon_team_application.snapshot(team_id).members == ("900001", "900002")
        assert (balances(probe), backpack(probe)) == assets
        probe.evidence.update(aliases=True, private=True, other_target_rejected=True, wrong_group_rejected=True,
                              capacity_and_sessions=True, exact_deadline=True, rejection_replay_stable=True, assets_unchanged=True)

    @case("dungeon-team-join-reply-failure", "同意组队", "合法接受后的回复失败",
          "隔离库已提交，再注入发送异常", "回复错误有定位号，业务仍已提交且同消息重放零写",
          group="dungeon-team-join-parameters")
    async def join_reply_failure(probe):
        from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_dungeon as module

        with patch.object(module, "runtime_clock", clock()):
            await probe.command("创建队伍", "创建队伍 回复核验队", number=973400)
            await probe.command("邀请组队", "邀请组队 诊断对手", number=973401)
            with patch.object(module, "handle_send", AsyncMock(side_effect=RuntimeError("isolated committed join reply failure"))):
                failed = await probe.command("同意组队", actor="900002", number=973402, allow_exception=True)
        assert "定位号：" in failed and "内部异常" in failed, failed
        assert any("isolated committed join reply failure" in row["exception"] for row in probe.calls[-1]["logs"])
        assert module.dungeon_team_application.snapshot("900100_973400").members == ("900001", "900002")
        assert probe.rows(probe.context.paths.player_db, "SELECT result_status FROM dungeon_team_operations WHERE operation_id='dungeon-team-join:973402:900002'") == [("applied",)]
        committed = projection(probe)
        replay = await probe.command("同意组队", actor="900002", number=973402)
        assert "成功加入队伍【回复核验队】" in replay and projection(probe) == committed, replay
        probe.evidence.update(reply_failure_committed=True, replay_zero_writes=True)
