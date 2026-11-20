"""Real transfer commands and isolated team/index/receipt state."""

from unittest.mock import patch

from tests.parameter_cases_assets import balances, backpack
from tests.parameter_cases_team_invite import clock


def register(case):
    @case("dungeon-team-transfer-write-recovery", "转移队长", "成员索引拒写、同消息恢复及重放",
          "真实创建/邀请/接受链、隔离 SQLite 和故障触发器", "具体原因、整笔回滚、故障解除后恢复且重放零写",
          group="dungeon-team-transfer-parameters")
    async def transfer_write_recovery(probe):
        from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork
        from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_dungeon as module

        player = probe.context.paths.player_db
        assets = balances(probe), backpack(probe)
        with patch.object(module, "runtime_clock", clock()):
            await probe.command("创建队伍", "创建队伍 转移红测队", number=974500)
            await probe.command("邀请组队", "邀请组队 诊断对手", number=974501)
            await probe.command("同意组队", actor="900002", number=974502)
        team_id = module.dungeon_team_application.team_id_for_user("900001")
        before_team = probe.rows(player, "SELECT leader,version FROM teams WHERE user_id=?", (team_id,))
        before_members = probe.rows(player, "SELECT team_id,member_id,version FROM dungeon_team_members WHERE team_id=? ORDER BY member_id", (team_id,))
        probe.seed(player, f"CREATE TRIGGER ignore_transfer_index BEFORE UPDATE ON dungeon_team_members WHEN OLD.team_id='{team_id}' BEGIN SELECT RAISE(IGNORE); END")
        try:
            failed = await probe.command("转移队长", "转移队长 诊断对手", number=974503)
        finally:
            probe.seed(player, "DROP TRIGGER ignore_transfer_index")
        assert "队伍成员索引更新失败" in failed and "队伍【转移红测队】" in failed, failed
        assert "队长已成功转移" not in failed, failed
        assert probe.rows(player, "SELECT leader,version FROM teams WHERE user_id=?", (team_id,)) == before_team
        assert probe.rows(player, "SELECT team_id,member_id,version FROM dungeon_team_members WHERE team_id=? ORDER BY member_id", (team_id,)) == before_members
        operation_id = "dungeon-team-transfer:974503:900001"
        assert probe.rows(player, "SELECT 1 FROM dungeon_team_operations WHERE operation_id=?", (operation_id,)) == []
        recovered = await probe.command("转移队长", "转移队长 诊断对手", number=974503)
        assert "队长已成功转移给 诊断对手" in recovered, recovered
        after_team = probe.rows(player, "SELECT leader,version FROM teams WHERE user_id=?", (team_id,))
        after_members = probe.rows(player, "SELECT team_id,member_id,version FROM dungeon_team_members WHERE team_id=? ORDER BY member_id", (team_id,))
        receipt = probe.rows(player, "SELECT result_status,result_json FROM dungeon_team_operations WHERE operation_id=?", (operation_id,))
        committed = (after_team, after_members, receipt)
        replay = await probe.command("转移队长", "转移队长 诊断对手", number=974503)
        assert "队长已成功转移给 诊断对手" in replay, replay
        assert committed == (
            probe.rows(player, "SELECT leader,version FROM teams WHERE user_id=?", (team_id,)),
            probe.rows(player, "SELECT team_id,member_id,version FROM dungeon_team_members WHERE team_id=? ORDER BY member_id", (team_id,)),
            probe.rows(player, "SELECT result_status,result_json FROM dungeon_team_operations WHERE operation_id=?", (operation_id,)),
        )
        assert before_team == [("900001", 1)] and before_members == [(team_id, "900001", 1), (team_id, "900002", 1)]
        assert after_team == [("900002", 2)] and after_members == [(team_id, "900001", 2), (team_id, "900002", 2)]
        assert len(receipt) == 1 and receipt[0][0] == "applied"
        assert (balances(probe), backpack(probe)) == assets
        for database in (player, probe.context.paths.game_db):
            with DatabaseUnitOfWork(database, read_only=True) as uow:
                assert uow.query_one("PRAGMA integrity_check") == {"integrity_check": "ok"}
        probe.evidence.update(team_id=team_id, before_team=before_team, before_members=before_members,
                              failed_reply=failed, recovered_reply=recovered, after_team=after_team,
                              after_members=after_members, receipt=receipt, assets_unchanged=True,
                              integrity_check="ok", replay_zero_writes=True)

    @case("dungeon-team-transfer-faults", "转移队长", "状态/索引/回执拒写改写与ABORT",
          "合法两人队伍、隔离 SQLite 和故障触发器", "六类原因和原异常、完整回滚、同消息恢复及重放",
          group="dungeon-team-transfer-parameters")
    async def transfer_faults(probe):
        from tests.parameter_cases_team_invite import projection
        from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork
        from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_dungeon as module

        player = probe.context.paths.player_db
        assets = balances(probe), backpack(probe)
        with patch.object(module, "runtime_clock", clock()):
            await probe.command("创建队伍", "创建队伍 转移故障队", number=974700)
            await probe.command("邀请组队", "邀请组队 诊断对手", number=974701)
            await probe.command("同意组队", actor="900002", number=974702)
        faults = (
            ("BEFORE UPDATE ON teams", "1", "SELECT RAISE(IGNORE)", "队长转移状态保存失败"),
            ("AFTER UPDATE ON teams", "1", "UPDATE teams SET description='changed' WHERE user_id=NEW.user_id", "队长转移状态与请求不一致"),
            ("AFTER UPDATE ON dungeon_team_members", "1", "UPDATE dungeon_team_members SET version=99 WHERE team_id=NEW.team_id", "队伍成员索引与队长状态不一致"),
            ("BEFORE INSERT ON dungeon_team_operations", "NEW.action='transfer'", "SELECT RAISE(IGNORE)", "队长转移回执保存失败"),
            ("AFTER INSERT ON dungeon_team_operations", "NEW.action='transfer'", "UPDATE dungeon_team_operations SET result_json='{}' WHERE operation_id=NEW.operation_id", "队长转移回执与请求不一致"),
            ("AFTER INSERT ON dungeon_team_operations", "NEW.action='transfer'", "UPDATE teams SET leader='changed'", "队长转移状态与请求不一致"),
            ("AFTER INSERT ON dungeon_team_operations", "NEW.action='transfer'", "DELETE FROM dungeon_team_members", "队伍成员索引与队长状态不一致"),
            ("BEFORE UPDATE ON teams", "1", "SELECT RAISE(ABORT,'isolated command transfer abort')", None),
            ("BEFORE UPDATE ON dungeon_team_members", "1", "SELECT RAISE(ABORT,'isolated command transfer abort')", None),
            ("BEFORE INSERT ON dungeon_team_operations", "NEW.action='transfer'", "SELECT RAISE(ABORT,'isolated command transfer abort')", None),
        )
        for index, (timing, condition, body, phrase) in enumerate(faults):
            number = 974710 + index
            before = projection(probe)
            probe.seed(player, f"CREATE TRIGGER transfer_fault {timing} WHEN {condition} BEGIN INSERT OR IGNORE INTO team_cd(user_id,join_cd_until) VALUES('side-effect','touched'); {body}; END")
            try:
                failed = await probe.command("转移队长", "转移队长 诊断对手", number=number, allow_exception=phrase is None)
                assert "队长已成功转移" not in failed, failed
                if phrase:
                    assert phrase in failed and "队伍【转移故障队】" in failed, failed
                else:
                    assert "定位号：" in failed and "内部异常" in failed, failed
                    assert any("isolated command transfer abort" in row["exception"] for row in probe.calls[-1]["logs"])
                assert projection(probe) == before
                probe.evidence[str(index)] = {"failed_reply": failed, "logs": probe.calls[-1]["logs"], "rollback": True}
            finally:
                probe.seed(player, "DROP TRIGGER transfer_fault")
            recovered = await probe.command("转移队长", "转移队长 诊断对手", number=number)
            assert "队长已成功转移给 诊断对手" in recovered, recovered
            committed = projection(probe)
            replay = await probe.command("转移队长", "转移队长 诊断对手", number=number)
            assert "队长已成功转移给 诊断对手" in replay and projection(probe) == committed, replay
            assert probe.rows(player, "SELECT leader FROM teams") == [("900002",)]
            versions = probe.rows(player, "SELECT DISTINCT version FROM dungeon_team_members")
            assert len(versions) == 1
            assert probe.rows(player, "SELECT version FROM teams") == versions
            back = await probe.command("转移队长", "转移队长 诊断道友", actor="900002", number=974730 + index)
            assert "队长已成功转移给 诊断道友" in back, back
            probe.evidence[str(index)].update(recovered=True, replay_zero_writes=True)
        assert (balances(probe), backpack(probe)) == assets
        for database in (player, probe.context.paths.game_db):
            with DatabaseUnitOfWork(database, read_only=True) as uow:
                assert uow.query_one("PRAGMA integrity_check") == {"integrity_check": "ok"}
        probe.evidence.update(assets_unchanged=True, integrity_check="ok")

    @case("dungeon-team-transfer-parameters", "转移队长", "别名/艾特、权限、成员与同消息冲突",
          "真实双人队伍、隔离 SQLite 和玩家活动状态", "正常拒绝不变更队长，合法转让一次且同目标重放稳定",
          group="dungeon-team-transfer-parameters")
    async def transfer_parameters(probe):
        from nonebot.adapters.onebot.v11 import PrivateMessageEvent
        from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork
        from tests import command_probe
        from tests.parameter_cases_team_invite import projection
        from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_dungeon as module

        player = probe.context.paths.player_db
        assets = balances(probe), backpack(probe)
        with patch.object(module, "runtime_clock", clock()):
            await probe.command("创建队伍", "创建队伍 转移参数队", number=974510)
            await probe.command("邀请组队", "邀请组队 诊断对手", number=974511)
            await probe.command("同意组队", actor="900002", number=974512)
            team_id = module.dungeon_team_application.team_id_for_user("900001")
            before = projection(probe)

            self_target = await probe.command("转移队长", "转移队长 诊断道友", number=974513)
            assert "已经是队长了" in self_target, self_target
            outsider = await probe.command("转移队长", "转移队长 诊断管理", number=974514)
            assert "只能将队长转移给当前队伍内的成员" in outsider, outsider
            empty = await probe.command("转移队长", "转移队长", number=974518)
            unknown = await probe.command("转移队长", "转移队长 不存在的道号", number=974519)
            assert "只能将队长转移给当前队伍内的成员" in empty, empty
            assert "只能将队长转移给当前队伍内的成员" in unknown, unknown
            not_leader = await probe.command("转移队长", "转移队长 诊断道友", actor="900002", number=974515)
            assert "只有队长才能转移队长职位" in not_leader, not_leader

            probe.seed(player, "INSERT INTO player_dungeon_status(user_id,dungeon_status) VALUES('900001','exploring') "
                       "ON CONFLICT(user_id) DO UPDATE SET dungeon_status='exploring'")
            active = await probe.command("队长转让", "队长转让 [CQ:at,qq=900002]", number=974516)
            assert "副本探索会话进行中" in active, active
            probe.seed(player, "UPDATE player_dungeon_status SET dungeon_status='not_started' WHERE user_id='900001'")
            for table in ("teams", "dungeon_team_members", "team_cd"):
                assert projection(probe)[table] == before[table], table

            sent = await probe.command("队长转让", "队长转让 [CQ:at,qq=900002]", number=974517)
            assert "队长已成功转移给 诊断对手" in sent, sent
            committed = projection(probe)
            same_target = await probe.command("转让队长", "转让队长 诊断对手", number=974517)
            assert "队长已成功转移给 诊断对手" in same_target and projection(probe) == committed, same_target
            changed_target = await probe.command("转移队长", "转移队长 诊断管理", number=974517)
            assert "该操作号已用于不同内容的请求" in changed_target
            assert "队长已成功转移" not in changed_target and projection(probe) == committed, changed_target

            make_group = command_probe.make_event

            def make_private(text, actor, number):
                data = make_group(text, actor, number).model_dump()
                data.pop("group_id")
                data.update(message_type="private", sub_type="friend")
                return PrivateMessageEvent.model_validate(data)

            with patch.object(command_probe, "make_event", side_effect=make_private):
                private_replay = await probe.command("转移队长", "转移队长 诊断对手", number=974517)
            assert "队长已成功转移给 诊断对手" in private_replay and projection(probe) == committed, private_replay

            def another_group(text, actor, number):
                return make_group(text, actor, number).model_copy(update={"group_id": 900101})

            with patch.object(command_probe, "make_event", side_effect=another_group):
                cross_group = await probe.command("转移队长", "转移队长 诊断对手", number=974517)
            assert "队长已成功转移给 诊断对手" in cross_group and projection(probe) == committed, cross_group
        assert probe.rows(player, "SELECT leader,version FROM teams WHERE user_id=?", (team_id,)) == [("900002", 2)]
        assert (balances(probe), backpack(probe)) == assets
        for database in (player, probe.context.paths.game_db):
            with DatabaseUnitOfWork(database, read_only=True) as uow:
                assert uow.query_one("PRAGMA integrity_check") == {"integrity_check": "ok"}
        probe.evidence.update(alias=True, mention_target=True, normal_rejections=True,
                              active_session=True, same_target_replay=True, changed_target_conflict=True,
                              private_replay=True, assets_unchanged=True, integrity_check="ok")

    @case("dungeon-team-transfer-reply-failure", "转移队长", "转让成功后的回复失败",
          "隔离库已提交，再注入发送异常", "回复失败有定位号，队长/成员/永久回执已提交且可重放",
          group="dungeon-team-transfer-parameters")
    async def transfer_reply_failure(probe):
        from unittest.mock import AsyncMock
        from tests.parameter_cases_team_invite import projection
        from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_dungeon as module

        with patch.object(module, "runtime_clock", clock()):
            await probe.command("创建队伍", "创建队伍 转移回复队", number=974520)
            await probe.command("邀请组队", "邀请组队 诊断对手", number=974521)
            await probe.command("同意组队", actor="900002", number=974522)
            with patch.object(module, "handle_send", AsyncMock(side_effect=RuntimeError("isolated committed transfer reply failure"))):
                failed = await probe.command("转移队长", "转移队长 诊断对手", number=974523, allow_exception=True)
        assert "定位号：" in failed and "内部异常" in failed, failed
        assert any("isolated committed transfer reply failure" in row["exception"] for row in probe.calls[-1]["logs"])
        assert module.dungeon_team_application.snapshot("900100_974520").leader_id == "900002"
        assert probe.rows(probe.context.paths.player_db, "SELECT result_status FROM dungeon_team_operations WHERE operation_id='dungeon-team-transfer:974523:900001'") == [("applied",)]
        committed = projection(probe)
        replay = await probe.command("转移队长", "转移队长 诊断对手", number=974523)
        assert "队长已成功转移给 诊断对手" in replay and projection(probe) == committed, replay
        probe.evidence.update(reply_failure_committed=True, replay_zero_writes=True)
