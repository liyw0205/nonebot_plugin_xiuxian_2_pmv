"""Finite rejection-command paths through real matchers and isolated storage."""

from unittest.mock import AsyncMock, patch

from tests.parameter_cases_assets import balances, backpack
from tests.parameter_cases_team_invite import clock, projection


def register(case):
    @case("dungeon-team-reject-replay", "拒绝组队", "合法拒绝、原消息/私聊重放和跨群改参",
          "真实创建/邀请和隔离 SQLite", "原拒绝稳定重放，换群不能误报原成功",
          group="dungeon-team-reject-parameters")
    async def reject_replay(probe):
        from nonebot.adapters.onebot.v11 import PrivateMessageEvent
        from tests import command_probe
        from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_dungeon as module

        with patch.object(module, "runtime_clock", clock()):
            await probe.command("创建队伍", "创建队伍 拒绝重放队", number=974100)
            await probe.command("邀请组队", "邀请组队 诊断对手", number=974101)
            reply = await probe.command("拒绝组队", actor="900002", number=974102)
        assert "已拒绝组队邀请" in reply, reply
        committed = projection(probe)
        make_group = command_probe.make_event

        def another_group(text, actor, number):
            return make_group(text, actor, number).model_copy(update={"group_id": 900101})

        with patch.object(command_probe, "make_event", side_effect=another_group):
            changed = await probe.command("拒绝组队", actor="900002", number=974102)
        probe.evidence["changed_group_reply"] = changed
        assert "不同内容的请求" in changed and "已拒绝组队邀请" not in changed, changed
        with patch.object(module, "runtime_clock", clock(1795177000)):
            replay = await probe.command("拒绝入队", actor="900002", number=974102)
        assert "已拒绝组队邀请" in replay and projection(probe) == committed, replay

        def make_private(text, actor, number):
            data = make_group(text, actor, number).model_dump()
            data.pop("group_id")
            data.update(message_type="private", sub_type="friend")
            return PrivateMessageEvent.model_validate(data)

        with patch.object(command_probe, "make_event", side_effect=make_private):
            private = await probe.command("拒绝组队", actor="900002", number=974102)
        assert "已拒绝组队邀请" in private and projection(probe) == committed, private
        probe.evidence.update(replay_zero_writes=True, private_replay=True, changed_group_conflict=True)

    @case("dungeon-team-reject-write-recovery", "拒绝组队", "合法邀请、状态/回执拒写改写与ABORT",
          "真实 matcher/DI/handler、固定时钟和隔离两库", "具体原因/邀请subject、整事务回滚、同消息恢复/replay",
          group="dungeon-team-reject-parameters")
    async def reject_write_recovery(probe):
        from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork
        from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_dungeon as module

        player = probe.context.paths.player_db
        assets = balances(probe), backpack(probe)
        with patch.object(module, "runtime_clock", clock()):
            await probe.command("创建队伍", "创建队伍 拒绝故障队", number=974110)
            faults = (
                ("BEFORE UPDATE ON dungeon_team_invites", "1", "SELECT RAISE(IGNORE)", "组队拒绝状态保存失败"),
                ("AFTER UPDATE ON dungeon_team_invites", "1", "UPDATE dungeon_team_invites SET bot_id='changed' WHERE invite_id=NEW.invite_id", "组队拒绝状态与请求不一致"),
                ("BEFORE INSERT ON dungeon_team_operations", "NEW.action='reject'", "SELECT RAISE(IGNORE)", "组队拒绝回执保存失败"),
                ("AFTER INSERT ON dungeon_team_operations", "NEW.action='reject'", "UPDATE dungeon_team_operations SET result_json='{}' WHERE operation_id=NEW.operation_id", "组队拒绝回执与请求不一致"),
                ("AFTER INSERT ON dungeon_team_operations", "NEW.action='reject'", "UPDATE dungeon_team_invites SET status='pending'", "组队拒绝状态与请求不一致"),
                ("BEFORE UPDATE ON dungeon_team_invites", "1", "SELECT RAISE(ABORT,'isolated command refusal abort')", None),
                ("BEFORE INSERT ON dungeon_team_operations", "NEW.action='reject'", "SELECT RAISE(ABORT,'isolated command refusal abort')", None),
            )
            for index, (timing, condition, body, phrase) in enumerate(faults):
                invite_number, number = 974120 + index, 974130 + index
                await probe.command("邀请组队", "邀请组队 诊断对手", number=invite_number)
                invite_id = f"dungeon-team-invite:{invite_number}:900001:900002"
                before = projection(probe)
                probe.seed(player, f"CREATE TRIGGER fail_reject {timing} WHEN {condition} BEGIN INSERT INTO team_cd(user_id,join_cd_until) VALUES('side-effect','touched'); {body}; END")
                try:
                    failed = await probe.command("拒绝组队", actor="900002", number=number, allow_exception=phrase is None)
                    probe.evidence[str(index)] = {"reply": failed, "logs": probe.calls[-1]["logs"]}
                    assert "已拒绝组队邀请" not in failed, failed
                    if phrase is None:
                        assert "定位号：" in failed and "内部异常" in failed, failed
                        assert any("isolated command refusal abort" in row["exception"] for row in probe.calls[-1]["logs"])
                    else:
                        assert phrase in failed and f"邀请【{invite_id}】" in failed, failed
                    assert projection(probe) == before
                finally:
                    probe.seed(player, "DROP TRIGGER fail_reject")
                recovered = await probe.command("拒绝组队", actor="900002", number=number)
                assert "已拒绝组队邀请" in recovered, recovered
                committed = projection(probe)
                replay = await probe.command("拒绝入队", actor="900002", number=number)
                assert "已拒绝组队邀请" in replay and projection(probe) == committed, replay
                assert probe.rows(player, "SELECT status,resolved_operation_id,bot_id,expires_at FROM dungeon_team_invites WHERE invite_id=?", (invite_id,)) == [("rejected", f"dungeon-team-reject:{number}:900002", "900000", 1795176060)]
                assert probe.rows(player, "SELECT result_status FROM dungeon_team_operations WHERE operation_id=?", (f"dungeon-team-reject:{number}:900002",)) == [("applied",)]
                for table in ("teams", "dungeon_team_members", "team_cd"):
                    assert before[table] == committed[table], table
                probe.evidence[str(index)].update(rollback=True, recovered=True, replay_zero_writes=True)
                probe.seed(player, "DELETE FROM dungeon_team_invites")
                probe.seed(player, "DELETE FROM dungeon_team_operations WHERE action IN ('invite','reject')")
        assert (balances(probe), backpack(probe)) == assets
        for database in (player, probe.context.paths.game_db):
            with DatabaseUnitOfWork(database, read_only=True) as uow:
                assert uow.query_one("PRAGMA integrity_check") == {"integrity_check": "ok"}
        probe.evidence.update(assets_unchanged=True, integrity_check="ok")

    @case("dungeon-team-reject-parameters", "拒绝组队", "别名、私聊、群/他人及59.999/60秒边界",
          "真实持久邀请和可控时钟", "正常拒绝零改邀请、有效拒绝消费一次，拒绝结果稳定重放",
          group="dungeon-team-reject-parameters")
    async def reject_parameters(probe):
        from nonebot.adapters.onebot.v11 import PrivateMessageEvent
        from tests import command_probe
        from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_dungeon as module

        player = probe.context.paths.player_db
        assets = balances(probe), backpack(probe)
        with patch.object(module, "runtime_clock", clock()):
            missing = await probe.command("拒绝组队", actor="900002", number=974150)
            assert "未找到这条组队邀请" in missing, missing
            await probe.command("创建队伍", "创建队伍 拒绝边界队", number=974151)
            await probe.command("邀请组队", "邀请组队 诊断对手", number=974152)
            before = projection(probe)
            make_group = command_probe.make_event

            def wrong_group(text, actor, number):
                return make_group(text, actor, number).model_copy(update={"group_id": 900101})

            with patch.object(command_probe, "make_event", side_effect=wrong_group):
                wrong = await probe.command("拒绝组队", actor="900002", number=974153)
                repeated = await probe.command("拒绝组队", actor="900002", number=974153)
            assert "此邀请是在群900100发出的" in wrong and wrong == repeated, (wrong, repeated)
            other = await probe.command("拒绝入队", actor="900003", number=974154)
            assert "未找到这条组队邀请" in other, other
        with patch.object(module, "runtime_clock", clock(1795176060)):
            expired = await probe.command("拒绝组队", actor="900002", number=974155)
            assert "未找到这条组队邀请" in expired, expired
        with patch.object(module, "runtime_clock", clock(1795176059.999)):
            repeated = await probe.command("拒绝组队", actor="900002", number=974155)
            assert "未找到这条组队邀请" in repeated, repeated
            for table in before.keys() - {"dungeon_team_operations"}:
                assert projection(probe)[table] == before[table], table

            def make_private(text, actor, number):
                data = make_group(text, actor, number).model_dump()
                data.pop("group_id")
                data.update(message_type="private", sub_type="friend")
                return PrivateMessageEvent.model_validate(data)

            with patch.object(command_probe, "make_event", side_effect=make_private):
                private = await probe.command("拒绝入队", actor="900002", number=974156)
            assert "已拒绝组队邀请" in private, private
        assert probe.rows(player, "SELECT status FROM dungeon_team_invites") == [("rejected",)]
        for table in ("teams", "dungeon_team_members", "team_cd"):
            assert projection(probe)[table] == before[table], table
        assert (balances(probe), backpack(probe)) == assets
        probe.evidence.update(alias=True, private=True, wrong_group_replay=True, other_target_rejected=True,
                              exact_deadline=True, rejection_replay_stable=True, assets_unchanged=True)

    @case("dungeon-team-reject-reply-failure", "拒绝组队", "拒绝完成后的回复异常",
          "隔离库已提交，再注入发送失败", "回复错误可定位，拒绝仍已提交并能零写重放",
          group="dungeon-team-reject-parameters")
    async def reject_reply_failure(probe):
        from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_dungeon as module

        with patch.object(module, "runtime_clock", clock()):
            await probe.command("创建队伍", "创建队伍 拒绝回复队", number=974160)
            await probe.command("邀请组队", "邀请组队 诊断对手", number=974161)
            with patch.object(module, "handle_send", AsyncMock(side_effect=RuntimeError("isolated committed refusal reply failure"))):
                failed = await probe.command("拒绝组队", actor="900002", number=974162, allow_exception=True)
        assert "定位号：" in failed and "内部异常" in failed, failed
        assert any("isolated committed refusal reply failure" in row["exception"] for row in probe.calls[-1]["logs"])
        assert probe.rows(probe.context.paths.player_db, "SELECT status FROM dungeon_team_invites") == [("rejected",)]
        assert probe.rows(probe.context.paths.player_db, "SELECT result_status FROM dungeon_team_operations WHERE operation_id='dungeon-team-reject:974162:900002'") == [("applied",)]
        committed = projection(probe)
        replay = await probe.command("拒绝组队", actor="900002", number=974162)
        assert "已拒绝组队邀请" in replay and projection(probe) == committed, replay
        probe.evidence.update(reply_failure_committed=True, replay_zero_writes=True)
