"""Bounded real invitation commands with isolated state and mock transport."""

from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from tests.parameter_cases_assets import balances, backpack


def projection(probe):
    return {table: probe.rows(probe.context.paths.player_db, f"SELECT * FROM {table} ORDER BY rowid")
            for table in ("teams", "dungeon_team_members", "dungeon_team_invites", "dungeon_team_operations", "team_cd")}


def clock(timestamp=1795176000):
    return SimpleNamespace(now=lambda: datetime.fromtimestamp(timestamp, timezone.utc))


def register(case):
    @case("dungeon-team-invite-replay", "邀请组队", "合法道号/@、同消息改参",
          "真实队长/目标和隔离两库", "原邀请稳定重放、换合法目标或群明确冲突，零写且不重复私聊",
          group="dungeon-team-invite-parameters")
    async def invite_replay(probe):
        from tests import command_probe
        from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_dungeon as module

        private = AsyncMock()
        with patch.object(module, "runtime_clock", clock()), patch.object(module.delivery_service, "send_to_user", private):
            await probe.command("创建队伍", "创建队伍 邀请核验队", number=971000)
            sent = await probe.command("邀请组队", "邀请组队 诊断对手", number=971001)
        assert "已向诊断对手发送组队邀请" in sent, sent
        private.assert_awaited_once()
        committed = projection(probe)
        original = module.dungeon_team_application.invite_by_id("dungeon-team-invite:971001:900001:900002")
        assert (original.created_at, original.expires_at, original.bot_id, original.source_message_id, original.notification_scene) == (
            1795176000, 1795176060, "900000", "971001", "group"), original

        with patch.object(module, "runtime_clock", clock(1795177000)), patch.object(module.delivery_service, "send_to_user", private):
            replay = await probe.command("邀请组队", "邀请组队 [CQ:at,qq=900002]", number=971001)
            assert "已向诊断对手发送组队邀请" in replay, replay
            conflict = await probe.command("邀请组队", "邀请组队 诊断管理", number=971001)
            probe.evidence["changed_target_reply"] = conflict
            assert "不同内容的请求" in conflict and "发送组队邀请" not in conflict, conflict
            make_group = command_probe.make_event

            def another_group(text, actor, number):
                event = make_group(text, actor, number)
                return event.model_copy(update={"group_id": 900101})

            with patch.object(command_probe, "make_event", side_effect=another_group):
                conflict_group = await probe.command("邀请组队", "邀请组队 诊断对手", number=971001)
            assert "不同内容的请求" in conflict_group, conflict_group
        private.assert_awaited_once()
        assert projection(probe) == committed
        probe.seed(probe.context.paths.player_db, "UPDATE teams SET leader='900003',version=version+1")
        changed_team = projection(probe)
        with patch.object(module.delivery_service, "send_to_user", private):
            original_reply = await probe.command("邀请组队", "邀请组队 诊断对手", number=971001)
        assert "已向诊断对手发送组队邀请" in original_reply and projection(probe) == changed_team, original_reply
        private.assert_awaited_once()
        probe.evidence.update(original_route=vars(original), replay_zero_writes=True, no_duplicate_notification=True)

    @case("dungeon-team-invite-write-recovery", "邀请组队", "合法目标、状态/过期链/回执故障与恢复",
          "隔离 SQLite、固定业务时间、真实注册 matcher/DI/handler", "精确原因、完整回滚、同消息恢复和永久回放",
          group="dungeon-team-invite-parameters")
    async def invite_write_recovery(probe):
        from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork
        from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_dungeon as module

        player = probe.context.paths.player_db
        assets = balances(probe), backpack(probe)
        with patch.object(module, "runtime_clock", clock()):
            await probe.command("创建队伍", "创建队伍 邀请故障队", number=971010)
            faults = (
                ("BEFORE UPDATE ON dungeon_team_invites", "OLD.invite_id='old-invite'", "SELECT RAISE(IGNORE)", "旧组队邀请过期标记保存失败"),
                ("AFTER UPDATE ON dungeon_team_invites", "OLD.invite_id='old-invite'", "UPDATE dungeon_team_invites SET group_id='changed' WHERE invite_id=NEW.invite_id", "旧组队邀请过期记录与预期不一致"),
                ("BEFORE INSERT ON dungeon_team_invites", "1", "SELECT RAISE(IGNORE)", "组队邀请记录保存失败"),
                ("AFTER INSERT ON dungeon_team_invites", "1", "UPDATE dungeon_team_invites SET bot_id='changed' WHERE invite_id=NEW.invite_id", "组队邀请记录与请求不一致"),
                ("BEFORE INSERT ON dungeon_team_operations", "NEW.action='invite'", "SELECT RAISE(IGNORE)", "组队邀请回执保存失败"),
                ("AFTER INSERT ON dungeon_team_operations", "NEW.action='invite'", "UPDATE dungeon_team_operations SET result_json='{}' WHERE operation_id=NEW.operation_id", "组队邀请回执与请求不一致"),
                ("AFTER INSERT ON dungeon_team_operations", "NEW.action='invite'", "UPDATE dungeon_team_invites SET status='joined' WHERE invite_id<>'old-invite'", "组队邀请记录与请求不一致"),
                ("AFTER INSERT ON dungeon_team_operations", "NEW.action='invite'", "UPDATE dungeon_team_invites SET status='pending' WHERE invite_id='old-invite'", "旧组队邀请过期记录与预期不一致"),
                ("BEFORE INSERT ON dungeon_team_invites", "1", "SELECT RAISE(ABORT,'isolated command invitation abort')", None),
            )
            for index, (timing, condition, body, phrase) in enumerate(faults):
                number = 971020 + index
                operation_id = f"dungeon-team-invite:{number}:900001"
                probe.seed(player, "INSERT INTO dungeon_team_invites(invite_id,team_id,inviter_id,invitee_id,group_id,created_at,expires_at) VALUES('old-invite','old-team','900003','900002','old-group',1795175900,1795176000)")
                before = projection(probe)
                probe.seed(player, f"CREATE TRIGGER fail_invite {timing} WHEN {condition} BEGIN INSERT INTO team_cd(user_id,join_cd_until) VALUES('side-effect','touched'); {body}; END")
                private = AsyncMock()
                try:
                    with patch.object(module.delivery_service, "send_to_user", private):
                        failed = await probe.command("邀请组队", "邀请组队 诊断对手", number=number, allow_exception=phrase is None)
                    probe.evidence[str(index)] = {"reply": failed, "logs": probe.calls[-1]["logs"]}
                    assert "发送组队邀请" not in failed, failed
                    if phrase is None:
                        assert "定位号：" in failed and "内部异常" in failed, failed
                        assert any("isolated command invitation abort" in row["exception"] for row in probe.calls[-1]["logs"])
                    else:
                        assert phrase in failed, failed
                        assert ("旧邀请【old-invite】" if index in (0, 1, 7) else "被邀请者【诊断对手】") in failed, failed
                    assert projection(probe) == before
                    private.assert_not_awaited()
                finally:
                    probe.seed(player, "DROP TRIGGER fail_invite")
                with patch.object(module.delivery_service, "send_to_user", private):
                    recovered = await probe.command("邀请组队", "邀请组队 诊断对手", number=number)
                    assert "已向诊断对手发送组队邀请" in recovered, recovered
                    applied = projection(probe)
                    replay = await probe.command("邀请组队", "邀请组队 诊断对手", number=number)
                    assert "已向诊断对手发送组队邀请" in replay and projection(probe) == applied, replay
                private.assert_awaited_once()
                assert probe.rows(player, "SELECT status FROM dungeon_team_invites WHERE invite_id='old-invite'") == [("expired",)]
                assert probe.rows(player, "SELECT created_at,expires_at,bot_id,source_message_id,notification_scene,status FROM dungeon_team_invites WHERE invite_id=?", (operation_id + ":900002",)) == [(
                    1795176000, 1795176060, "900000", str(number), "group", "pending")]
                assert probe.rows(player, "SELECT result_status FROM dungeon_team_operations WHERE operation_id=?", (operation_id,)) == [("applied",)]
                probe.evidence[str(index)].update(rollback=True, recovered=True, replay_zero_writes=True)
                probe.seed(player, "DELETE FROM dungeon_team_invites")
                probe.seed(player, "DELETE FROM dungeon_team_operations WHERE operation_id=?", (operation_id,))
        assert (balances(probe), backpack(probe)) == assets
        for database in (player, probe.context.paths.game_db):
            with DatabaseUnitOfWork(database, read_only=True) as uow:
                assert uow.query_one("PRAGMA integrity_check") == {"integrity_check": "ok"}
        probe.evidence.update(assets_unchanged=True, integrity_check="ok")

    @case("dungeon-team-invite-parameters", "邀请组队", "道号/@、别名、合法拒绝和精确60秒边界",
          "真实迁移、隔离两库与可控业务时钟", "正常规则不重构，发送失败不能抹去已提交邀请",
          group="dungeon-team-invite-parameters")
    async def invite_parameters(probe):
        from nonebot.adapters.onebot.v11 import PrivateMessageEvent
        from tests import command_probe
        from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_dungeon as module

        player = probe.context.paths.player_db
        assets = balances(probe), backpack(probe)
        private = AsyncMock()
        with patch.object(module, "runtime_clock", clock()), patch.object(module.delivery_service, "send_to_user", private):
            no_team = await probe.command("邀请组队", "邀请组队 诊断对手", number=971050)
            assert "还没有创建或加入" in no_team, no_team
            await probe.command("创建队伍", "创建队伍 参数邀请队", number=971051)
            empty = await probe.command("邀请组队", number=971052)
            unknown = await probe.command("邀请组队", "邀请组队 不存在道号", number=971053)
            missing = await probe.command("邀请组队", "邀请组队 [CQ:at,qq=900099]", number=971054)
            assert "未找到指定的用户" in empty and "未找到指定的用户" in unknown, (empty, unknown)
            assert "目标用户信息异常" in missing, missing
            own_team = await probe.command("邀请组队", "邀请组队 诊断道友", number=971055)
            assert "已有队伍" in own_team, own_team
            probe.seed(player, "UPDATE teams SET max_members=1")
            full = await probe.command("邀请组队", "邀请组队 诊断对手", number=971056)
            assert "队伍已满" in full, full
            probe.seed(player, "UPDATE teams SET max_members=4,leader='900003'")
            not_leader = await probe.command("邀请组队", "邀请组队 诊断对手", number=971057)
            assert "只有队长" in not_leader, not_leader
            probe.seed(player, "UPDATE teams SET leader='900001'")
            probe.seed(player, "INSERT INTO player_dungeon_status(user_id,dungeon_status) VALUES('900002','exploring') ON CONFLICT(user_id) DO UPDATE SET dungeon_status='exploring'")
            active = await probe.command("邀请组队", "邀请组队 诊断对手", number=971058)
            assert "探索会话进行中" in active, active
            probe.seed(player, "UPDATE player_dungeon_status SET dungeon_status='not_started' WHERE user_id='900002'")
            probe.seed(player, "INSERT INTO player_dungeon_status(user_id,dungeon_status) VALUES('900001','exploring') ON CONFLICT(user_id) DO UPDATE SET dungeon_status='exploring'")
            inviter_active = await probe.command("邀请组队", "邀请组队 诊断对手", number=971063)
            assert "探索会话进行中" in inviter_active, inviter_active
            probe.seed(player, "UPDATE player_dungeon_status SET dungeon_status='not_started' WHERE user_id='900001'")
            make_group = command_probe.make_event

            def make_private(text, actor, number):
                data = make_group(text, actor, number).model_dump()
                data.pop("group_id")
                data.update(message_type="private", sub_type="friend")
                return PrivateMessageEvent.model_validate(data)

            with patch.object(command_probe, "make_event", side_effect=make_private):
                wrong_scene = await probe.command("邀请组队", "邀请组队 诊断对手", number=971059)
            assert "只能在群聊中使用" in wrong_scene, wrong_scene
            assert probe.rows(player, "SELECT * FROM dungeon_team_invites") == []
            private.assert_not_awaited()
            sent = await probe.command("邀请入队", "邀请入队 [CQ:at,qq=900002]", number=971060)
            assert "已向诊断对手发送组队邀请" in sent, sent
            original = module.dungeon_team_application.invite_by_id("dungeon-team-invite:971060:900001:900002")
        assert (original.created_at, original.expires_at) == (1795176000, 1795176060), original
        with patch.object(module, "runtime_clock", clock(1795176059.999)):
            pending = await probe.command("组队邀请", "组队邀请 诊断对手", number=971061)
            assert "已有待处理" in pending, pending
        failed_delivery = AsyncMock(side_effect=RuntimeError("isolated private notification unavailable"))
        with patch.object(module, "runtime_clock", clock(1795176060)), patch.object(module.delivery_service, "send_to_user", failed_delivery):
            renewed = await probe.command("组队邀请", "组队邀请 诊断对手", number=971062, allow_exception=True)
            assert "已向诊断对手发送组队邀请" in renewed, renewed
            applied = projection(probe)
            await probe.command("组队邀请", "组队邀请 诊断对手", number=971062)
            assert projection(probe) == applied
        failed_delivery.assert_awaited_once()
        assert module.dungeon_team_application.invite_by_id(original.invite_id).status == "expired"
        replacement = module.dungeon_team_application.invite_by_id("dungeon-team-invite:971062:900001:900002")
        assert (replacement.status, replacement.created_at, replacement.expires_at) == ("pending", 1795176060, 1795176120), replacement
        assert probe.rows(player, "SELECT result_status FROM dungeon_team_operations WHERE operation_id='dungeon-team-invite:971062:900001'") == [("applied",)]
        assert (balances(probe), backpack(probe)) == assets
        probe.evidence.update(name=True, mention=True, aliases=True, boundaries=True, normal_rejections=True, notification_failure_committed=True, assets_unchanged=True)
