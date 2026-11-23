"""Bounded team dissolution through real matchers and isolated SQLite."""

from unittest.mock import AsyncMock, patch

from tests.parameter_cases_assets import balances, backpack
from tests.parameter_cases_team_invite import clock
from tests.parameter_cases_team_leave import integrity, projection


def register(case):
    @case("dungeon-team-disband-write-recovery", "解散队伍", "合法队长、删除/冷却/邀请/回执故障与恢复",
          "真实创建/邀请/接受及隔离双库，固定业务时钟", "具体原因/subject、整笔回滚、原消息恢复/replay",
          group="dungeon-team-disband-parameters")
    async def disband_write_recovery(probe):
        from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_dungeon as module

        player = probe.context.paths.player_db
        assets = balances(probe), backpack(probe)
        faults = (
            ("BEFORE DELETE ON teams", "1", "SELECT RAISE(IGNORE)", "解散队伍状态保存失败", "team"),
            ("AFTER DELETE ON teams", "1", "INSERT INTO teams(user_id) VALUES(OLD.user_id)", "解散队伍状态与请求不一致", "team"),
            ("BEFORE DELETE ON dungeon_team_members", "1", "SELECT RAISE(IGNORE)", "解散队伍成员索引保存失败", "team"),
            ("AFTER DELETE ON dungeon_team_members", "1", "INSERT INTO dungeon_team_members VALUES(OLD.team_id,OLD.member_id,99)", "解散队伍成员索引与请求不一致", "team"),
            ("BEFORE INSERT ON team_cd", "NEW.user_id='900001'", "SELECT RAISE(IGNORE)", "解散队伍成员冷却保存失败", "leader"),
            ("AFTER UPDATE ON team_cd", "NEW.user_id='900001'", "UPDATE team_cd SET had_first_join=0 WHERE user_id=NEW.user_id", "解散队伍成员冷却与原记录不一致", "leader"),
            ("BEFORE INSERT ON team_cd", "NEW.user_id='900002'", "SELECT RAISE(IGNORE)", "解散队伍成员冷却保存失败", "member"),
            ("AFTER UPDATE ON team_cd", "NEW.user_id='900002'", "UPDATE team_cd SET had_first_join=0 WHERE user_id=NEW.user_id", "解散队伍成员冷却与原记录不一致", "member"),
            ("BEFORE UPDATE ON dungeon_team_invites", "OLD.status='pending'", "SELECT RAISE(IGNORE)", "解散队伍关联邀请保存失败", "invite"),
            ("AFTER UPDATE ON dungeon_team_invites", "NEW.status='team_changed'", "UPDATE dungeon_team_invites SET bot_id='changed' WHERE invite_id=NEW.invite_id", "解散队伍关联邀请与原记录不一致", "invite"),
            ("BEFORE INSERT ON dungeon_team_exit_operations", "1", "SELECT RAISE(IGNORE)", "解散队伍操作回执保存失败", "team"),
            ("AFTER INSERT ON dungeon_team_exit_operations", "1", "UPDATE dungeon_team_exit_operations SET result_json='{}' WHERE operation_id=NEW.operation_id", "解散队伍操作回执与请求不一致", "team"),
            ("AFTER INSERT ON dungeon_team_exit_operations", "1", "INSERT INTO teams(user_id) VALUES('$TEAM')", "解散队伍状态与请求不一致", "team"),
            ("AFTER INSERT ON dungeon_team_exit_operations", "1", "INSERT INTO dungeon_team_members VALUES('$TEAM','$LEADER',99)", "解散队伍成员索引与请求不一致", "team"),
            ("AFTER INSERT ON dungeon_team_exit_operations", "1", "UPDATE team_cd SET join_cd_until='changed' WHERE user_id='$MEMBER'", "解散队伍成员冷却与原记录不一致", "member"),
            ("AFTER INSERT ON dungeon_team_exit_operations", "1", "DELETE FROM dungeon_team_invites WHERE invite_id='$INVITE'", "解散队伍关联邀请与原记录不一致", "invite"),
            ("AFTER DELETE ON teams", "1", "UPDATE team_cd SET had_first_join=0 WHERE user_id='$MEMBER'", "解散队伍成员冷却与原记录不一致", "member"),
            ("AFTER UPDATE ON team_cd", "NEW.user_id='$LEADER'", "UPDATE team_cd SET had_first_join=0 WHERE user_id='$MEMBER'", "解散队伍成员冷却与原记录不一致", "member"),
        )
        with patch.object(module, "runtime_clock", clock()):
            for index, (timing, condition, body, phrase, subject) in enumerate(faults):
                create_number, invite_number, join_number, pending_number, number = (
                    977050 + index, 977110 + index, 977150 + index, 977190 + index, 977230 + index)
                await probe.command("创建队伍", "创建队伍 解散故障队", number=create_number)
                team_id = module.dungeon_team_application.team_id_for_user("900001")
                await probe.command("邀请组队", "邀请组队 诊断对手", number=invite_number)
                await probe.command("同意组队", actor="900002", number=join_number)
                await probe.command("邀请组队", "邀请组队 诊断管理", number=pending_number)
                pending_id = f"dungeon-team-invite:{pending_number}:900001:900003"
                replacements = {"$TEAM": team_id, "$LEADER": "900001", "$MEMBER": "900002", "$INVITE": pending_id}
                condition = condition.replace("'leader'", "'900001'").replace("'member'", "'900002'")
                body = body.replace("'leader'", "'900001'").replace("'member'", "'900002'")
                for placeholder, value in replacements.items():
                    condition = condition.replace(placeholder, value)
                    body = body.replace(placeholder, value)
                probe.seed(player, "INSERT INTO team_cd(user_id,join_cd_until,had_first_join) VALUES('900001','',1) "
                           "ON CONFLICT(user_id) DO UPDATE SET join_cd_until='',had_first_join=1")
                before = projection(probe)
                probe.seed(player, f"CREATE TRIGGER dissolution_fault {timing} WHEN {condition} BEGIN INSERT OR IGNORE INTO team_cd(user_id,join_cd_until) VALUES('side-effect','touched'); {body}; END")
                try:
                    failed = await probe.command("解散队伍", number=number)
                    assert "已解散" not in failed, failed
                    label = {"team": "队伍【解散故障队】", "leader": "队伍成员【900001】",
                             "member": "队伍成员【900002】", "invite": f"邀请【{pending_id}】"}[subject]
                    assert phrase in failed and label in failed, failed
                    assert projection(probe) == before
                    probe.evidence[str(index)] = {"reply": failed, "rollback": True}
                finally:
                    probe.seed(player, "DROP TRIGGER dissolution_fault")
                applied = await probe.command("解散队伍", number=number)
                assert "队伍【解散故障队】已解散" in applied and "2名符合冷却条件的队员" in applied, applied
                assert "2026-11-20 15:00:00" in applied, applied
                committed = projection(probe)
                assert probe.rows(player, "SELECT * FROM teams") == []
                assert probe.rows(player, "SELECT * FROM dungeon_team_members") == []
                assert probe.rows(player, "SELECT user_id,join_cd_until,had_first_join FROM team_cd WHERE user_id IN ('900001','900002') ORDER BY user_id") == [
                    ("900001", "2026-11-20 15:00:00", 1), ("900002", "2026-11-20 15:00:00", 1)]
                assert probe.rows(player, "SELECT status,resolved_operation_id,bot_id FROM dungeon_team_invites WHERE invite_id=?", (pending_id,)) == [("team_changed", f"dungeon-team-disband:{number}:900001", "900000")]
                replay = await probe.command("解散组队", number=number)
                assert replay == applied and projection(probe) == committed, replay
                probe.evidence[str(index)].update(recovered=True, replay_zero_writes=True)
        assert (balances(probe), backpack(probe)) == assets
        integrity(probe)
        probe.evidence.update(assets_unchanged=True, integrity_check="ok")

    @case("dungeon-team-disband-normal", "解散队伍", "队长权限、成员活动会话和拒绝重放",
          "隔离真实队伍及固定业务时钟", "仅合法队长可解散，任一成员探索中均稳定拒绝",
          group="dungeon-team-disband-parameters")
    async def disband_normal(probe):
        from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_dungeon as module

        player = probe.context.paths.player_db
        assets = balances(probe), backpack(probe)
        with patch.object(module, "runtime_clock", clock()):
            missing = await probe.command("解散队伍", number=977300)
            assert "你不在任何队伍中" in missing, missing
            await probe.command("创建队伍", "创建队伍 解散权限队", number=977301)
            await probe.command("邀请组队", "邀请组队 诊断对手", number=977302)
            await probe.command("同意组队", actor="900002", number=977303)
            before = projection(probe)
            denied = await probe.command("解散队伍", actor="900002", number=977304)
            assert "只有队长才能执行此操作" in denied, denied
            denied_committed = projection(probe)
            assert denied_committed["teams"] == before["teams"]
            assert await probe.command("解散队伍", actor="900002", number=977304) == denied
            assert projection(probe) == denied_committed
            for index, actor in enumerate(("900001", "900002")):
                probe.seed(player, "INSERT INTO player_dungeon_status(user_id,dungeon_status) VALUES(?,'exploring') "
                           "ON CONFLICT(user_id) DO UPDATE SET dungeon_status='exploring'", (actor,))
                snapshot = projection(probe)
                blocked = await probe.command("解散队伍", number=977305 + index)
                assert "副本探索会话进行中" in blocked, blocked
                assert projection(probe)["teams"] == snapshot["teams"]
                probe.seed(player, "UPDATE player_dungeon_status SET dungeon_status='not_started' WHERE user_id=?", (actor,))
            committed = projection(probe)
            blocked_replay = await probe.command("解散队伍", number=977305)
            assert "副本探索会话进行中" in blocked_replay and projection(probe) == committed
            assert module.dungeon_team_application.snapshot("900100_977301") is not None
        assert (balances(probe), backpack(probe)) == assets
        integrity(probe)
        probe.evidence.update(missing_and_nonleader_rejection=True, both_members_active_block=True,
                              rejection_replay=True, integrity_check="ok")

    @case("dungeon-team-disband-cooldown-replay", "解散队伍", "别名/私聊、单人与多人实际冷却、回执重放",
          "隔离真实队伍和冷却行，固定业务时钟", "不再对无冷却成员宣称冷却，按原回执稳定重放",
          group="dungeon-team-disband-parameters")
    async def disband_cooldown_replay(probe):
        from nonebot.adapters.onebot.v11 import PrivateMessageEvent
        from tests import command_probe
        from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_dungeon as module

        player = probe.context.paths.player_db
        assets = balances(probe), backpack(probe)
        with patch.object(module, "runtime_clock", clock()):
            await probe.command("创建队伍", "创建队伍 首次单人解散", number=977350)
            solo = await probe.command("解散组队", number=977351)
            assert "队伍【首次单人解散】已解散" in solo and "冷却" not in solo, solo
            assert probe.rows(player, "SELECT * FROM team_cd WHERE user_id='900001'") == []
            assert await probe.command("解散队伍", number=977351) == solo
            await probe.command("创建队伍", "创建队伍 解散私聊队", number=977352)
            await probe.command("邀请组队", "邀请组队 诊断对手", number=977353)
            await probe.command("同意组队", actor="900002", number=977354)
            probe.seed(player, "INSERT INTO team_cd(user_id,join_cd_until,had_first_join) VALUES('900001','',1)")
            make_group = command_probe.make_event

            def make_private(text, actor, number):
                data = make_group(text, actor, number).model_dump()
                data.pop("group_id")
                data.update(message_type="private", sub_type="friend")
                return PrivateMessageEvent.model_validate(data)

            with patch.object(command_probe, "make_event", side_effect=make_private):
                dissolved = await probe.command("解散组队", number=977355)
        assert "队伍【解散私聊队】已解散" in dissolved and "2名符合冷却条件的队员" in dissolved, dissolved
        assert "2026-11-20 15:00:00" in dissolved, dissolved
        committed = projection(probe)
        make_group = command_probe.make_event

        def make_private(text, actor, number):
            data = make_group(text, actor, number).model_dump()
            data.pop("group_id")
            data.update(message_type="private", sub_type="friend")
            return PrivateMessageEvent.model_validate(data)

        with patch.object(module, "runtime_clock", clock(1795177000)), \
                patch.object(command_probe, "make_event", side_effect=make_private):
            replay = await probe.command("解散队伍", number=977355)
            assert replay == dissolved and projection(probe) == committed
        assert (balances(probe), backpack(probe)) == assets
        integrity(probe)
        probe.evidence.update(solo_no_false_cooldown=True, alias=True, private=True,
                              multiple_actual_cooldowns=True, original_deadline=True, replay_zero_writes=True,
                              integrity_check="ok")

    @case("dungeon-team-disband-reply-failure", "解散队伍", "解散已提交后的回复异常",
          "隔离库业务先提交，再注入发送异常", "定位号/原日志保留，实际已解散且原消息可零写重放",
          group="dungeon-team-disband-parameters")
    async def disband_reply_failure(probe):
        from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_dungeon as module

        with patch.object(module, "runtime_clock", clock()):
            await probe.command("创建队伍", "创建队伍 解散回复队", number=977400)
            await probe.command("邀请组队", "邀请组队 诊断对手", number=977401)
            await probe.command("同意组队", actor="900002", number=977402)
            with patch.object(module, "handle_send", AsyncMock(side_effect=RuntimeError("isolated committed dissolution reply failure"))):
                failed = await probe.command("解散队伍", number=977403, allow_exception=True)
        assert "定位号：" in failed and "内部异常" in failed, failed
        assert any("isolated committed dissolution reply failure" in row["exception"] for row in probe.calls[-1]["logs"])
        assert module.dungeon_team_application.snapshot("900100_977400") is None
        assert module.dungeon_team_application.exit_operation_result("dungeon-team-disband:977403:900001", "disband", "900001").status == "duplicate"
        committed = projection(probe)
        replay = await probe.command("解散队伍", number=977403)
        assert "队伍【解散回复队】已解散" in replay and projection(probe) == committed, replay
        probe.evidence.update(reply_failure_committed=True, replay_zero_writes=True)
