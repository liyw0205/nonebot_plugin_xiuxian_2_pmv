"""Read team membership and invitations through live matchers and isolated data."""

from unittest.mock import patch

from tests.parameter_cases_team_invite import clock
from tests.parameter_cases_team_leave import integrity


def database_projection(probe):
    from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork

    state = {}
    for database in (probe.context.paths.game_db, probe.context.paths.player_db):
        with DatabaseUnitOfWork(database, read_only=True) as uow:
            tables = uow.query_all("SELECT name,sql FROM sqlite_master WHERE type='table' ORDER BY name")
            state[database.name] = [
                (row, uow.query_all('SELECT * FROM "' + row["name"].replace('"', '""') + '"'))
                for row in tables
            ]
    return state


def register(case):
    @case("dungeon-team-view-members-readonly", "查看队伍", "无队伍、单人/多人、队长变更、别名和私聊",
          "真实创建/邀请/接受/转让，隔离双库", "显示实际队名/成员/队长，完整双库和回执零写",
          group="dungeon-team-view-parameters")
    async def view_members(probe):
        from nonebot.adapters.onebot.v11 import PrivateMessageEvent
        from tests import command_probe
        from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_dungeon as module

        before = database_projection(probe)
        missing = await probe.command("查看队伍", number=978000)
        assert "你不在任何队伍中" in missing and "创建队伍" in missing, missing
        assert database_projection(probe) == before
        with patch.object(module, "runtime_clock", clock()):
            created = await probe.command("创建队伍", "创建队伍 查询验收队", number=978001)
            assert "创建成功" in created, created
            team_id = module.dungeon_team_application.team_id_for_user("900001")
            before = database_projection(probe)
            solo = await probe.command("我的队伍", number=978002)
            for phrase in ("队伍名：查询验收队", f"队伍ID：{team_id}", "成员：1/4", "👑 诊断道友"):
                assert phrase in solo, (phrase, solo)
            assert database_projection(probe) == before
            invited = await probe.command("邀请组队", "邀请组队 诊断对手", number=978003)
            assert "发送组队邀请" in invited, invited
            joined = await probe.command("同意组队", actor="900002", number=978004)
            assert "成功加入队伍" in joined, joined
            before = database_projection(probe)
            for index, (name, actor) in enumerate((("查看队伍", "900001"), ("队伍信息", "900002"), ("我的队伍", "900001"))):
                reply = await probe.command(name, actor=actor, number=978005 + index)
                for phrase in ("【队伍信息】", "队伍名：查询验收队", "成员：2/4", "👑 诊断道友", "👤 诊断对手"):
                    assert phrase in reply, (phrase, reply)
                assert database_projection(probe) == before
                assert await probe.command(name, actor=actor, number=978005 + index) == reply
                assert database_projection(probe) == before
            transferred = await probe.command("转移队长", "转移队长 诊断对手", number=978008)
            assert "转移" in transferred, transferred
            before = database_projection(probe)
            make_group = command_probe.make_event

            def make_private(text, actor, number):
                data = make_group(text, actor, number).model_dump()
                data.pop("group_id")
                data.update(message_type="private", sub_type="friend")
                return PrivateMessageEvent.model_validate(data)

            with patch.object(command_probe, "make_event", side_effect=make_private):
                private = await probe.command("队伍信息", actor="900002", number=978009)
            assert "👑 诊断对手" in private and "👤 诊断道友" in private, private
            assert database_projection(probe) == before
        integrity(probe)
        probe.evidence.update(missing=True, solo_and_members=True, leader_change=True,
                              aliases_and_private=True, repeat_zero_writes=True, integrity_check="ok")

    @case("dungeon-team-view-invite-readonly", "查看队伍", "合法邀请、59.999/60秒、已拒绝和历史已消费邀请",
          "真实创建/邀请及隔离历史记录，统一可控墙钟/业务钟", "只显示本人的有效未消费邀请，查询不消费或改写任何数据",
          group="dungeon-team-view-parameters")
    async def view_invites(probe):
        from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_dungeon as module

        player = probe.context.paths.player_db
        with patch.object(module, "runtime_clock", clock()):
            await probe.command("创建队伍", "创建队伍 邀请查询队", number=978050)
            sent = await probe.command("邀请组队", "邀请组队 诊断对手", number=978051)
            assert "发送组队邀请" in sent, sent
        for timestamp in (1795176000, 1795176059.999, 1795176060):
            before = database_projection(probe)
            with patch.object(module.time, "time", return_value=timestamp):
                reply = await probe.command("查看队伍", actor="900002", number=978052)
                if timestamp < 1795176060:
                    for phrase in ("【待处理组队邀请】", "队伍：邀请查询队", "邀请人：诊断道友", "发起群：900100", "剩余：1分钟"):
                        assert phrase in reply, (phrase, reply)
                else:
                    assert "你不在任何队伍中" in reply and "待处理组队邀请" not in reply, reply
            assert database_projection(probe) == before
        with patch.object(module, "runtime_clock", clock()):
            rejected = await probe.command("拒绝组队", actor="900002", number=978053)
            assert "已拒绝组队邀请" in rejected, rejected
        before = database_projection(probe)
        with patch.object(module.time, "time", return_value=1795176001):
            hidden = await probe.command("我的队伍", actor="900002", number=978054)
        assert "待处理组队邀请" not in hidden and "你不在任何队伍中" in hidden, hidden
        assert database_projection(probe) == before

        # Legacy rows can retain pending after consumed_at was written; reads must not revive them.
        probe.seed(player, "UPDATE dungeon_team_invites SET status='pending' WHERE invitee_id='900002'")
        before = database_projection(probe)
        with patch.object(module.time, "time", return_value=1795176001):
            consumed = await probe.command("队伍信息", actor="900002", number=978055)
        probe.evidence.update(consumed_reply=consumed, stored_invite=probe.rows(player,
                              "SELECT status,consumed_at,expires_at FROM dungeon_team_invites WHERE invitee_id='900002'"))
        assert "待处理组队邀请" not in consumed and "你不在任何队伍中" in consumed, consumed
        assert database_projection(probe) == before
        integrity(probe)
        probe.evidence.update(valid_and_expiry_boundary=True, rejected_and_consumed_hidden=True,
                              readonly=True, integrity_check="ok")
