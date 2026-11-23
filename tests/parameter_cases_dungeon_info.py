"""Read published dungeon information through real matchers and SQLite."""

import json
from unittest.mock import patch

from tests.parameter_cases_dungeon_exit import live_manager
from tests.parameter_cases_team_leave import integrity
from tests.parameter_cases_team_view import database_projection


def published(probe):
    player = probe.context.paths.player_db
    rows = probe.rows(player, "SELECT dungeon_id,dungeon_name,total_layers,dungeon_type,description,date,reset_operation_id "
                      "FROM dungeon_global_state WHERE user_id='0'")
    assert len(rows) == 1, rows
    dungeon_id, name, layers, kind, description, date, operation = rows[0]
    snapshot = json.loads(probe.rows(player, "SELECT dungeon_snapshot FROM dungeon_reset_operations WHERE operation_id=?", (operation,))[0][0])
    assert (snapshot["dungeon_id"], snapshot["dungeon_name"], snapshot["total_layers"],
            snapshot["dungeon_type"], snapshot["description"]) == (dungeon_id, name, layers, kind, description)
    return (f"名称：{name}", f"简介：{description}", f"副本类型：{kind}", f"总层数：{layers}层", f"副本日期：{date}")


def register(case):
    @case("dungeon-info-published-readonly", "副本信息", "已发布快照、别名/私聊/重复查询、探索与已退出玩家",
          "真实manager/应用、隔离双库及固定业务钟", "所有展示与数据库发布一致，玩家进度/资产/回执及完整双库零写",
          group="dungeon-info-parameters")
    async def published_readonly(probe):
        from nonebot.adapters.onebot.v11 import PrivateMessageEvent
        from tests import command_probe

        player = probe.context.paths.player_db
        with live_manager() as (module, manager):
            for user, phase in (("900001", "exploring"), ("900002", "exited")):
                manager.get_player_status(user)
                probe.seed(player, "UPDATE player_dungeon_status SET dungeon_status=?,current_layer=2 WHERE user_id=?", (phase, user))
            expected = published(probe)
            before = database_projection(probe)
            for index, (name, actor) in enumerate((("副本信息", "900001"), ("今日副本", "900002"))):
                reply = await probe.command(name, actor=actor, number=979700 + index)
                for phrase in ("【今日副本】", *expected):
                    assert phrase in reply, (phrase, reply)
                assert database_projection(probe) == before
                assert await probe.command(name, actor=actor, number=979700 + index) == reply
                assert database_projection(probe) == before
            make_group = command_probe.make_event

            def make_private(text, actor, number):
                data = make_group(text, actor, number).model_dump()
                data.pop("group_id")
                data.update(message_type="private", sub_type="friend")
                return PrivateMessageEvent.model_validate(data)

            with patch.object(command_probe, "make_event", side_effect=make_private):
                private = await probe.command("今日副本", number=979702)
            for phrase in expected:
                assert phrase in private, private
            assert database_projection(probe) == before
        integrity(probe)
        probe.evidence.update(published_fields=expected, aliases_and_private=True,
                              repeat_full_database_zero_writes=True, player_progress_unchanged=True, integrity_check="ok")

    @case("dungeon-info-publication-snapshot", "副本信息", "同日发布、同ID配置修改/移除、旧请求重新查询",
          "真实发布保存完整快照，查询期间只改变运行配置", "显示当前权威快照，配置漂移/旧消息不覆盖发布，查询双库零写",
          group="dungeon-info-parameters")
    async def publication_snapshot(probe):
        from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_dungeon.dungeon_manager import DungeonTemplate

        with live_manager() as (module, manager):
            manager.get_dungeon_progress()
            original = DungeonTemplate({"id": "info-trial", "name": "已发布试炼", "total_layers": 7,
                                        "type": "puzzle", "description": "已发布简介\n第二行描述"})
            changed = DungeonTemplate({"id": "info-trial", "name": "未发布配置", "total_layers": 99,
                                       "type": "challenge", "description": "未发布简介"})
            with patch.object(manager.random, "choice", return_value=original):
                manager.reset_dungeon("info-published-first", source="manual")
            expected = published(probe)
            before = database_projection(probe)
            for index, configured in enumerate(([changed], [])):
                with patch.object(manager, "dungeon_templates", configured), patch.object(manager, "current_dungeon", changed):
                    reply = await probe.command("副本信息", number=979750 + index)
                for phrase in expected:
                    assert phrase in reply.replace("\r", "\n"), reply
                assert "未发布配置" not in reply and "99层" not in reply, reply
                assert database_projection(probe) == before
            next_dungeon = DungeonTemplate({"id": "info-trial", "name": "同日新发布", "total_layers": 9,
                                            "type": "challenge", "description": "新代际权威简介"})
            with patch.object(manager.random, "choice", return_value=next_dungeon):
                manager.reset_dungeon("info-published-second", source="manual")
            expected = published(probe)
            before = database_projection(probe)
            reply = await probe.command("今日副本", number=979750)
            for phrase in expected:
                assert phrase in reply.replace("\r", "\n"), reply
            assert "已发布试炼" not in reply and database_projection(probe) == before, reply
        integrity(probe)
        probe.evidence.update(config_drift_and_removed_template=True, latest_publication=True,
                              old_message_reads_current_state=True, full_database_zero_writes=True, integrity_check="ok")
