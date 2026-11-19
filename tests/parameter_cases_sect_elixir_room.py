"""Real command-path coverage for the sect elixir-room upgrade."""

from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from tests.parameter_cases_sect_body import seed_sect, sect_module


def register(case):
    def prepare(probe):
        module = sect_module()
        sid = seed_sect(probe)
        database = probe.context.paths.game_db
        costs = module.config["宗门丹房参数"]["elixir_room_level"]["1"]["level_up_cost"]
        probe.player(sect_position=0)
        probe.seed(
            database,
            "UPDATE sects SET elixir_room_level=?,sect_used_stone=?,sect_scale=? WHERE sect_id=?",
            (0, 10 * costs["stone"], 10 * costs["建设度"], sid),
        )
        return costs, sid, database

    async def transport_contract(reply):
        from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils import utils

        event = SimpleNamespace(get_user_id=lambda: "900001")
        for onebot, markdown, send_fails in ((True, True, False), (False, False, False), (False, True, False), (False, True, True)):
            bot = SimpleNamespace(adapter=SimpleNamespace(get_name=lambda: "OneBot V11" if onebot else "QQ"))
            config = SimpleNamespace(markdown_status=markdown, markdown_id="", markdown_id2="", markdown_button_status=False, at_sender=False, message_optimization=False, img=False)
            plain = AsyncMock()
            markdown_send = AsyncMock(side_effect=RuntimeError("isolated elixir-room delivery failure") if send_fails else None)
            with patch.object(utils, "XiuConfig", return_value=config), patch.object(utils, "get_chat_scene", return_value="group"), patch.object(utils, "is_group_event", return_value=True), patch.object(utils, "get_real_id_async", new=AsyncMock(return_value="")), patch.object(utils, "_send_event_message", plain), patch.object(utils, "_send_markdown_or_keyboard", markdown_send):
                await utils.handle_send(bot, event, reply, md_type="宗门", k1="领取丹药", v1="宗门丹药领取")
            if onebot or not markdown or send_fails:
                plain.assert_awaited_once()
                assert plain.call_args.kwargs["message"] == reply
            else:
                markdown_send.assert_awaited_once()

    @case(
        "sect-elixir-room-upgrade-receipt-recovery", "宗门丹房建设", "永久回执 IGNORE/解除后同消息恢复",
        "真实宗主、隔离丹房升级回执被 INSERT IGNORE", "具体回执拒写原因、等级/储备/建设度整笔回滚，解除后同消息成功",
        group="sect-elixir-room-upgrade-recovery", outcome="rejection",
    )
    async def receipt_recovery(probe):
        _, sid, database = prepare(probe)
        probe.seed(database, "CREATE TRIGGER ignore_elixir_room_receipt BEFORE INSERT ON sect_elixir_room_operations BEGIN SELECT RAISE(IGNORE); END")
        before = probe.rows(database, "SELECT elixir_room_level,sect_used_stone,sect_scale FROM sects WHERE sect_id=?", (sid,))
        failed = await probe.command("宗门丹房建设", "宗门丹房建设", number=989301)
        assert "丹房升级回执保存失败" in failed and "撤销" in failed, failed
        assert probe.rows(database, "SELECT elixir_room_level,sect_used_stone,sect_scale FROM sects WHERE sect_id=?", (sid,)) == before
        assert probe.rows(database, "SELECT COUNT(*) FROM sect_elixir_room_operations") == [(0,)]
        await transport_contract(failed)
        probe.seed(database, "DROP TRIGGER ignore_elixir_room_receipt")
        probe.clear_cooldowns()
        recovered = await probe.command("宗门丹房建设", "宗门丹房建设", number=989301)
        assert "成功升级宗门丹房" in recovered, recovered
        assert probe.rows(database, "SELECT elixir_room_level FROM sects WHERE sect_id=?", (sid,)) == [(1,)]
        assert probe.rows(database, "SELECT COUNT(*) FROM sect_elixir_room_operations") == [(1,)]
        probe.evidence.update(failed=failed, recovered=recovered, adapter_contracts=4)

    @case(
        "sect-elixir-room-upgrade-replay", "宗门丹房建设", "成功重放",
        "真实宗主与充足宗门储备/建设度", "等级与资产只变一次，重放提示已完成",
        group="sect-elixir-room-upgrade-parameters",
    )
    async def replay(probe):
        _, sid, database = prepare(probe)
        first = await probe.command("宗门丹房建设", "宗门丹房建设", number=989302)
        assert "成功升级宗门丹房" in first, first
        state = probe.rows(database, "SELECT elixir_room_level,sect_used_stone,sect_scale FROM sects WHERE sect_id=?", (sid,))
        probe.clear_cooldowns()
        again = await probe.command("宗门丹房建设", "宗门丹房建设", number=989302)
        assert "已经完成" in again and probe.rows(database, "SELECT elixir_room_level,sect_used_stone,sect_scale FROM sects WHERE sect_id=?", (sid,)) == state, again
        probe.evidence.update(first=first, replay=again, state=state)

    @case(
        "sect-elixir-room-upgrade-boundaries", "宗门丹房建设", "宗主权限/储备与建设度边界",
        "真实宗主、同宗成员和可控宗门资源", "非宗主、储备不足、建设度不足均明确拒绝且不写入",
        group="sect-elixir-room-upgrade-parameters", outcome="rejection",
    )
    async def boundaries(probe):
        costs, sid, database = prepare(probe)
        before = probe.rows(database, "SELECT elixir_room_level FROM sects WHERE sect_id=?", (sid,))
        probe.player("900002", sect_id=sid, sect_position=12)
        forbidden = await probe.command("宗门丹房建设", "宗门丹房建设", actor="900002", number=989303)
        assert "不是宗主" in forbidden, forbidden
        probe.clear_cooldowns()
        probe.seed(database, "UPDATE sects SET sect_used_stone=? WHERE sect_id=?", (costs["stone"] - 1, sid))
        stone = await probe.command("宗门丹房建设", "宗门丹房建设", number=989304)
        assert "不满足升级条件" in stone, stone
        probe.clear_cooldowns()
        probe.seed(database, "UPDATE sects SET sect_used_stone=?,sect_scale=? WHERE sect_id=?", (costs["stone"] * 10, costs["建设度"] - 1, sid))
        scale = await probe.command("宗门丹房建设", "宗门丹房建设", number=989305)
        assert "建设度不满足" in scale, scale
        assert probe.rows(database, "SELECT elixir_room_level FROM sects WHERE sect_id=?", (sid,)) == before
        probe.evidence.update(forbidden=forbidden, stone=stone, scale=scale, unchanged=True)
