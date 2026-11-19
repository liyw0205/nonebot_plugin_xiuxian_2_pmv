"""Real command-path coverage for the sect fairyland upgrade."""

from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from tests.parameter_cases_sect_body import seed_sect, sect_module


def register(case):
    def prepare(probe):
        module = sect_module()
        sid = seed_sect(probe)
        database = probe.context.paths.game_db
        config = module._get_sect_fairyland_config(1)
        probe.player(sect_position=0)
        probe.seed(
            database,
            "UPDATE sects SET sect_fairyland=?,sect_used_stone=?,sect_materials=? WHERE sect_id=?",
            (0, 10 * config["stone"], 10 * config["materials"], sid),
        )
        return config, sid, database

    async def transport_contract(reply):
        from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils import utils

        event = SimpleNamespace(get_user_id=lambda: "900001")
        for onebot, markdown, send_fails in (
            (True, True, False),
            (False, False, False),
            (False, True, False),
            (False, True, True),
        ):
            bot = SimpleNamespace(
                adapter=SimpleNamespace(get_name=lambda: "OneBot V11" if onebot else "QQ")
            )
            config = SimpleNamespace(
                markdown_status=markdown,
                markdown_id="",
                markdown_id2="",
                markdown_button_status=False,
                at_sender=False,
                message_optimization=False,
                img=False,
            )
            plain = AsyncMock()
            markdown_send = AsyncMock(
                side_effect=RuntimeError("isolated fairyland delivery failure") if send_fails else None
            )
            with patch.object(utils, "XiuConfig", return_value=config), \
                    patch.object(utils, "get_chat_scene", return_value="group"), \
                    patch.object(utils, "is_group_event", return_value=True), \
                    patch.object(utils, "get_real_id_async", new=AsyncMock(return_value="")), \
                    patch.object(utils, "_send_event_message", plain), \
                    patch.object(utils, "_send_markdown_or_keyboard", markdown_send):
                await utils.handle_send(
                    bot, event, reply, md_type="宗门", k1="炼体堂", v1="宗门炼体堂"
                )
            if onebot or not markdown or send_fails:
                plain.assert_awaited_once()
                assert plain.call_args.kwargs["message"] == reply
            else:
                markdown_send.assert_awaited_once()

    @case(
        "sect-fairyland-upgrade-receipt-recovery",
        "宗门炼体堂升级",
        "永久回执 IGNORE/解除后同消息恢复",
        "真实宗主、隔离宗门升级回执被 INSERT IGNORE",
        "具体回执拒写原因、等级/储备/资材整笔回滚，解除后同消息成功",
        group="sect-fairyland-upgrade-recovery",
        outcome="rejection",
    )
    async def receipt_recovery(probe):
        config, sid, database = prepare(probe)
        probe.seed(
            database,
            "CREATE TRIGGER ignore_fairyland_receipt BEFORE INSERT ON sect_fairyland_operations "
            "BEGIN SELECT RAISE(IGNORE); END",
        )
        before = probe.rows(
            database,
            "SELECT sect_fairyland,sect_used_stone,sect_materials FROM sects WHERE sect_id=?",
            (sid,),
        )
        failed = await probe.command("宗门炼体堂升级", "宗门炼体堂升级", number=989201)
        assert "炼体堂升级回执保存失败" in failed and "撤销" in failed, failed
        assert probe.rows(database, "SELECT sect_fairyland,sect_used_stone,sect_materials FROM sects WHERE sect_id=?", (sid,)) == before
        assert probe.rows(database, "SELECT COUNT(*) FROM sect_fairyland_operations") == [(0,)]
        await transport_contract(failed)
        probe.seed(database, "DROP TRIGGER ignore_fairyland_receipt")
        probe.clear_cooldowns()
        recovered = await probe.command("宗门炼体堂升级", "宗门炼体堂升级", number=989201)
        assert "升级成功" in recovered, recovered
        assert probe.rows(database, "SELECT sect_fairyland FROM sects WHERE sect_id=?", (sid,)) == [(1,)]
        assert probe.rows(database, "SELECT COUNT(*) FROM sect_fairyland_operations") == [(1,)]
        probe.evidence.update(failed=failed, recovered=recovered, adapter_contracts=4)

    @case(
        "sect-fairyland-upgrade-replay-conflict",
        "宗门炼体堂升级",
        "成功重放",
        "真实宗主与充足宗门储备/资材",
        "等级与资产只变一次，重放提示已完成",
        group="sect-fairyland-upgrade-parameters",
    )
    async def replay_conflict(probe):
        _, sid, database = prepare(probe)
        first = await probe.command("宗门炼体堂升级", "宗门炼体堂升级", number=989202)
        assert "升级成功" in first, first
        state = probe.rows(database, "SELECT sect_fairyland,sect_used_stone,sect_materials FROM sects WHERE sect_id=?", (sid,))
        probe.clear_cooldowns()
        replay = await probe.command("宗门炼体堂升级", "宗门炼体堂升级", number=989202)
        assert "已经完成" in replay and probe.rows(database, "SELECT sect_fairyland,sect_used_stone,sect_materials FROM sects WHERE sect_id=?", (sid,)) == state, replay
        probe.clear_cooldowns()
        probe.evidence.update(first=first, replay=replay, state=state)

    @case(
        "sect-fairyland-upgrade-boundaries",
        "宗门炼体堂升级",
        "宗主权限/储备与资材边界",
        "真实宗主、同宗成员和可控宗门资源",
        "非宗主、储备不足、资材不足均明确拒绝且不写入",
        group="sect-fairyland-upgrade-parameters",
        outcome="rejection",
    )
    async def boundaries(probe):
        config, sid, database = prepare(probe)
        before = probe.rows(database, "SELECT sect_fairyland,sect_used_stone,sect_materials FROM sects WHERE sect_id=?", (sid,))
        probe.player("900002", sect_id=sid, sect_position=12)
        forbidden = await probe.command("宗门炼体堂升级", "宗门炼体堂升级", actor="900002", number=989203)
        assert "只有宗主" in forbidden, forbidden
        probe.clear_cooldowns()
        probe.seed(database, "UPDATE sects SET sect_used_stone=? WHERE sect_id=?", (config["stone"] - 1, sid))
        stone = await probe.command("宗门炼体堂升级", "宗门炼体堂升级", number=989204)
        assert "储备不足" in stone, stone
        probe.clear_cooldowns()
        probe.seed(database, "UPDATE sects SET sect_used_stone=?,sect_materials=? WHERE sect_id=?", (config["stone"] * 10, config["materials"] - 1, sid))
        materials = await probe.command("宗门炼体堂升级", "宗门炼体堂升级", number=989205)
        assert "资材不足" in materials, materials
        assert probe.rows(database, "SELECT sect_fairyland FROM sects WHERE sect_id=?", (sid,)) == [(before[0][0],)]
        probe.evidence.update(forbidden=forbidden, stone=stone, materials=materials, unchanged=True)
