"""Real command-path coverage for the three sect practice upgrades."""
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from tests.parameter_cases_sect_body import seed_sect, sect_module


def register(case):
    def prepare(probe):
        module = sect_module()
        sid = seed_sect(probe)
        database = probe.context.paths.game_db
        probe.player(
            stone=10**12,
            sect_contribution=10**12,
            sect_position=0,
            atkpractice=0,
            hppractice=0,
            mppractice=0,
        )
        probe.seed(
            database,
            "UPDATE sects SET sect_scale=?,sect_used_stone=?,sect_materials=? WHERE sect_id=?",
            (10**12, 10**12, 10**12, sid),
        )
        return module, sid, database

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
                side_effect=RuntimeError("isolated practice delivery failure") if send_fails else None
            )
            with patch.object(utils, "XiuConfig", return_value=config), \
                    patch.object(utils, "get_chat_scene", return_value="group"), \
                    patch.object(utils, "is_group_event", return_value=True), \
                    patch.object(utils, "get_real_id_async", new=AsyncMock(return_value="")), \
                    patch.object(utils, "_send_event_message", plain), \
                    patch.object(utils, "_send_markdown_or_keyboard", markdown_send):
                await utils.handle_send(
                    bot, event, reply, md_type="宗门", k1="升级", v1="升级攻击修炼"
                )
            if onebot or not markdown or send_fails:
                plain.assert_awaited_once()
                assert plain.call_args.kwargs["message"] == reply
            else:
                markdown_send.assert_awaited_once()
                assert "宗门" in str(markdown_send.call_args.args[2])
            if onebot:
                markdown_send.assert_not_awaited()

    @case(
        "sect-practice-receipt-ignore",
        "升级攻击修炼",
        "合法数量，永久修炼回执 INSERT 被隔离 trigger 忽略",
        "真实宗门成员、充足玩家灵石/贡献/宗门资材",
        "具体回执拒写原因，玩家灵石/等级/宗门资材/副作用整笔回滚并可恢复",
        group="sect-practice-handoff",
        outcome="rejection",
    )
    async def receipt_ignore(probe):
        _, sid, database = prepare(probe)
        before = probe.rows(
            database,
            "SELECT stone,atkpractice FROM user_xiuxian WHERE user_id='900001'",
        )
        sect_before = probe.rows(
            database,
            "SELECT sect_materials FROM sects WHERE sect_id=?",
            (sid,),
        )
        probe.seed(database, "CREATE TABLE isolated_effect(n INTEGER NOT NULL)")
        probe.seed(database, "INSERT INTO isolated_effect VALUES(0)")
        probe.seed(
            database,
            "CREATE TRIGGER practice_effect AFTER UPDATE OF atkpractice ON user_xiuxian "
            "BEGIN UPDATE isolated_effect SET n=n+1; END",
        )
        probe.seed(
            database,
            "CREATE TRIGGER ignore_practice_receipt BEFORE INSERT ON sect_practice_operations "
            "BEGIN SELECT RAISE(IGNORE); END",
        )
        failed = await probe.command("升级攻击修炼", "升级攻击修炼 1", number=989101)
        assert "回执未保存" in failed and "撤销" in failed, failed
        assert probe.rows(database, "SELECT stone,atkpractice FROM user_xiuxian WHERE user_id='900001'") == before
        assert probe.rows(database, "SELECT sect_materials FROM sects WHERE sect_id=?", (sid,)) == sect_before
        assert probe.rows(database, "SELECT COUNT(*) FROM sect_practice_operations") == [(0,)]
        assert probe.rows(database, "SELECT n FROM isolated_effect") == [(0,)]
        probe.seed(database, "DROP TRIGGER ignore_practice_receipt")
        probe.clear_cooldowns()
        recovered = await probe.command("升级攻击修炼", "升级攻击修炼 1", number=989101)
        assert "升级成功" in recovered, recovered
        assert probe.rows(database, "SELECT stone,atkpractice FROM user_xiuxian WHERE user_id='900001'")[0][1] == 1
        assert probe.rows(database, "SELECT COUNT(*) FROM sect_practice_operations") == [(1,)]
        probe.evidence.update(failed=failed, recovered=recovered, rollback=True, receipt_count=1)

    @case(
        "sect-practice-late-player-rewrite",
        "升级攻击修炼",
        "永久回执触发器改写玩家资产",
        "真实宗门成员、充足玩家灵石/贡献/宗门资材，回执 INSERT 后改写玩家灵石",
        "具体玩家写拒绝原因，等级/灵石/宗门资材/回执整笔回滚",
        group="sect-practice-handoff",
        outcome="rejection",
    )
    async def late_player_rewrite(probe):
        _, sid, database = prepare(probe)
        before = probe.rows(
            database,
            "SELECT stone,atkpractice FROM user_xiuxian WHERE user_id='900001'",
        )
        sect_before = probe.rows(
            database,
            "SELECT sect_materials FROM sects WHERE sect_id=?",
            (sid,),
        )
        probe.seed(
            database,
            "CREATE TRIGGER late_practice_player_rewrite AFTER INSERT ON sect_practice_operations "
            "BEGIN UPDATE user_xiuxian SET stone=stone+777 WHERE user_id='900001'; END",
        )
        failed = await probe.command("升级攻击修炼", "升级攻击修炼 1", number=989102)
        assert "灵石与角色修炼等级保存失败" in failed and "回滚" in failed, failed
        assert probe.rows(database, "SELECT stone,atkpractice FROM user_xiuxian WHERE user_id='900001'") == before
        assert probe.rows(database, "SELECT sect_materials FROM sects WHERE sect_id=?", (sid,)) == sect_before
        assert probe.rows(database, "SELECT COUNT(*) FROM sect_practice_operations") == [(0,)]
        probe.evidence.update(failed=failed, rollback=True)

    @case(
        "sect-practice-three-types-replay-conflict",
        "升级攻击修炼",
        "攻击/元血/灵海三入口合法数量并重放、改参冲突",
        "真实宗门成员，三项等级为0，玩家灵石/贡献/宗门资材充足",
        "三项等级各提升1、玩家灵石与宗门资材各扣一次，重放不重复且改参保留operation_conflict",
        group="sect-practice-parameters",
    )
    async def three_types(probe):
        _, sid, database = prepare(probe)
        commands = (("升级攻击修炼", "atkpractice", 989111), ("升级元血修炼", "hppractice", 989112), ("升级灵海修炼", "mppractice", 989113))
        replies = []
        for command, field, number in commands:
            reply = await probe.command(command, f"{command} 1", number=number)
            assert "升级成功" in reply, reply
            replies.append(reply)
            probe.clear_cooldowns()
        state = probe.rows(
            database,
            "SELECT stone,atkpractice,hppractice,mppractice FROM user_xiuxian WHERE user_id='900001'",
        )[0]
        sect = probe.rows(database, "SELECT sect_materials,sect_used_stone FROM sects WHERE sect_id=?", (sid,))[0]
        assert state[1:] == (1, 1, 1) and state[0] < 10**12, (state, sect)
        assert sect[0] < 10**12 and sect[1] == 10**12, sect
        probe.clear_cooldowns()
        replay = await probe.command("升级攻击修炼", "升级攻击修炼 1", number=989111)
        assert "已经完成" in replay and probe.rows(database, "SELECT COUNT(*) FROM sect_practice_operations")[0][0] == 3, replay
        probe.clear_cooldowns()
        conflict = await probe.command("升级攻击修炼", "升级攻击修炼 2", number=989111)
        assert "该操作号已用于不同内容" in conflict, conflict
        await transport_contract(replies[0])
        probe.evidence.update(state=state, sect=sect, replay=replay, conflict=conflict, adapter_contracts=4)

    @case(
        "sect-practice-argument-boundaries",
        "升级攻击修炼",
        "空白/0/负数/非数字/多余参数/超过单次上限",
        "真实宗门成员与充足资源",
        "所有非法数量明确拒绝且玩家等级、灵石、宗门资材和永久回执不变",
        group="sect-practice-parameters",
        outcome="rejection",
    )
    async def argument_boundaries(probe):
        module, sid, database = prepare(probe)
        maximum = max(int(key) for key in module.LEVLECOST)
        blank = await probe.command("升级攻击修炼", "升级攻击修炼", number=989119)
        assert "升级成功" in blank, blank
        probe.player(stone=10**12, atkpractice=0)
        probe.seed(database, "DELETE FROM sect_practice_operations")
        probe.seed(database, "UPDATE sects SET sect_materials=? WHERE sect_id=?", (10**12, sid))
        probe.clear_cooldowns()
        inputs = ("0", "-1", "abc", "1 extra", str(maximum + 1))
        before = probe.rows(database, "SELECT stone,atkpractice FROM user_xiuxian WHERE user_id='900001'")
        for offset, value in enumerate(inputs):
            text = "升级攻击修炼" if not value else f"升级攻击修炼 {value}"
            reply = await probe.command("升级攻击修炼", text, number=989120 + offset)
            assert "正整数" in reply, (value, reply)
            probe.clear_cooldowns()
        assert probe.rows(database, "SELECT stone,atkpractice FROM user_xiuxian WHERE user_id='900001'") == before
        assert probe.rows(database, "SELECT sect_materials FROM sects WHERE sect_id=?", (sid,))[0][0] == 10**12
        assert probe.rows(database, "SELECT COUNT(*) FROM sect_practice_operations") == [(0,)]
        probe.evidence.update(blank_default=blank, inputs=inputs, unchanged=True)

    @case(
        "sect-practice-resource-position-boundaries",
        "升级元血修炼",
        "禁止职位与宗门资材不足边界",
        "真实宗门成员、当前职位和资材可控",
        "权限/资源具体拒绝，玩家等级、灵石、宗门资材与回执不变",
        group="sect-practice-parameters",
        outcome="rejection",
    )
    async def resource_position_boundaries(probe):
        module, sid, database = prepare(probe)
        before = probe.rows(database, "SELECT stone,hppractice FROM user_xiuxian WHERE user_id='900001'")
        probe.player(sect_position=12)
        forbidden = await probe.command("升级元血修炼", "升级元血修炼 1", number=989130)
        assert "不满足使用资材" in forbidden, forbidden
        probe.clear_cooldowns()
        probe.player(sect_position=0)
        cost = module.LEVLECOST["0"]
        probe.seed(database, "UPDATE sects SET sect_materials=? WHERE sect_id=?", (cost * 10 - 1, sid))
        insufficient = await probe.command("升级灵海修炼", "升级灵海修炼 1", number=989131)
        assert "资材不足" in insufficient, insufficient
        assert probe.rows(database, "SELECT stone,hppractice FROM user_xiuxian WHERE user_id='900001'") == before
        assert probe.rows(database, "SELECT COUNT(*) FROM sect_practice_operations") == [(0,)]
        probe.evidence.update(forbidden=forbidden, insufficient=insufficient, unchanged=True)
