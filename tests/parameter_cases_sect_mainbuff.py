"""Real command-path coverage for sect main-buff learning."""
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from tests.parameter_cases_sect_body import seed_sect, sect_module


def register(case):
    def prepare(probe, *, materials=None, position=1):
        module = sect_module()
        sid = seed_sect(probe)
        buff_id = "9001"
        buff = module.items.get_data_by_item_id(buff_id)
        assert buff and buff["name"]
        cost = module.buffrankkey.get(buff["level"], 100) * module.config["宗门主功法参数"]["学习资材消耗"]
        database = probe.context.paths.game_db
        probe.player(sect_position=position)
        probe.seed(database, "UPDATE sects SET mainbuff=?,sect_materials=? WHERE sect_id=?",
                   (f"[{buff_id}]", cost * 2 if materials is None else materials, sid))
        probe.seed(database, "INSERT OR IGNORE INTO BuffInfo(user_id,main_buff) VALUES('900001',0)")
        return module, sid, buff_id, buff, cost, database

    async def transport_contract(reply):
        from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils import utils
        event = SimpleNamespace(get_user_id=lambda: "900001")
        for onebot, markdown, send_fails in ((True, True, False), (False, False, False),
                                              (False, True, False), (False, True, True)):
            bot = SimpleNamespace(adapter=SimpleNamespace(get_name=lambda: "OneBot V11" if onebot else "QQ"))
            config = SimpleNamespace(markdown_status=markdown, markdown_id="", markdown_id2="",
                                     markdown_button_status=False, at_sender=False,
                                     message_optimization=False, img=False)
            plain = AsyncMock()
            md = AsyncMock(side_effect=RuntimeError("isolated mainbuff delivery failure") if send_fails else None)
            with patch.object(utils, "XiuConfig", return_value=config), patch.object(utils, "get_chat_scene", return_value="group"), \
                    patch.object(utils, "is_group_event", return_value=True), patch.object(utils, "get_real_id_async", new=AsyncMock(return_value="")), \
                    patch.object(utils, "_send_event_message", plain), patch.object(utils, "_send_markdown_or_keyboard", md):
                await utils.handle_send(bot, event, reply, md_type="宗门", k1="学习", v1="宗门功法学习")
            if onebot or not markdown or send_fails:
                plain.assert_awaited_once()
                assert plain.call_args.kwargs["message"] == reply
            else:
                md.assert_awaited_once()
                assert "宗门" in str(md.call_args.args[2])
            if onebot:
                md.assert_not_awaited()

    @case(
        "sect-mainbuff-receipt-ignore",
        "学习宗门功法",
        "合法功法名，永久学习回执 INSERT 被隔离 trigger 忽略",
        "真实宗门成员、迁移表、功法目录与充足宗门资材",
        "具体回执拒写原因，资材/BuffInfo/trigger副作用整笔回滚",
        group="sect-mainbuff-handoff",
        outcome="rejection",
    )
    async def mainbuff_receipt_ignore_red(probe):
        module, sid, buff_id, buff, cost, database = prepare(probe, materials=1_000_000_000)
        before = probe.rows(
            database,
            "SELECT sect_materials FROM sects WHERE sect_id=?",
            (sid,),
        )
        probe.seed(database, "CREATE TABLE isolated_effect(name TEXT)")
        probe.seed(
            database,
            "CREATE TRIGGER ignore_mainbuff_receipt BEFORE INSERT ON sect_mainbuff_learn_operations "
            "BEGIN INSERT INTO isolated_effect VALUES('receipt'); SELECT RAISE(IGNORE); END",
        )
        reply = await probe.command(
            "学习宗门功法", f"学习宗门功法 {buff['name']}", number=988001
        )
        after = probe.rows(
            database,
            "SELECT sect_materials FROM sects WHERE sect_id=?",
            (sid,),
        )
        player_buff = probe.rows(
            database, "SELECT main_buff FROM BuffInfo WHERE user_id='900001'"
        )
        receipt_count = probe.rows(
            database, "SELECT COUNT(*) FROM sect_mainbuff_learn_operations"
        )
        effects = probe.rows(database, "SELECT name FROM isolated_effect")
        probe.evidence.update(
            reply=reply,
            buff_name=buff["name"],
            materials_before=before,
            materials_after=after,
            player_buff=player_buff,
            receipt_count=receipt_count,
            trigger_effects=effects,
        )
        assert "回执未保存" in reply and "撤销" in reply, reply
        assert after == before, (before, after)
        assert player_buff == [(0,)], player_buff
        assert receipt_count == [(0,)], receipt_count
        assert effects == [], effects
        probe.seed(database, "DROP TRIGGER ignore_mainbuff_receipt")
        probe.clear_cooldowns()
        recovered = await probe.command(
            "学习宗门功法", f"学习宗门功法 {buff['name']}", number=988001
        )
        assert "成功学习" in recovered, recovered
        assert probe.rows(database, "SELECT sect_materials FROM sects WHERE sect_id=?", (sid,))[0][0] == before[0][0] - cost
        assert probe.rows(database, "SELECT main_buff FROM BuffInfo WHERE user_id='900001'") == [(int(buff_id),)]
        assert probe.rows(database, "SELECT COUNT(*) FROM sect_mainbuff_learn_operations") == [(1,)]
        probe.evidence["recovery"] = {"reply": recovered, "same_operation": True, "receipt_count": 1}

    @case(
        "sect-mainbuff-valid-replay", "学习宗门功法", "合法功法名并重放",
        "真实宗门成员、目录中存在该功法、资材充足且未学习",
        "学习成功写入资材/BuffInfo/永久回执，重放不重复扣费并满足适配器合同",
        group="sect-mainbuff-parameters",
    )
    async def mainbuff_valid(probe):
        module, sid, buff_id, buff, cost, database = prepare(probe)
        before = probe.rows(database, "SELECT sect_materials FROM sects WHERE sect_id=?", (sid,))[0][0]
        reply = await probe.command("学习宗门功法", f"学习宗门功法 {buff['name']}", number=988010)
        after = probe.rows(database, "SELECT sect_materials FROM sects WHERE sect_id=?", (sid,))[0][0]
        assert "成功学习" in reply and after == before - cost, reply
        assert probe.rows(database, "SELECT main_buff FROM BuffInfo WHERE user_id='900001'") == [(int(buff_id),)]
        assert probe.rows(database, "SELECT COUNT(*) FROM sect_mainbuff_learn_operations") == [(1,)]
        probe.clear_cooldowns()
        replay = await probe.command("学习宗门功法", f"学习宗门功法 {buff['name']}", number=988010)
        assert "成功学习" in replay and probe.rows(database, "SELECT sect_materials FROM sects WHERE sect_id=?", (sid,))[0][0] == after
        assert probe.rows(database, "SELECT COUNT(*) FROM sect_mainbuff_learn_operations") == [(1,)]
        await transport_contract(reply)
        probe.evidence.update(reply=reply, replay=replay, cost=cost, materials_after=after, receipt_count=1,
                              adapter_contracts=4)

    @case(
        "sect-mainbuff-invalid-arguments", "学习宗门功法", "空白与未知功法名",
        "真实宗门目录与未学习玩家",
        "参数拒绝不改变资材、BuffInfo或回执",
        group="sect-mainbuff-parameters", outcome="rejection",
    )
    async def mainbuff_invalid(probe):
        _, sid, _, buff, cost, database = prepare(probe)
        before = probe.rows(database, "SELECT sect_materials FROM sects WHERE sect_id=?", (sid,))[0][0]
        blank = await probe.command("学习宗门功法", "学习宗门功法", number=988011)
        probe.clear_cooldowns()
        unknown = await probe.command("学习宗门功法", "学习宗门功法 不存在的功法", number=988012)
        assert "功法" in blank and "功法" in unknown
        assert probe.rows(database, "SELECT sect_materials FROM sects WHERE sect_id=?", (sid,))[0][0] == before
        assert probe.rows(database, "SELECT main_buff FROM BuffInfo WHERE user_id='900001'") == [(0,)]
        assert probe.rows(database, "SELECT COUNT(*) FROM sect_mainbuff_learn_operations") == [(0,)]
        probe.evidence.update(blank=blank, unknown=unknown, unchanged=True)

    @case(
        "sect-mainbuff-materials-insufficient", "学习宗门功法", "合法功法名但资材不足",
        "真实宗门目录存在功法、资材低于配置消耗",
        "明确资材不足，不写入学习状态或永久回执",
        group="sect-mainbuff-parameters", outcome="rejection",
    )
    async def mainbuff_materials_insufficient(probe):
        _, sid, _, buff, cost, database = prepare(probe)
        probe.seed(database, "UPDATE sects SET sect_materials=? WHERE sect_id=?", (max(cost - 1, 0), sid))
        reply = await probe.command("学习宗门功法", f"学习宗门功法 {buff['name']}", number=988013)
        assert "不满足条件" in reply, reply
        assert probe.rows(database, "SELECT sect_materials FROM sects WHERE sect_id=?", (sid,))[0][0] == max(cost - 1, 0)
        assert probe.rows(database, "SELECT main_buff FROM BuffInfo WHERE user_id='900001'") == [(0,)]
        probe.evidence.update(reply=reply, materials=max(cost - 1, 0), unchanged=True)

    @case(
        "sect-mainbuff-position-forbidden", "学习宗门功法", "宗主/长老等禁止职位",
        "真实功法与充足资材，但玩家职位为宗主",
        "职位限制具体拒绝且不改变宗门资材或玩家功法",
        group="sect-mainbuff-parameters", outcome="rejection",
    )
    async def mainbuff_position(probe):
        _, sid, _, buff, cost, database = prepare(probe, position=12)
        before = probe.rows(database, "SELECT sect_materials FROM sects WHERE sect_id=?", (sid,))[0][0]
        reply = await probe.command("学习宗门功法", f"学习宗门功法 {buff['name']}", number=988014)
        assert "不满足学习要求" in reply, reply
        assert probe.rows(database, "SELECT sect_materials FROM sects WHERE sect_id=?", (sid,))[0][0] == before
        assert probe.rows(database, "SELECT main_buff FROM BuffInfo WHERE user_id='900001'") == [(0,)]
        probe.evidence.update(reply=reply, position=12, unchanged=True)
