"""Real command-path coverage for sect secondary-buff learning."""
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from tests.parameter_cases_sect_body import seed_sect, sect_module


def register(case):
    def prepare(probe, *, position=1):
        module = sect_module()
        sid = seed_sect(probe)
        buff_id = "8001"
        buff = module.items.get_data_by_item_id(buff_id)
        assert buff and buff["name"]
        cost = module.buffrankkey[buff["level"]] * module.config["宗门神通参数"]["学习资材消耗"]
        database = probe.context.paths.game_db
        probe.player(sect_position=position)
        probe.seed(database, "UPDATE sects SET secbuff=?,sect_materials=? WHERE sect_id=?",
                   (f"[{buff_id}]", cost * 2, sid))
        probe.seed(database, "INSERT OR IGNORE INTO BuffInfo(user_id,sec_buff) VALUES('900001',0)")
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
            md = AsyncMock(side_effect=RuntimeError("isolated secbuff delivery failure") if send_fails else None)
            with patch.object(utils, "XiuConfig", return_value=config), patch.object(utils, "get_chat_scene", return_value="group"), \
                    patch.object(utils, "is_group_event", return_value=True), patch.object(utils, "get_real_id_async", new=AsyncMock(return_value="")), \
                    patch.object(utils, "_send_event_message", plain), patch.object(utils, "_send_markdown_or_keyboard", md):
                await utils.handle_send(bot, event, reply, md_type="宗门", k1="学习", v1="宗门神通学习")
            if onebot or not markdown or send_fails:
                plain.assert_awaited_once()
                assert plain.call_args.kwargs["message"] == reply
            else:
                md.assert_awaited_once()
                assert "宗门" in str(md.call_args.args[2])
            if onebot:
                md.assert_not_awaited()

    @case("sect-secbuff-receipt-ignore", "学习宗门神通", "合法神通名，永久回执 INSERT 被隔离 trigger 忽略",
          "真实宗门成员、迁移表、神通目录与充足宗门资材",
          "具体回执拒写原因，资材/BuffInfo/trigger副作用整笔回滚并可恢复",
          group="sect-secbuff-handoff", outcome="rejection")
    async def receipt_ignore(probe):
        _, sid, buff_id, buff, cost, database = prepare(probe)
        before = probe.rows(database, "SELECT sect_materials FROM sects WHERE sect_id=?", (sid,))
        probe.seed(database, "CREATE TABLE isolated_effect(name TEXT)")
        probe.seed(database, "CREATE TRIGGER ignore_secbuff_receipt BEFORE INSERT ON sect_secbuff_learn_operations "
                   "BEGIN INSERT INTO isolated_effect VALUES('receipt'); SELECT RAISE(IGNORE); END")
        reply = await probe.command("学习宗门神通", f"学习宗门神通 {buff['name']}", number=988101)
        assert "回执未保存" in reply and "撤销" in reply
        assert probe.rows(database, "SELECT sect_materials FROM sects WHERE sect_id=?", (sid,)) == before
        assert probe.rows(database, "SELECT sec_buff FROM BuffInfo WHERE user_id='900001'") == [(0,)]
        assert probe.rows(database, "SELECT COUNT(*) FROM sect_secbuff_learn_operations") == [(0,)]
        assert probe.rows(database, "SELECT * FROM isolated_effect") == []
        probe.seed(database, "DROP TRIGGER ignore_secbuff_receipt")
        probe.clear_cooldowns()
        recovered = await probe.command("学习宗门神通", f"学习宗门神通 {buff['name']}", number=988101)
        assert "成功学习" in recovered
        assert probe.rows(database, "SELECT sec_buff FROM BuffInfo WHERE user_id='900001'") == [(int(buff_id),)]
        assert probe.rows(database, "SELECT COUNT(*) FROM sect_secbuff_learn_operations") == [(1,)]
        probe.evidence.update(reply=reply, recovery=recovered, cost=cost, receipt_count=1)

    @case("sect-secbuff-valid-replay", "学习宗门神通", "合法神通名并重放",
          "真实宗门成员、目录中存在该神通、资材充足且未学习",
          "学习成功与重放不重复扣费，OneBot/QQ mock合同通过", group="sect-secbuff-parameters")
    async def valid_replay(probe):
        _, sid, buff_id, buff, cost, database = prepare(probe)
        before = probe.rows(database, "SELECT sect_materials FROM sects WHERE sect_id=?", (sid,))[0][0]
        reply = await probe.command("学习宗门神通", f"学习宗门神通 {buff['name']}", number=988110)
        after = probe.rows(database, "SELECT sect_materials FROM sects WHERE sect_id=?", (sid,))[0][0]
        assert "成功学习" in reply and after == before - cost
        assert probe.rows(database, "SELECT sec_buff FROM BuffInfo WHERE user_id='900001'") == [(int(buff_id),)]
        probe.clear_cooldowns()
        replay = await probe.command("学习宗门神通", f"学习宗门神通 {buff['name']}", number=988110)
        assert "请勿重复学习" in replay and probe.rows(database, "SELECT sect_materials FROM sects WHERE sect_id=?", (sid,))[0][0] == after
        await transport_contract(reply)
        probe.evidence.update(reply=reply, replay=replay, cost=cost, adapter_contracts=4)

    @case("sect-secbuff-invalid-arguments", "学习宗门神通", "空白与未知神通名",
          "真实宗门目录与未学习玩家", "参数拒绝且资产/回执不变", group="sect-secbuff-parameters", outcome="rejection")
    async def invalid_arguments(probe):
        _, sid, _, _, _, database = prepare(probe)
        before = probe.rows(database, "SELECT sect_materials FROM sects WHERE sect_id=?", (sid,))
        blank = await probe.command("学习宗门神通", "学习宗门神通", number=988111)
        probe.clear_cooldowns()
        unknown = await probe.command("学习宗门神通", "学习宗门神通 不存在的神通", number=988112)
        assert "神通" in blank and "神通" in unknown
        assert probe.rows(database, "SELECT sect_materials FROM sects WHERE sect_id=?", (sid,)) == before
        assert probe.rows(database, "SELECT sec_buff FROM BuffInfo WHERE user_id='900001'") == [(0,)]

    @case("sect-secbuff-materials-and-position", "学习宗门神通", "资材不足与职位限制",
          "真实神通目录、边界资材和禁止职位", "资材门槛/职位具体拒绝且不写入", group="sect-secbuff-parameters", outcome="rejection")
    async def materials_and_position(probe):
        _, sid, _, buff, cost, database = prepare(probe)
        probe.seed(database, "UPDATE sects SET sect_materials=? WHERE sect_id=?", (max(cost - 1, 0), sid))
        insufficient = await probe.command("学习宗门神通", f"学习宗门神通 {buff['name']}", number=988113)
        assert "不满足条件" in insufficient
        probe.clear_cooldowns()
        probe.player(sect_position=12)
        probe.seed(database, "UPDATE sects SET sect_materials=? WHERE sect_id=?", (cost * 2, sid))
        forbidden = await probe.command("学习宗门神通", f"学习宗门神通 {buff['name']}", number=988114)
        assert "不满足学习要求" in forbidden
        assert probe.rows(database, "SELECT sec_buff FROM BuffInfo WHERE user_id='900001'") == [(0,)]
