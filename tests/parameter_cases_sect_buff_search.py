"""Real command-path coverage for both sect buff-search commands."""
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from tests.parameter_cases_sect_body import seed_sect, sect_module


def _buff(module, kind):
    config = module.config["宗门主功法参数" if kind == "main" else "宗门神通参数"]
    _, tiers = module.get_sectbufftxt(10**7, config)
    tier = tiers[0]
    key = "gf_list" if kind == "main" else "st_list"
    item_id = str(module.BuffJsonDate().get_gfpeizhi()[tier][key][0])
    item = module.items.get_data_by_item_id(item_id)
    assert item and item["name"], (kind, tier, item_id)
    return item_id, item


def _patch_random(module):
    return (
        patch.object(module.runtime_random, "randint", return_value=0),
        patch.object(module.runtime_random, "choice", side_effect=lambda values: values[0]),
    )


def register(case):
    def prepare(probe):
        module = sect_module()
        sid = seed_sect(probe)
        database = probe.context.paths.game_db
        probe.player(sect_position=0)
        probe.seed(
            database,
            "UPDATE sects SET sect_scale=?,sect_used_stone=?,sect_materials=?,mainbuff=?,secbuff=? WHERE sect_id=?",
            (10**7, 10**10, 10**10, "[]", "[]", sid),
        )
        return module, sid, database

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
            md = AsyncMock(side_effect=RuntimeError("isolated buff-search delivery failure") if send_fails else None)
            with patch.object(utils, "XiuConfig", return_value=config), patch.object(utils, "get_chat_scene", return_value="group"), \
                    patch.object(utils, "is_group_event", return_value=True), patch.object(utils, "get_real_id_async", new=AsyncMock(return_value="")), \
                    patch.object(utils, "_send_event_message", plain), patch.object(utils, "_send_markdown_or_keyboard", md):
                await utils.handle_send(bot, event, reply, md_type="宗门", k1="搜寻", v1="宗门功法搜寻")
            if onebot or not markdown or send_fails:
                plain.assert_awaited_once()
                assert plain.call_args.kwargs["message"] == reply
            else:
                md.assert_awaited_once()
                assert "宗门" in str(md.call_args.args[2])
            if onebot:
                md.assert_not_awaited()

    @case("sect-buff-search-main-replay", "宗门功法搜寻", "无参功法搜寻并重放",
          "真实宗主、当前建设度档位、充足宗门灵石和资材",
          "功法目录与宗门资产持久化，重复消息不重复扣费，适配器合同通过",
          group="sect-buff-search-parameters")
    async def main_valid(probe):
        module, sid, database = prepare(probe)
        operation_ids = []
        original_apply = module.sect_application.apply_buff_search
        def capture_apply(*args, **kwargs):
            operation_ids.append(args[0] if args else kwargs.get("operation_id"))
            return original_apply(*args, **kwargs)
        randint, choice = _patch_random(module)
        with patch.object(module.sect_application, "apply_buff_search", side_effect=capture_apply), randint, choice:
            reply = await probe.command("宗门功法搜寻", "宗门功法搜寻", number=989001)
        state = probe.rows(database, "SELECT mainbuff,sect_used_stone,sect_materials FROM sects WHERE sect_id=?", (sid,))[0]
        assert "共消耗" in reply and state[0] != "[]" and state[1] < 10**10 and state[2] < 10**10, (reply, state)
        probe.clear_cooldowns()
        with patch.object(module.sect_application, "apply_buff_search", side_effect=capture_apply), randint, choice:
            replay = await probe.command("宗门功法搜寻", "宗门功法搜寻", number=989001)
        after_replay = probe.rows(database, "SELECT mainbuff,sect_used_stone,sect_materials FROM sects WHERE sect_id=?", (sid,))[0]
        assert "已经完成" in replay, (replay, operation_ids, state, after_replay)
        assert after_replay == state
        await transport_contract(reply)
        probe.evidence.update(reply=reply, replay=replay, state=state, operation_ids=operation_ids, adapter_contracts=4)

    @case("sect-buff-search-secondary-replay", "宗门神通搜寻", "无参神通搜寻并重放",
          "真实宗主、当前建设度档位、充足宗门灵石和资材",
          "神通目录与宗门资产持久化，重复消息不重复扣费",
          group="sect-buff-search-parameters")
    async def secondary_valid(probe):
        module, sid, database = prepare(probe)
        operation_ids = []
        original_apply = module.sect_application.apply_buff_search
        def capture_apply(*args, **kwargs):
            operation_ids.append(args[0] if args else kwargs.get("operation_id"))
            return original_apply(*args, **kwargs)
        randint, choice = _patch_random(module)
        with patch.object(module.sect_application, "apply_buff_search", side_effect=capture_apply), randint, choice:
            reply = await probe.command("宗门神通搜寻", "宗门神通搜寻", number=989002)
        state = probe.rows(database, "SELECT secbuff,sect_used_stone,sect_materials FROM sects WHERE sect_id=?", (sid,))[0]
        assert "共消耗" in reply and state[0] != "[]" and state[1] < 10**10 and state[2] < 10**10, (reply, state)
        probe.clear_cooldowns()
        with patch.object(module.sect_application, "apply_buff_search", side_effect=capture_apply), randint, choice:
            replay = await probe.command("宗门神通搜寻", "宗门神通搜寻", number=989002)
        after_replay = probe.rows(database, "SELECT secbuff,sect_used_stone,sect_materials FROM sects WHERE sect_id=?", (sid,))[0]
        assert "已经完成" in replay, (replay, operation_ids, state, after_replay)
        assert after_replay == state
        probe.evidence.update(reply=reply, replay=replay, state=state, operation_ids=operation_ids)

    @case("sect-buff-search-receipt-ignore", "宗门功法搜寻", "回执拒写后的同消息恢复",
          "真实宗主、充足资源、永久搜寻回执 INSERT 被隔离 trigger 忽略",
          "具体回执原因，目录/资产/回执/触发副作用整笔回滚，故障解除同消息成功",
          group="sect-buff-search-handoff", outcome="rejection")
    async def receipt_ignore(probe):
        module, sid, database = prepare(probe)
        probe.seed(database, "CREATE TABLE isolated_effect(n INTEGER NOT NULL)")
        probe.seed(database, "INSERT INTO isolated_effect VALUES (0)")
        probe.seed(database, "CREATE TRIGGER search_effect AFTER UPDATE OF mainbuff ON sects BEGIN UPDATE isolated_effect SET n=n+1; END")
        probe.seed(database, "CREATE TRIGGER ignore_search_receipt BEFORE INSERT ON sect_buff_search_operations BEGIN SELECT RAISE(IGNORE); END")
        before = probe.rows(database, "SELECT mainbuff,sect_used_stone,sect_materials FROM sects WHERE sect_id=?", (sid,))
        randint, choice = _patch_random(module)
        with randint, choice:
            failed = await probe.command("宗门功法搜寻", "宗门功法搜寻", number=989003)
        assert "回执未保存" in failed and "撤销" in failed, failed
        assert probe.rows(database, "SELECT mainbuff,sect_used_stone,sect_materials FROM sects WHERE sect_id=?", (sid,)) == before
        assert probe.rows(database, "SELECT COUNT(*) FROM sect_buff_search_operations") == [(0,)]
        assert probe.rows(database, "SELECT n FROM isolated_effect") == [(0,)]
        probe.seed(database, "DROP TRIGGER ignore_search_receipt")
        probe.clear_cooldowns()
        with randint, choice:
            recovered = await probe.command("宗门功法搜寻", "宗门功法搜寻", number=989003)
        assert "共消耗" in recovered
        assert probe.rows(database, "SELECT COUNT(*) FROM sect_buff_search_operations") == [(1,)]
        probe.evidence.update(failed=failed, recovered=recovered, receipt_count=1, rollback=True)

    @case("sect-buff-search-resource-boundary", "宗门功法搜寻", "资源不足边界",
          "真实宗主但宗门灵石低于当前档位搜寻费用",
          "明确资源不足，目录、资产和回执不变", group="sect-buff-search-parameters", outcome="rejection")
    async def resource_boundary(probe):
        module, sid, database = prepare(probe)
        config = module.config["宗门主功法参数"]
        cost = config["获取消耗的灵石"]
        probe.seed(database, "UPDATE sects SET sect_used_stone=? WHERE sect_id=?", (cost - 1, sid))
        before = probe.rows(database, "SELECT mainbuff,sect_used_stone,sect_materials FROM sects WHERE sect_id=?", (sid,))
        reply = await probe.command("宗门功法搜寻", "宗门功法搜寻", number=989004)
        assert "不满足条件" in reply and probe.rows(database, "SELECT mainbuff,sect_used_stone,sect_materials FROM sects WHERE sect_id=?", (sid,)) == before
        probe.evidence.update(reply=reply, unchanged=True)

    @case("sect-buff-search-owner-boundary", "宗门神通搜寻", "非宗主无参指令",
          "真实同宗成员但职位不是宗主",
          "明确权限拒绝，神通目录、资产和回执不变", group="sect-buff-search-parameters", outcome="rejection")
    async def owner_boundary(probe):
        module, sid, database = prepare(probe)
        probe.player(sect_position=12)
        before = probe.rows(database, "SELECT secbuff,sect_used_stone,sect_materials FROM sects WHERE sect_id=?", (sid,))
        reply = await probe.command("宗门神通搜寻", "宗门神通搜寻", number=989005)
        assert "不是宗主" in reply and probe.rows(database, "SELECT secbuff,sect_used_stone,sect_materials FROM sects WHERE sect_id=?", (sid,)) == before
        probe.evidence.update(reply=reply, unchanged=True)
