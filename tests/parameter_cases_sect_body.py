"""Sect and body training consume valid migrated membership/profile state."""
from tests.parameter_cases_assets import balances, backpack


def sect_module():
    from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_sect
    return xiuxian_sect


def seed_sect(probe, members=True):
    module = sect_module()
    probe.player(stone=10 ** 12, sect_id=None, sect_position=None)
    created = module.sect_application.create_sect("fixture-sect", "900001", "参数测试宗", 0, 0)
    assert created.applied, created
    if members:
        probe.player("900002", sect_id=created.sect_id, sect_position=12)
    return created.sect_id


def sect_rows(probe):
    return probe.rows(probe.context.paths.game_db, "SELECT * FROM sects ORDER BY sect_id")


def register(case):
    for command, target, actor in (("踢出宗门", "诊断对手", "900001"), ("退出宗门", "", "900002"),
                                   ("宗主传位", "[CQ:at,qq=900002]", "900001")):
        def build(command, target, actor):
            async def run(probe):
                sid = seed_sect(probe)
                text = await probe.command(command, command + " " + target, actor=actor)
                members = probe.rows(probe.context.paths.game_db, "SELECT user_id,sect_id,sect_position FROM user_xiuxian ORDER BY user_id")
                if command == "宗主传位":
                    assert members[1][1:] == (sid, 0) and members[0][2] != 0, (text, members)
                else:
                    assert not members[1][1] and members[0][1] == sid, (text, members)
                probe.evidence.update(membership=members, sect_id=sid)
            return run
            case("sect-member-operation-" + command, command, "真实道号" if target else "无参数（注册声明了CommandArg）",
                 "宗主和同宗弟子，合法权限", "成员关系按操作真实更新", group="sect-extended")(build(command, target, actor))

    @case("sect-owner-transfer-replay-conflict", "宗主传位", "艾特目标/同消息重放/改参",
          "真实宗主与同宗候选成员", "owner、双方职位和回执真实持久化；重放不重复传位并保留冲突原因",
          group="sect-owner-transfer-parameters")
    async def owner_transfer(probe):
        sid = seed_sect(probe)
        database = probe.context.paths.game_db
        command = "宗主传位 [CQ:at,qq=900002]"
        text = await probe.command("宗主传位", command, actor="900001", number=960110)
        state = probe.rows(database, "SELECT sect_owner FROM sects WHERE sect_id=?", (sid,))
        members = probe.rows(database, "SELECT user_id,sect_position FROM user_xiuxian WHERE user_id IN ('900001','900002') ORDER BY user_id")
        receipt = probe.rows(database, "SELECT operation_type,actor_id,target_id,sect_id FROM sect_operations")
        assert [str(row[0]) for row in state] == ["900002"] and members == [("900001", 1), ("900002", 0)] and receipt == [("transfer_owner", "900001", "900002", sid)], (text, state, members, receipt)
        probe.clear_cooldowns()
        replay = await probe.command("宗主传位", command, actor="900001", number=960110)
        assert "已经完成" in replay and "继任" not in replay, replay
        probe.clear_cooldowns()
        conflict = await probe.command("宗主传位", "宗主传位 [CQ:at,qq=900003]", actor="900001", number=960110)
        assert "该操作号已用于不同内容" in conflict, conflict
        from types import SimpleNamespace
        from unittest.mock import AsyncMock, patch
        from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils import utils
        event = SimpleNamespace(get_user_id=lambda: "900001")
        for onebot, markdown, send_fails in ((True, True, False), (False, False, False), (False, True, False), (False, True, True)):
            bot = SimpleNamespace(adapter=SimpleNamespace(get_name=lambda: "OneBot V11" if onebot else "QQ"))
            config = SimpleNamespace(markdown_status=markdown, markdown_id="", markdown_id2="", markdown_button_status=False, at_sender=False, message_optimization=False, img=False)
            plain = AsyncMock()
            markdown_send = AsyncMock(side_effect=RuntimeError("isolated owner transfer delivery failure") if send_fails else None)
            with patch.object(utils, "XiuConfig", return_value=config), patch.object(utils, "get_chat_scene", return_value="group"), patch.object(utils, "is_group_event", return_value=True), patch.object(utils, "get_real_id_async", new=AsyncMock(return_value="")), patch.object(utils, "_send_event_message", plain), patch.object(utils, "_send_markdown_or_keyboard", markdown_send):
                await utils.handle_send(bot, event, "传位回执合同", md_type="宗门", k1="宗门", v1="我的宗门")
            if onebot or not markdown or send_fails:
                plain.assert_awaited_once()
                assert plain.call_args.kwargs["message"] == "传位回执合同"
            else:
                markdown_send.assert_awaited_once()
        probe.evidence.update(owner=state, members=members, receipt=receipt, replay=replay, conflict=conflict, adapter_contracts=4)

    for key, trigger, expected in (
        ("sect-owner-transfer-member-recovery", "CREATE TRIGGER sect_owner_transfer_target_rewrite AFTER UPDATE OF sect_position ON user_xiuxian WHEN NEW.user_id='900002' AND NEW.sect_position=0 BEGIN UPDATE user_xiuxian SET sect_position=4 WHERE user_id='900002'; END", "候选宗主职位保存失败"),
        ("sect-owner-transfer-receipt-recovery", "CREATE TRIGGER sect_owner_transfer_receipt_ignore BEFORE INSERT ON sect_operations BEGIN SELECT RAISE(IGNORE); END", "宗主传位回执保存失败"),
    ):
        @case(key, "宗主传位", "艾特目标/故障解除后同消息恢复", "隔离数据库注入一次性写拒绝触发器", "拒绝保留具体原因、owner/职位不变；解除后同操作号成功", group="sect-owner-transfer-recovery", outcome="recovery")
        async def owner_transfer_recovery(probe, trigger=trigger, expected=expected):
            sid = seed_sect(probe)
            database = probe.context.paths.game_db
            probe.seed(database, trigger)
            command = "宗主传位 [CQ:at,qq=900002]"
            failed = await probe.command("宗主传位", command, actor="900001", number=960120 if "target" in trigger else 960130)
            assert expected in failed, failed
            assert [str(row[0]) for row in probe.rows(database, "SELECT sect_owner FROM sects WHERE sect_id=?", (sid,))] == ["900001"]
            assert probe.rows(database, "SELECT user_id,sect_position FROM user_xiuxian WHERE user_id IN ('900001','900002') ORDER BY user_id") == [("900001", 0), ("900002", 12)]
            probe.seed(database, "DROP TRIGGER " + ("sect_owner_transfer_target_rewrite" if "target" in trigger else "sect_owner_transfer_receipt_ignore"))
            probe.clear_cooldowns()
            recovered = await probe.command("宗主传位", command, actor="900001", number=960120 if "target" in trigger else 960130)
            assert "继任" in recovered
            assert [str(row[0]) for row in probe.rows(database, "SELECT sect_owner FROM sects WHERE sect_id=?", (sid,))] == ["900002"]
            assert probe.rows(database, "SELECT COUNT(*) FROM sect_operations WHERE operation_type='transfer_owner'") == [(1,)]
            probe.evidence.update(failed=failed, recovered=True, owner="900002", receipt_count=1)

    @case("sect-owner-lifecycle-replay", "宗门封闭与继承", "封闭山门/确认封闭山门/继承宗主",
          "真实宗主、同宗长老和迁移回执表", "封闭后 owner/职位/开放状态持久化，两个入口 replay 不重复修改",
          group="sect-owner-lifecycle-parameters")
    async def owner_lifecycle(probe):
        sid = seed_sect(probe)
        database = probe.context.paths.game_db
        prompt = await probe.command("封闭山门", "封闭山门", actor="900001", number=960140)
        assert "确认封闭山门" in prompt, prompt
        closed = await probe.command("确认封闭山门", "确认封闭山门", actor="900001", number=960141)
        assert "已封闭山门" in closed, closed
        state = probe.rows(database, "SELECT sect_owner,closed,join_open FROM sects WHERE sect_id=?", (sid,))
        members = probe.rows(database, "SELECT user_id,sect_position FROM user_xiuxian WHERE user_id IN ('900001','900002') ORDER BY user_id")
        assert [tuple(str(value) if index == 0 and value is not None else value for index, value in enumerate(row)) for row in state] == [(None, 1, 0)] and members == [("900001", 2), ("900002", 12)], (closed, state, members)
        probe.clear_cooldowns()
        close_replay = await probe.command("确认封闭山门", "确认封闭山门", actor="900001", number=960141)
        assert "已经完成" in close_replay, close_replay
        inherited = await probe.command("继承宗主", "继承宗主", actor="900001", number=960142)
        assert "继承宗主之位" in inherited, inherited
        state = probe.rows(database, "SELECT sect_owner,closed,join_open FROM sects WHERE sect_id=?", (sid,))
        assert [str(row[0]) for row in state] == ["900001"] and state[0][1:] == (0, 1), (inherited, state)
        probe.clear_cooldowns()
        inherit_replay = await probe.command("继承宗主", "继承宗主", actor="900001", number=960142)
        assert "已经完成" in inherit_replay, inherit_replay
        from types import SimpleNamespace
        from unittest.mock import AsyncMock, patch
        from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils import utils
        event = SimpleNamespace(get_user_id=lambda: "900002")
        adapter_contracts = 0
        for onebot, markdown, send_fails in ((True, True, False), (False, False, False), (False, True, False), (False, True, True)):
            bot = SimpleNamespace(adapter=SimpleNamespace(get_name=lambda: "OneBot V11" if onebot else "QQ"))
            config = SimpleNamespace(markdown_status=markdown, markdown_id="", markdown_id2="", markdown_button_status=False, at_sender=False, message_optimization=False, img=False)
            plain = AsyncMock()
            markdown_send = AsyncMock(side_effect=RuntimeError("isolated owner lifecycle delivery failure") if send_fails else None)
            with patch.object(utils, "XiuConfig", return_value=config), patch.object(utils, "get_chat_scene", return_value="group"), patch.object(utils, "is_group_event", return_value=True), patch.object(utils, "get_real_id_async", new=AsyncMock(return_value="")), patch.object(utils, "_send_event_message", plain), patch.object(utils, "_send_markdown_or_keyboard", markdown_send):
                await utils.handle_send(bot, event, "宗主状态回执合同", md_type="宗门", k1="继承", v1="继承宗主")
            adapter_contracts += 1
            if onebot or not markdown or send_fails:
                plain.assert_awaited_once()
                assert plain.call_args.kwargs["message"] == "宗主状态回执合同"
            else:
                markdown_send.assert_awaited_once()
        probe.evidence.update(closed=closed, inherited=inherited, close_replay=close_replay, inherit_replay=inherit_replay, state=state, adapter_contracts=adapter_contracts)

    @case("sect-owner-lifecycle-close-recovery", "确认封闭山门", "成员职位故障/解除后同消息恢复",
          "已进入确认封闭山门，隔离数据库注入成员职位改写", "具体原因、状态回滚，解除触发器后封闭成功",
          group="sect-owner-lifecycle-recovery", outcome="recovery")
    async def close_recovery(probe):
        sid = seed_sect(probe)
        database = probe.context.paths.game_db
        await probe.command("封闭山门", "封闭山门", actor="900001", number=960150)
        probe.seed(database, "CREATE TRIGGER sect_close_member_rewrite AFTER UPDATE OF sect_position ON user_xiuxian WHEN NEW.user_id='900001' AND NEW.sect_position=2 BEGIN UPDATE user_xiuxian SET sect_position=3 WHERE user_id='900001'; END")
        failed = await probe.command("确认封闭山门", "确认封闭山门", actor="900001", number=960151)
        assert "原宗主职位保存失败" in failed, failed
        close_state = probe.rows(database, "SELECT sect_owner,closed,join_open FROM sects WHERE sect_id=?", (sid,))
        assert [(str(row[0]), row[1], row[2]) for row in close_state] == [("900001", 0, 1)]
        probe.seed(database, "DROP TRIGGER sect_close_member_rewrite")
        probe.clear_cooldowns()
        recovered = await probe.command("确认封闭山门", "确认封闭山门", actor="900001", number=960151)
        assert "已封闭山门" in recovered, recovered
        probe.evidence.update(failed=failed, recovered=True)

    @case("sect-owner-lifecycle-inherit-recovery", "继承宗主", "回执故障/解除后同消息恢复",
          "宗门已封闭且长老符合继承资格，隔离数据库注入回执 IGNORE", "具体原因、owner/职位回滚，解除触发器后继承成功",
          group="sect-owner-lifecycle-recovery", outcome="recovery")
    async def inherit_recovery(probe):
        sid = seed_sect(probe)
        database = probe.context.paths.game_db
        await probe.command("封闭山门", "封闭山门", actor="900001", number=960160)
        await probe.command("确认封闭山门", "确认封闭山门", actor="900001", number=960161)
        probe.seed(database, "CREATE TRIGGER sect_inherit_receipt_ignore BEFORE INSERT ON sect_owner_inherit_operations BEGIN SELECT RAISE(IGNORE); END")
        failed = await probe.command("继承宗主", "继承宗主", actor="900001", number=960162)
        assert "继承宗主回执保存失败" in failed, failed
        assert probe.rows(database, "SELECT sect_owner,closed,join_open FROM sects WHERE sect_id=?", (sid,)) == [(None, 1, 0)]
        probe.seed(database, "DROP TRIGGER sect_inherit_receipt_ignore")
        probe.clear_cooldowns()
        recovered = await probe.command("继承宗主", "继承宗主", actor="900001", number=960162)
        assert "继承宗主之位" in recovered, recovered
        probe.evidence.update(failed=failed, recovered=True)

    @case("sect-join-state-replay-conflict", "宗门加入开放/关闭", "关闭、开放、同消息重放与改参",
          "真实宗主、开放宗门和同宗成员", "join_open 与两张永久回执实际持久化；重放不重复修改，换 actor 保留冲突原因",
          group="sect-join-state-parameters")
    async def join_state_replay(probe):
        sid = seed_sect(probe)
        database = probe.context.paths.game_db
        closed = await probe.command("关闭宗门加入", "关闭宗门加入", actor="900001", number=960180)
        assert "已关闭宗门加入" in closed, closed
        assert probe.rows(database, "SELECT join_open FROM sects WHERE sect_id=?", (sid,)) == [(0,)]
        assert probe.rows(database, "SELECT actor_id,sect_id FROM sect_close_join_operations") == [("900001", sid)]
        probe.clear_cooldowns()
        close_replay = await probe.command("关闭宗门加入", "关闭宗门加入", actor="900001", number=960180)
        assert "已经完成" in close_replay and "已关闭宗门加入" not in close_replay, close_replay
        opened = await probe.command("开放宗门加入", "开放宗门加入", actor="900001", number=960181)
        assert "已开放宗门加入" in opened, opened
        assert probe.rows(database, "SELECT join_open FROM sects WHERE sect_id=?", (sid,)) == [(1,)]
        assert probe.rows(database, "SELECT actor_id,sect_id FROM sect_open_join_operations") == [("900001", sid)]
        probe.clear_cooldowns()
        open_replay = await probe.command("开放宗门加入", "开放宗门加入", actor="900001", number=960181)
        assert "已经完成" in open_replay and "已开放宗门加入" not in open_replay, open_replay
        probe.clear_cooldowns()
        conflict = await probe.command("开放宗门加入", "开放宗门加入", actor="900002", number=960181)
        assert "冲突" in conflict or "不同" in conflict, conflict
        from types import SimpleNamespace
        from unittest.mock import AsyncMock, patch
        from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils import utils
        event = SimpleNamespace(get_user_id=lambda: "900001")
        adapter_contracts = 0
        for onebot, markdown, send_fails in ((True, True, False), (False, False, False), (False, True, False), (False, True, True)):
            bot = SimpleNamespace(adapter=SimpleNamespace(get_name=lambda: "OneBot V11" if onebot else "QQ"))
            config = SimpleNamespace(markdown_status=markdown, markdown_id="", markdown_id2="", markdown_button_status=False, at_sender=False, message_optimization=False, img=False)
            plain = AsyncMock()
            markdown_send = AsyncMock(side_effect=RuntimeError("isolated join-state delivery failure") if send_fails else None)
            with patch.object(utils, "XiuConfig", return_value=config), patch.object(utils, "get_chat_scene", return_value="group"), patch.object(utils, "is_group_event", return_value=True), patch.object(utils, "get_real_id_async", new=AsyncMock(return_value="")), patch.object(utils, "_send_event_message", plain), patch.object(utils, "_send_markdown_or_keyboard", markdown_send):
                await utils.handle_send(bot, event, conflict, md_type="宗门", k1="加入", v1="宗门加入")
            adapter_contracts += 1
            if onebot or not markdown or send_fails:
                plain.assert_awaited_once()
            else:
                markdown_send.assert_awaited_once()
        probe.evidence.update(sid=sid, closed=closed, opened=opened, close_replay=close_replay, open_replay=open_replay, conflict=conflict, adapter_contracts=adapter_contracts)

    @case("sect-close-join-state-recovery", "关闭宗门加入", "状态改写/解除后同消息恢复",
          "真实开放宗门与一次性 AFTER trigger", "具体状态拒写原因、join_open 回滚；解除 trigger 后同消息关闭并写回执",
          group="sect-join-state-recovery", outcome="recovery")
    async def close_join_state_recovery(probe):
        sid = seed_sect(probe)
        database = probe.context.paths.game_db
        probe.seed(database, "CREATE TRIGGER sect_close_join_state_rewrite AFTER UPDATE ON sects BEGIN UPDATE sects SET join_open=1 WHERE sect_id=?; END".replace("?", str(sid)))
        failed = await probe.command("关闭宗门加入", "关闭宗门加入", actor="900001", number=960182)
        assert "关闭宗门加入状态保存失败" in failed, failed
        assert probe.rows(database, "SELECT join_open FROM sects WHERE sect_id=?", (sid,)) == [(1,)]
        assert probe.rows(database, "SELECT COUNT(*) FROM sect_close_join_operations") == [(0,)]
        probe.seed(database, "DROP TRIGGER sect_close_join_state_rewrite")
        probe.clear_cooldowns()
        recovered = await probe.command("关闭宗门加入", "关闭宗门加入", actor="900001", number=960182)
        assert "已关闭宗门加入" in recovered, recovered
        assert probe.rows(database, "SELECT join_open FROM sects WHERE sect_id=?", (sid,)) == [(0,)]
        probe.evidence.update(sid=sid, failed=failed, recovered=recovered)

    @case("sect-open-join-receipt-recovery", "开放宗门加入", "回执 IGNORE/解除后同消息恢复",
          "真实关闭宗门与一次性永久回执 trigger", "具体回执拒写原因、状态回滚；解除 trigger 后同消息开放并写回执",
          group="sect-join-state-recovery", outcome="recovery")
    async def open_join_receipt_recovery(probe):
        sid = seed_sect(probe)
        database = probe.context.paths.game_db
        await probe.command("关闭宗门加入", "关闭宗门加入", actor="900001", number=960183)
        probe.seed(database, "CREATE TRIGGER sect_open_join_receipt_ignore BEFORE INSERT ON sect_open_join_operations BEGIN SELECT RAISE(IGNORE); END")
        failed = await probe.command("开放宗门加入", "开放宗门加入", actor="900001", number=960184)
        assert "开放宗门加入回执保存失败" in failed, failed
        assert probe.rows(database, "SELECT join_open FROM sects WHERE sect_id=?", (sid,)) == [(0,)]
        assert probe.rows(database, "SELECT COUNT(*) FROM sect_open_join_operations") == [(0,)]
        probe.seed(database, "DROP TRIGGER sect_open_join_receipt_ignore")
        probe.clear_cooldowns()
        recovered = await probe.command("开放宗门加入", "开放宗门加入", actor="900001", number=960184)
        assert "已开放宗门加入" in recovered, recovered
        assert probe.rows(database, "SELECT join_open FROM sects WHERE sect_id=?", (sid,)) == [(1,)]
        probe.evidence.update(sid=sid, failed=failed, recovered=recovered)

    for command, field in (("升级攻击修炼", "atkpractice"), ("升级元血修炼", "hppractice"), ("升级灵海修炼", "mppractice")):
        for label, value in (("valid", "2"), ("zero", "0"), ("negative", "-2"), ("bad", "abc"), ("extra", "2 extra")):
            def build(command, field, label, value):
                async def run(probe):
                    sid = seed_sect(probe)
                    probe.player(sect_contribution=10**12, atkpractice=0, hppractice=0, mppractice=0)
                    probe.seed(probe.context.paths.game_db, "UPDATE sects SET sect_scale=?,sect_materials=?,sect_used_stone=? WHERE sect_id=?", (10**12, 10**15, 10**15, sid))
                    before, sect = balances(probe), sect_rows(probe)
                    text = await probe.command(command, command + " " + value)
                    level = probe.rows(probe.context.paths.game_db, f"SELECT {field} FROM user_xiuxian WHERE user_id='900001'")[0][0]
                    if label == "valid":
                        assert level == 2 and sect_rows(probe) != sect, (text, level)
                    else:
                        assert level == 0 and balances(probe) == before and sect_rows(probe) == sect, (text, level)
                    probe.evidence.update(field=field, level=level, argument=value)
                return run
            case("sect-practice-" + field + "-" + label, command, "数量" + label, "充足灵石/贡献/宗门资材与修炼上限",
                 "精确提升2级并扣费" if label == "valid" else "数量格式拒绝且资产与等级不变", group="sect-extended",
                 outcome="success" if label == "valid" else "rejection")(build(command, field, label, value))

    for label, quantity in (("valid", "2"), ("zero", "0"), ("negative", "-2"), ("bad", "abc"), ("extra", "2 extra")):
        def build(label, quantity):
            async def run(probe):
                sid = seed_sect(probe)
                module = sect_module()
                item_id = next(key for key, value in module.config["商店商品"].items() if value["weekly_limit"] >= 2)
                probe.player(sect_contribution=10**12)
                probe.seed(probe.context.paths.game_db, "UPDATE sects SET sect_materials=? WHERE sect_id=?", (10**15, sid))
                prior = probe.managers[0].goods_num("900001", int(item_id)) or 0
                before, sect, bag = balances(probe), sect_rows(probe), backpack(probe)
                text = await probe.command("宗门兑换", f"宗门兑换 {item_id} {quantity}", number=960008)
                if label == "valid":
                    assert probe.managers[0].goods_num("900001", int(item_id)) == prior + 2, text
                    cost = 2 * module.config["商店商品"][item_id]["cost"]
                    assert probe.rows(probe.context.paths.game_db, "SELECT sect_contribution FROM user_xiuxian WHERE user_id='900001'") == [(10**12 - cost,)]
                    saved = backpack(probe)
                    await probe.command("宗门兑换", f"宗门兑换 {item_id} 2", number=960008)
                    assert backpack(probe) == saved
                else:
                    assert backpack(probe) == bag and balances(probe) == before and sect_rows(probe) == sect, text
                probe.evidence.update(item_id=item_id, quantity=quantity, actual_quantity=probe.managers[0].goods_num("900001", int(item_id)))
            return run
        case("sect-shop-" + label, "宗门兑换", "真实商品编号 数量" + label, "真实宗门/商品与充足贡献资材",
             "实际获得2件、扣贡献与资材、重放不重复" if label == "valid" else "参数拒绝不兑换", group="sect-extended",
             outcome="success" if label == "valid" else "rejection")(build(label, quantity))

    @case("sect-join-name", "加入宗门", "真实宗门名", "已创建开放宗门，目标尚未入宗",
          "真实成员加入，宗门人数与玩家成员状态持久化", group="sect-body-parameters")
    async def join(probe):
        sid = seed_sect(probe, members=False)
        text = await probe.command("加入宗门", "加入宗门 参数测试宗", actor="900002")
        rows = probe.rows(probe.context.paths.game_db, "SELECT sect_id,sect_position FROM user_xiuxian WHERE user_id='900002'")
        assert rows == [(sid, 12)], (text, rows)
        probe.evidence.update(sect_id=sid, membership=rows)

    @case("sect-donate-valid-amount", "宗门捐献", "正整数灵石数量", "真实宗主、宗门和充足灵石",
          "扣款、贡献/建设/资材增加和回执持久化，重放不重复且提示已完成", group="sect-donation-parameters")
    async def donate(probe):
        sid = seed_sect(probe)
        database = probe.context.paths.game_db
        before = balances(probe)
        prior = probe.rows(database, "SELECT sect_scale,sect_materials FROM sects WHERE sect_id=?", (sid,))
        text = await probe.command("宗门捐献", "宗门捐献 1000", number=960001)
        after = balances(probe)
        saved = probe.rows(database, "SELECT sect_scale,sect_materials FROM sects WHERE sect_id=?", (sid,))
        assert before[0][1] - after[0][1] == 1000 and saved[0][0] - prior[0][0] == 1000 and saved[0][1] > prior[0][1], (text, before, after, prior, saved)
        assert probe.rows(database, "SELECT sect_contribution FROM user_xiuxian WHERE user_id='900001'")[0][0] >= 1000
        replay = await probe.command("宗门捐献", "宗门捐献 1000", number=960001)
        assert "已经完成" in replay and "捐献灵石1000枚" not in replay, replay
        conflict = await probe.command("宗门捐献", "宗门捐献 2000", number=960001)
        assert "该操作号已用于不同内容" in conflict, conflict
        assert balances(probe) == after and probe.rows(database, "SELECT sect_scale,sect_materials FROM sects WHERE sect_id=?", (sid,)) == saved
        from types import SimpleNamespace
        from unittest.mock import AsyncMock, patch
        from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils import utils
        event = SimpleNamespace(get_user_id=lambda: "900001")
        for onebot, markdown, send_fails in ((True, True, False), (False, False, False), (False, True, False), (False, True, True)):
            bot = SimpleNamespace(adapter=SimpleNamespace(get_name=lambda: "OneBot V11" if onebot else "QQ"))
            config = SimpleNamespace(markdown_status=markdown, markdown_id="", markdown_id2="", markdown_button_status=False, at_sender=False, message_optimization=False, img=False)
            plain = AsyncMock()
            markdown_send = AsyncMock(side_effect=RuntimeError("isolated donation delivery failure") if send_fails else None)
            with patch.object(utils, "XiuConfig", return_value=config), patch.object(utils, "get_chat_scene", return_value="group"), patch.object(utils, "is_group_event", return_value=True), patch.object(utils, "get_real_id_async", new=AsyncMock(return_value="")), patch.object(utils, "_send_event_message", plain), patch.object(utils, "_send_markdown_or_keyboard", markdown_send):
                await utils.handle_send(bot, event, "捐献回执合同", md_type="宗门", k1="捐献", v1="宗门捐献")
            if onebot or not markdown or send_fails:
                plain.assert_awaited_once()
                assert plain.call_args.kwargs["message"] == "捐献回执合同"
            else:
                markdown_send.assert_awaited_once()
        probe.evidence.update(sect_id=sid, before=prior, after=saved, wallet_delta=-1000, replay_unchanged=True, replay=replay, conflict=conflict, adapter_contracts=4)

    @case("sect-member-keyword-page", "宗门成员查看", "真实成员名 页码", "宗门中两位合法成员",
          "展示指定成员而不显示未匹配成员，资料不变", group="sect-body-parameters")
    async def members(probe):
        seed_sect(probe)
        text = await probe.command("宗门成员查看", "宗门成员查看 诊断对手 1")
        assert "诊断对手" in text, text
        probe.evidence.update(filtered_member="900002")

    @case("sect-position-name-target", "宗门职位变更", "真实成员道号 合法职位名", "宗主有权限、目标同宗且非宗主",
          "职位管理写入实际成员职位，不改变另一宗成员", group="sect-body-parameters")
    async def position(probe):
        seed_sect(probe)
        module = sect_module()
        positions = module.jsondata.sect_config_data()
        target = next((key, value["title"]) for key, value in positions.items() if value["title"] == "长老")
        text = await probe.command("宗门职位变更", "宗门职位变更 诊断对手 " + target[1])
        assert probe.rows(probe.context.paths.game_db, "SELECT sect_position FROM user_xiuxian WHERE user_id='900002'") == [(int(target[0]),)], text
        probe.evidence.update(target="900002", position=target)

    @case("sect-position-replay-conflict", "宗门职位变更", "职位名称成功/同消息重放/改参", "真实宗主与同宗成员，职位名额可用",
          "职位实际持久化；重放不重复播报；改参保留operation_conflict", group="sect-position-parameters")
    async def position_replay(probe):
        sid = seed_sect(probe)
        module = sect_module()
        target = next((key, value["title"]) for key, value in module.jsondata.sect_config_data().items() if value["title"] == "长老")
        command = "宗门职位变更 诊断对手 " + target[1]
        text = await probe.command("宗门职位变更", command, number=960040)
        assert probe.rows(probe.context.paths.game_db, "SELECT sect_position FROM user_xiuxian WHERE user_id='900002'") == [(int(target[0]),)], (text, sid)
        probe.clear_cooldowns()
        replay = await probe.command("宗门职位变更", command, number=960040)
        assert "已经完成" in replay and "传长老" not in replay, replay
        probe.clear_cooldowns()
        conflict = await probe.command("宗门职位变更", "宗门职位变更 诊断对手 外门弟子", number=960040)
        assert "该操作号已用于不同内容" in conflict, conflict
        from types import SimpleNamespace
        from unittest.mock import AsyncMock, patch
        from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils import utils
        event = SimpleNamespace(get_user_id=lambda: "900001")
        contracts = 0
        for onebot, markdown, send_fails in ((True, True, False), (False, False, False), (False, True, False), (False, True, True)):
            bot = SimpleNamespace(adapter=SimpleNamespace(get_name=lambda: "OneBot V11" if onebot else "QQ"))
            config = SimpleNamespace(markdown_status=markdown, markdown_id="", markdown_id2="", markdown_button_status=False, at_sender=False, message_optimization=False, img=False)
            plain = AsyncMock()
            markdown_send = AsyncMock(side_effect=RuntimeError("isolated position delivery failure") if send_fails else None)
            with patch.object(utils, "XiuConfig", return_value=config), patch.object(utils, "get_chat_scene", return_value="group"), patch.object(utils, "is_group_event", return_value=True), patch.object(utils, "get_real_id_async", new=AsyncMock(return_value="")), patch.object(utils, "_send_event_message", plain), patch.object(utils, "_send_markdown_or_keyboard", markdown_send):
                await utils.handle_send(bot, event, "职位回执合同", md_type="宗门", k1="宗门", v1="我的宗门")
            if onebot or not markdown or send_fails:
                plain.assert_awaited_once()
            else:
                markdown_send.assert_awaited_once()
            contracts += 1
        probe.evidence.update(sect_id=sid, position=target, replay=replay, conflict=conflict, adapter_contracts=contracts)

    @case("sect-position-member-write-recovery", "宗门职位变更", "成员职位 trigger 改写后同消息恢复", "真实宗主、同宗成员和可用职位",
          "具体成员写拒绝、职位与回执回滚；解除故障后同消息成功", group="sect-position-recovery", outcome="rejection")
    async def position_member_recovery(probe):
        sid = seed_sect(probe)
        module = sect_module()
        target = next((key, value["title"]) for key, value in module.jsondata.sect_config_data().items() if value["title"] == "长老")
        database = probe.context.paths.game_db
        probe.seed(database, f"CREATE TRIGGER sect_position_member_rewrite AFTER UPDATE OF sect_position ON user_xiuxian WHEN NEW.user_id='900002' AND NEW.sect_position={int(target[0])} BEGIN UPDATE user_xiuxian SET sect_position=3 WHERE user_id='900002'; END")
        first = await probe.command("宗门职位变更", "宗门职位变更 诊断对手 " + target[1], number=960050)
        assert "成员职位保存失败" in first, first
        assert probe.rows(database, "SELECT sect_position FROM user_xiuxian WHERE user_id='900002'") == [(12,)]
        assert probe.rows(database, "SELECT COUNT(*) FROM sect_position_change_operations WHERE operation_id LIKE 'sect:960050:%'") == [(0,)]
        probe.seed(database, "DROP TRIGGER sect_position_member_rewrite")
        probe.clear_cooldowns()
        recovered = await probe.command("宗门职位变更", "宗门职位变更 诊断对手 " + target[1], number=960050)
        assert "晋升为本宗" in recovered, recovered
        assert probe.rows(database, "SELECT sect_position FROM user_xiuxian WHERE user_id='900002'") == [(int(target[0]),)]
        assert probe.rows(database, "SELECT COUNT(*) FROM sect_position_change_operations WHERE operation_id LIKE 'sect:960050:%'") == [(1,)]
        probe.evidence.update(sect_id=sid, reason="sect_position_member_write_rejected", recovered=True)

    @case("sect-position-receipt-write-recovery", "宗门职位变更", "回执 IGNORE 后同消息恢复", "真实宗主、同宗成员和可用职位",
          "具体回执拒绝、职位与回执回滚；解除故障后同消息成功", group="sect-position-recovery", outcome="rejection")
    async def position_receipt_recovery(probe):
        sid = seed_sect(probe)
        module = sect_module()
        target = next((key, value["title"]) for key, value in module.jsondata.sect_config_data().items() if value["title"] == "长老")
        database = probe.context.paths.game_db
        probe.seed(database, "CREATE TRIGGER sect_position_receipt_ignore BEFORE INSERT ON sect_position_change_operations BEGIN SELECT RAISE(IGNORE); END")
        first = await probe.command("宗门职位变更", "宗门职位变更 诊断对手 " + target[1], number=960060)
        assert "职位变更回执保存失败" in first, first
        assert probe.rows(database, "SELECT sect_position FROM user_xiuxian WHERE user_id='900002'") == [(12,)]
        assert probe.rows(database, "SELECT COUNT(*) FROM sect_position_change_operations WHERE operation_id LIKE 'sect:960060:%'") == [(0,)]
        probe.seed(database, "DROP TRIGGER sect_position_receipt_ignore")
        probe.clear_cooldowns()
        recovered = await probe.command("宗门职位变更", "宗门职位变更 诊断对手 " + target[1], number=960060)
        assert "晋升为本宗" in recovered, recovered
        assert probe.rows(database, "SELECT sect_position FROM user_xiuxian WHERE user_id='900002'") == [(int(target[0]),)]
        assert probe.rows(database, "SELECT COUNT(*) FROM sect_position_change_operations WHERE operation_id LIKE 'sect:960060:%'") == [(1,)]
        probe.evidence.update(sect_id=sid, reason="sect_position_receipt_write_rejected", recovered=True)

    @case("sect-kick-replay", "踢出宗门", "成员道号成功/同消息重放", "真实宗主与同宗成员",
          "成员实际脱离宗门；重放只提示已完成且不重复写", group="sect-member-removal-parameters")
    async def kick_replay(probe):
        sid = seed_sect(probe)
        database = probe.context.paths.game_db
        text = await probe.command("踢出宗门", "踢出宗门 诊断对手", number=960070)
        assert "被" in text and probe.rows(database, "SELECT sect_id,sect_position,sect_contribution FROM user_xiuxian WHERE user_id='900002'") == [(None, None, 0)], text
        probe.clear_cooldowns()
        replay = await probe.command("踢出宗门", "踢出宗门 诊断对手", number=960070)
        assert "已经完成" in replay and "被" not in replay, replay
        from types import SimpleNamespace
        from unittest.mock import AsyncMock, patch
        from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils import utils
        event = SimpleNamespace(get_user_id=lambda: "900001")
        contracts = 0
        for onebot, markdown, send_fails in ((True, True, False), (False, False, False), (False, True, False), (False, True, True)):
            bot = SimpleNamespace(adapter=SimpleNamespace(get_name=lambda: "OneBot V11" if onebot else "QQ"))
            config = SimpleNamespace(markdown_status=markdown, markdown_id="", markdown_id2="", markdown_button_status=False, at_sender=False, message_optimization=False, img=False)
            plain = AsyncMock()
            markdown_send = AsyncMock(side_effect=RuntimeError("isolated removal delivery failure") if send_fails else None)
            with patch.object(utils, "XiuConfig", return_value=config), patch.object(utils, "get_chat_scene", return_value="group"), patch.object(utils, "is_group_event", return_value=True), patch.object(utils, "get_real_id_async", new=AsyncMock(return_value="")), patch.object(utils, "_send_event_message", plain), patch.object(utils, "_send_markdown_or_keyboard", markdown_send):
                await utils.handle_send(bot, event, "成员关系回执合同", md_type="宗门", k1="踢出", v1="宗门踢出")
            if onebot or not markdown or send_fails:
                plain.assert_awaited_once()
            else:
                markdown_send.assert_awaited_once()
            contracts += 1
        probe.evidence.update(sect_id=sid, replay=replay, adapter_contracts=contracts)

    @case("sect-leave-replay", "退出宗门", "同门成员成功/同消息重放", "真实同宗成员（非宗主）",
          "成员实际脱离宗门；成员已无宗门时重放仍读取历史回执", group="sect-member-removal-parameters")
    async def leave_replay(probe):
        sid = seed_sect(probe)
        database = probe.context.paths.game_db
        text = await probe.command("退出宗门", "退出宗门", actor="900002", number=960080)
        assert "已退出" in text and probe.rows(database, "SELECT sect_id,sect_position,sect_contribution FROM user_xiuxian WHERE user_id='900002'") == [(None, None, 0)], text
        probe.clear_cooldowns()
        replay = await probe.command("退出宗门", "退出宗门", actor="900002", number=960080)
        assert "已经完成" in replay and "已退出" not in replay, replay
        probe.evidence.update(sect_id=sid, replay=replay)

    @case("sect-kick-recovery", "踢出宗门", "成员写入 trigger 改写后同消息恢复", "真实宗主与同宗成员",
          "具体成员写拒绝、成员关系与回执回滚；解除故障后同消息成功", group="sect-member-removal-recovery", outcome="rejection")
    async def kick_recovery(probe):
        sid = seed_sect(probe)
        database = probe.context.paths.game_db
        probe.seed(database, "CREATE TRIGGER sect_kick_member_rewrite AFTER UPDATE OF sect_id ON user_xiuxian WHEN NEW.user_id='900002' AND NEW.sect_id IS NULL BEGIN UPDATE user_xiuxian SET sect_contribution=7 WHERE user_id='900002'; END")
        first = await probe.command("踢出宗门", "踢出宗门 诊断对手", number=960090)
        assert "成员关系保存失败" in first, first
        assert probe.rows(database, "SELECT sect_id,sect_position,sect_contribution FROM user_xiuxian WHERE user_id='900002'") == [(sid, 12, 0)]
        probe.seed(database, "DROP TRIGGER sect_kick_member_rewrite")
        probe.clear_cooldowns()
        recovered = await probe.command("踢出宗门", "踢出宗门 诊断对手", number=960090)
        assert "被" in recovered and probe.rows(database, "SELECT sect_id FROM user_xiuxian WHERE user_id='900002'") == [(None,)], recovered
        probe.evidence.update(sect_id=sid, reason="sect_kick_member_write_rejected", recovered=True)

    @case("sect-leave-recovery", "退出宗门", "回执 IGNORE 后同消息恢复", "真实同宗成员（非宗主）",
          "具体回执拒绝、成员关系与回执回滚；解除故障后同消息成功", group="sect-member-removal-recovery", outcome="rejection")
    async def leave_recovery(probe):
        sid = seed_sect(probe)
        database = probe.context.paths.game_db
        probe.seed(database, "CREATE TRIGGER sect_leave_receipt_ignore BEFORE INSERT ON sect_member_removal_operations BEGIN SELECT RAISE(IGNORE); END")
        first = await probe.command("退出宗门", "退出宗门", actor="900002", number=960100)
        assert "退出宗门回执保存失败" in first, first
        assert probe.rows(database, "SELECT sect_id,sect_position,sect_contribution FROM user_xiuxian WHERE user_id='900002'") == [(sid, 12, 0)]
        probe.seed(database, "DROP TRIGGER sect_leave_receipt_ignore")
        probe.clear_cooldowns()
        recovered = await probe.command("退出宗门", "退出宗门", actor="900002", number=960100)
        assert "已退出" in recovered and probe.rows(database, "SELECT sect_id FROM user_xiuxian WHERE user_id='900002'") == [(None,)], recovered
        probe.evidence.update(sect_id=sid, reason="sect_leave_receipt_write_rejected", recovered=True)

    @case("sect-rename-valid-name", "宗门改名", "合法中文新名称", "真实宗主与充足改名费用",
          "宗门名称持久化，扣除真实配置费用或道具", group="sect-body-parameters")
    async def rename(probe):
        sid = seed_sect(probe)
        module = sect_module()
        probe.seed(probe.context.paths.game_db, "UPDATE sects SET sect_used_stone=? WHERE sect_id=?",
                   (module.XiuConfig().sect_rename_cost, sid))
        probe.grant_item(module.SECT_RENAME_CARD_ID, 1)
        probe.seed(probe.context.paths.game_db, "UPDATE back SET bind_num=1 WHERE user_id='900001' AND goods_id=?",
                   (module.SECT_RENAME_CARD_ID,))
        text = await probe.command("宗门改名", "宗门改名 参数新宗")
        assert probe.rows(probe.context.paths.game_db, "SELECT sect_name FROM sects WHERE sect_id=?", (sid,)) == [("参数新宗",)], text
        probe.evidence.update(sect_id=sid, name="参数新宗")

    @case("sect-rename-replay-conflict", "宗门改名", "成功后同消息/改参重放", "真实宗主、改名费用和易名符",
          "同消息提示已完成且不重复扣费，改参保留操作冲突原因", group="sect-rename-parameters")
    async def rename_replay(probe):
        sid = seed_sect(probe)
        module = sect_module()
        cost = module.XiuConfig().sect_rename_cost
        probe.seed(probe.context.paths.game_db, "UPDATE sects SET sect_used_stone=? WHERE sect_id=?", (cost, sid))
        probe.grant_item(module.SECT_RENAME_CARD_ID, 1)
        probe.seed(probe.context.paths.game_db, "UPDATE back SET bind_num=1 WHERE user_id='900001' AND goods_id=?", (module.SECT_RENAME_CARD_ID,))
        text = await probe.command("宗门改名", "宗门改名 参数新宗", number=960010)
        assert "宗门改名为参数新宗" in text, text
        before = probe.rows(probe.context.paths.game_db, "SELECT sect_name,sect_used_stone FROM sects WHERE sect_id=?", (sid,))
        probe.clear_cooldowns()
        replay = await probe.command("宗门改名", "宗门改名 参数新宗", number=960010)
        assert "未重复扣除资源" in replay and "传宗门" not in replay, replay
        probe.clear_cooldowns()
        conflict = await probe.command("宗门改名", "宗门改名 另一个宗门", number=960010)
        assert "该操作号已用于不同内容" in conflict, conflict
        assert probe.rows(probe.context.paths.game_db, "SELECT sect_name,sect_used_stone FROM sects WHERE sect_id=?", (sid,)) == before
        from types import SimpleNamespace
        from unittest.mock import AsyncMock, patch
        from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils import utils
        event = SimpleNamespace(get_user_id=lambda: "900001")
        for onebot, markdown, send_fails in ((True, True, False), (False, False, False), (False, True, False), (False, True, True)):
            bot = SimpleNamespace(adapter=SimpleNamespace(get_name=lambda: "OneBot V11" if onebot else "QQ"))
            config = SimpleNamespace(markdown_status=markdown, markdown_id="", markdown_id2="", markdown_button_status=False, at_sender=False, message_optimization=False, img=False)
            plain = AsyncMock()
            markdown_send = AsyncMock(side_effect=RuntimeError("isolated rename delivery failure") if send_fails else None)
            with patch.object(utils, "XiuConfig", return_value=config), patch.object(utils, "get_chat_scene", return_value="group"), patch.object(utils, "is_group_event", return_value=True), patch.object(utils, "get_real_id_async", new=AsyncMock(return_value="")), patch.object(utils, "_send_event_message", plain), patch.object(utils, "_send_markdown_or_keyboard", markdown_send):
                await utils.handle_send(bot, event, "改名回执合同", md_type="宗门", k1="宗门", v1="我的宗门")
            if onebot or not markdown or send_fails:
                plain.assert_awaited_once()
                assert plain.call_args.kwargs["message"] == "改名回执合同"
            else:
                markdown_send.assert_awaited_once()
        probe.evidence.update(replay=replay, conflict=conflict, unchanged=True, adapter_contracts=4)

    @case("sect-rename-boundaries", "宗门改名", "空白/超长/同名/费用/道具边界", "真实宗主、改名费用和易名符",
          "拒绝参数与资源边界且名称、储备、道具不变", group="sect-rename-parameters", outcome="rejection")
    async def rename_boundaries(probe):
        sid = seed_sect(probe)
        module = sect_module()
        cost = module.XiuConfig().sect_rename_cost
        database = probe.context.paths.game_db
        probe.seed(database, "UPDATE sects SET sect_used_stone=? WHERE sect_id=?", (cost, sid))
        probe.grant_item(module.SECT_RENAME_CARD_ID, 1)
        probe.seed(database, "UPDATE back SET bind_num=1 WHERE user_id='900001' AND goods_id=?", (module.SECT_RENAME_CARD_ID,))
        before = (
            probe.rows(database, "SELECT sect_name,sect_used_stone FROM sects WHERE sect_id=?", (sid,)),
            probe.rows(database, "SELECT goods_num,bind_num FROM back WHERE user_id='900001' AND goods_id=?", (module.SECT_RENAME_CARD_ID,)),
        )
        for index, value in enumerate(("", "宗" * 11, "参数测试宗"), start=1):
            probe.clear_cooldowns()
            text = await probe.command("宗门改名", "宗门改名" + (" " + value if value else ""), number=960030 + index)
            assert probe.rows(database, "SELECT sect_name,sect_used_stone FROM sects WHERE sect_id=?", (sid,)) == before[0], text
            assert probe.rows(database, "SELECT goods_num,bind_num FROM back WHERE user_id='900001' AND goods_id=?", (module.SECT_RENAME_CARD_ID,)) == before[1], text
        probe.seed(database, "UPDATE sects SET sect_used_stone=? WHERE sect_id=?", (cost - 1, sid))
        probe.clear_cooldowns()
        stone_text = await probe.command("宗门改名", "宗门改名 费用不足宗", number=960034)
        assert "灵石储备不足" in stone_text, stone_text
        probe.seed(database, "UPDATE sects SET sect_used_stone=? WHERE sect_id=?", (cost, sid))
        probe.seed(database, "UPDATE back SET goods_num=0,bind_num=0 WHERE user_id='900001' AND goods_id=?", (module.SECT_RENAME_CARD_ID,))
        probe.clear_cooldowns()
        card_text = await probe.command("宗门改名", "宗门改名 缺符宗", number=960035)
        assert "需要消耗1个" in card_text, card_text
        probe.evidence.update(unchanged=True, stone_rejection=stone_text, card_rejection=card_text)

    for index, (table, verb, reason) in enumerate((
        ("sects", "UPDATE", "宗门名称与储备保存失败"),
        ("back", "UPDATE", "易名符消耗保存失败"),
        ("sect_rename_operations", "INSERT", "改名回执保存失败"),
    ), start=1):
        def build(table, verb, reason, index):
            async def run(probe):
                sid = seed_sect(probe)
                module = sect_module()
                cost = module.XiuConfig().sect_rename_cost
                database = probe.context.paths.game_db
                probe.seed(database, "UPDATE sects SET sect_used_stone=? WHERE sect_id=?", (cost, sid))
                probe.grant_item(module.SECT_RENAME_CARD_ID, 1)
                probe.seed(database, "UPDATE back SET bind_num=1 WHERE user_id='900001' AND goods_id=?", (module.SECT_RENAME_CARD_ID,))
                before = (
                    probe.rows(database, "SELECT sect_name,sect_used_stone FROM sects WHERE sect_id=?", (sid,)),
                    probe.rows(database, "SELECT goods_num,bind_num FROM back WHERE user_id='900001' AND goods_id=?", (module.SECT_RENAME_CARD_ID,)),
                )
                probe.seed(database, f"CREATE TRIGGER refuse_rename BEFORE {verb} ON {table} BEGIN SELECT RAISE(IGNORE); END")
                failed = await probe.command("宗门改名", "宗门改名 参数恢复宗", number=960020 + index)
                assert reason in failed, failed
                assert probe.rows(database, "SELECT sect_name,sect_used_stone FROM sects WHERE sect_id=?", (sid,)) == before[0]
                assert probe.rows(database, "SELECT goods_num,bind_num FROM back WHERE user_id='900001' AND goods_id=?", (module.SECT_RENAME_CARD_ID,)) == before[1]
                assert not probe.rows(database, "SELECT * FROM sect_rename_operations")
                probe.seed(database, "DROP TRIGGER refuse_rename")
                probe.clear_cooldowns()
                recovered = await probe.command("宗门改名", "宗门改名 参数恢复宗", number=960020 + index)
                assert "宗门改名为参数恢复宗" in recovered, recovered
                assert probe.rows(database, "SELECT COUNT(*) FROM sect_rename_operations")[0][0] == 1
                probe.evidence.update(rejected_table=table, complete_rollback=True, recovered=True, failure=failed)
            return run
        case("sect-rename-recovery-" + table, "宗门改名", "写入拒绝后恢复", "真实宗主、费用、易名符和SQLite触发器",
             "具体原因、资产整笔回滚，解除后同消息成功", group="sect-rename-recovery", outcome="rejection")(build(table, verb, reason, index))

    for table in ("user_xiuxian", "sects", "sect_donation_operations"):
        def build(table):
            async def run(probe):
                seed_sect(probe)
                database = probe.context.paths.game_db
                before, sect = balances(probe), sect_rows(probe)
                verb = "INSERT" if table == "sect_donation_operations" else "UPDATE"
                probe.seed(database, f"CREATE TRIGGER refuse_donation BEFORE {verb} ON {table} BEGIN SELECT RAISE(IGNORE); END")
                text = await probe.command("宗门捐献", "宗门捐献 1000")
                assert balances(probe) == before and sect_rows(probe) == sect, text
                assert not probe.rows(database, "SELECT * FROM sect_donation_operations")
                assert any(token in text for token in ("捐献扣款与个人贡献保存失败", "宗门捐献储备与资材保存失败", "宗门捐献回执保存失败")), text
                probe.seed(database, "DROP TRIGGER refuse_donation")
                recovered = await probe.command("宗门捐献", "宗门捐献 1000")
                assert "捐献灵石1000枚" in recovered, recovered
                assert balances(probe)[0][1] == before[0][1] - 1000 and probe.rows(database, "SELECT COUNT(*) FROM sect_donation_operations")[0][0] == 1
                probe.evidence.update(rejected_write=table, complete_rollback=True, recovered=True, failure=text)
            return run
        case("sect-donate-rollback-" + table, "宗门捐献", "1000，逐写拒绝", "合法成员与资源，真实SQLite触发器拒绝一处写入",
             "具体写入原因、玩家/宗门/回执完整回滚", group="sect-donation-recovery", outcome="rejection")(build(table))

    for label, value in (("zero", "0"), ("negative", "-2"), ("decimal", "1.5"), ("extra", "100 extra"),
                         ("huge", "9" * 350), ("missing", "")):
        def build(value):
            async def run(probe):
                seed_sect(probe)
                before, sect = balances(probe), sect_rows(probe)
                text = await probe.command("宗门捐献", "宗门捐献 " + value)
                assert balances(probe) == before and sect_rows(probe) == sect, (text, before, balances(probe))
                probe.evidence.update(wallet_unchanged=True, sect_unchanged=True)
            return run
        case("sect-donate-reject-" + label, "宗门捐献", label, "真实宗门且充分灵石",
             "参数或资源拒绝，玩家和宗门资产不变", group="sect-donation-parameters", outcome="rejection")(build(value))

    @case("body-stone-valid-count", "灵石炼体", "灵石正整数数量1000", "真实炼体档案、未达上限和充分灵石",
          "实际扣1000灵石、炼体气血增加、永久回执且重放不重复", group="sect-body-parameters")
    async def stone_body(probe):
        from nonebot_plugin_xiuxian_2.features.tianti_training.repository import TiantiBreakthroughSqlRepository
        TiantiBreakthroughSqlRepository(probe.context.paths.player_db).ensure_profile("900001")
        sql = "SELECT tianti_hp FROM tianti_info WHERE user_id='900001'"
        before = probe.rows(probe.context.paths.player_db, sql)
        text = await probe.command("灵石炼体", "灵石炼体 1000", number=960002)
        after = probe.rows(probe.context.paths.player_db, sql)
        assert after[0][0] > before[0][0] and balances(probe)[0][1] == 999000, (text, before, after)
        probe.clear_cooldowns()
        await probe.command("灵石炼体", "灵石炼体 1000", number=960002)
        assert probe.rows(probe.context.paths.player_db, sql) == after and balances(probe)[0][1] == 999000
        probe.evidence.update(before=before, after=after, spent=1000, replay_unchanged=True)

    @case("body-bath-herb-combination", "炼体药浴", "当前时段真实药材名x数量组合", "当前时窗有效的合法药材、背包库存、炼体档案",
          "消耗指定药材且实际药浴上下文和回执持久化", group="sect-body-parameters")
    async def bath(probe):
        from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_tianti as module
        from nonebot_plugin_xiuxian_2.features.tianti_training.repository import TiantiBreakthroughSqlRepository
        TiantiBreakthroughSqlRepository(probe.context.paths.player_db).ensure_profile("900001")
        slot = module._medicine_bath_slot(module.runtime_clock.now().hour)
        name = sorted(slot["herbs"])[0]
        item_id, definition = module.items.get_data_by_item_name(name)
        probe.grant_item(item_id, 20)
        text = await probe.command("炼体药浴", f"炼体药浴 {name}x2")
        assert probe.managers[0].goods_num("900001", item_id) == 18, text
        state = probe.rows(probe.context.paths.player_db, "SELECT * FROM tianti_info WHERE user_id='900001'")
        probe.evidence.update(item_id=item_id, name=name, consumed=2, profile=state)
