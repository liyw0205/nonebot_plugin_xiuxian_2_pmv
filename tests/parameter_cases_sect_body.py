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
          "扣款、贡献/建设/资材增加和回执持久化，重放不重复", group="sect-body-parameters")
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
        await probe.command("宗门捐献", "宗门捐献 1000", number=960001)
        assert balances(probe) == after and probe.rows(database, "SELECT sect_scale,sect_materials FROM sects WHERE sect_id=?", (sid,)) == saved
        probe.evidence.update(sect_id=sid, before=prior, after=saved, wallet_delta=-1000, replay_unchanged=True)

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
                probe.evidence.update(rejected_write=table, complete_rollback=True)
            return run
        case("sect-donate-rollback-" + table, "宗门捐献", "1000，逐写拒绝", "合法成员与资源，真实SQLite触发器拒绝一处写入",
             "具体写入原因、玩家/宗门/回执完整回滚", group="sect-body-recovery", outcome="rejection")(build(table))

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
             "参数或资源拒绝，玩家和宗门资产不变", group="sect-body-parameters", outcome="rejection")(build(value))

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
