"""Bank subcommands and actual multi-argument market consumers."""
from tests.parameter_cases_assets import balances, backpack


def trade_module():
    from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_trade
    return xiuxian_trade


def herb(probe, uid="900001"):
    module = trade_module()
    item_id = int(next(iter(module.items.get_data_by_item_type("药材"))))
    return item_id, probe.grant_item(item_id, 10, uid)


async def listing(probe):
    probe.player(stone=100_000_000)
    probe.player("900002", stone=100_000_000)
    item_id, name = herb(probe, "900002")
    module = trade_module()
    text = await probe.command("仙肆上架", f"仙肆上架 {name} {module.MIN_PRICE} 3", actor="900002")
    rows = module.trade_application.xianshi_get_items()
    assert len(rows) == 3 and all(row["name"] == name and row["quantity"] == 1 for row in rows), (text, rows)
    assert probe.managers[0].goods_num("900002", item_id) == 7
    return item_id, name, rows[0]


def register(case):
    @case("bank-subcommands-success", "灵庄", "存灵石 金额 → 取灵石 金额 → 信息/结算", "真实启动银行迁移，1000000灵石无银行账户",
          "存1000/取400、真实钱包与银行余额/回执一致，重放不重复", group="market-parameters")
    async def bank_chain(probe):
        database = probe.context.paths.game_db
        text = await probe.command("灵庄", "灵庄 存灵石 1000", number=950001)
        assert probe.rows(database, "SELECT saved_stone FROM bank_accounts WHERE user_id='900001'") == [(1000,)], text
        assert balances(probe)[0][1] == 999000, text
        await probe.command("灵庄", "灵庄 存灵石 1000", number=950001)
        assert balances(probe)[0][1] == 999000
        text = await probe.command("灵庄", "灵庄取灵石 400")
        assert balances(probe)[0][1] == 999400, text
        assert probe.rows(database, "SELECT saved_stone FROM bank_accounts WHERE user_id='900001'") == [(600,)], text
        text = await probe.command("灵庄", "灵庄信息")
        assert "600" in text, text
        text = await probe.command("灵庄", "灵庄结算")
        assert balances(probe)[0][1] == 999400 and "结息成功" in text, text
        probe.evidence.update(wallet=999400, savings=600, deposit_replayed=True)

    for label, value in (("zero", "0"), ("negative", "-2"), ("decimal", "1.5"), ("extra", "100 extra"),
                         ("huge", "9" * 350), ("missing", "")):
        def build(value):
            async def run(probe):
                before = balances(probe)
                text = await probe.command("灵庄", "灵庄 存灵石 " + value)
                assert before == balances(probe), text
                assert not probe.rows(probe.context.paths.game_db, "SELECT * FROM bank_accounts")
                assert "正确" in text or "正整数" in text, text
                probe.evidence.update(wallet_unchanged=True, no_bank_account=True)
            return run
        case("bank-deposit-reject-" + label, "灵庄", "存灵石 " + label, "足够灵石、无银行账户",
             "金额格式拒绝，不创建账户或扣款", group="market-parameters", outcome="rejection")(build(value))

    @case("market-list-buy-id-count", "仙肆购买", "真实单件挂单ID 数量1", "真实上架生成3个单件挂单，买方足够灵石",
          "买1件，买方扣款/卖方入账/库存/挂单与回执一致，售罄重放不重复", group="market-parameters")
    async def buy(probe):
        item_id, name, offer = await listing(probe)
        before = balances(probe)
        text = await probe.command("仙肆购买", f"仙肆购买 {offer['id']} 1", number=950002)
        after = balances(probe)
        assert before[0][1] - after[0][1] == offer["price"], (text, before, after)
        assert after[1][1] - before[1][1] == offer["price"]
        assert probe.managers[0].goods_num("900001", item_id) == 1
        await probe.command("仙肆购买", f"仙肆购买 {offer['id']} 1", number=950002)
        assert balances(probe) == after and probe.managers[0].goods_num("900001", item_id) == 1
        probe.evidence.update(listing_id=offer["id"], item_id=item_id, price=offer["price"], before=before, after=after, replay_unchanged=True)

    @case("market-fast-buy-multiple-listings", "仙肆快速购买", "实际物品名 数量2", "卖家3个同名真实挂单、买方充足灵石和体力",
          "跨2个挂单购买2件，只扣一次10体力，售罄后事件重放不多买剩余挂单", group="market-fast-parameters")
    async def fast_buy(probe):
        item_id, name, offer = await listing(probe)
        module = trade_module()
        before = balances(probe)
        stamina = probe.rows(probe.context.paths.game_db, "SELECT user_stamina FROM user_xiuxian WHERE user_id='900001'")
        text = await probe.command("仙肆快速购买", f"仙肆快速购买 {name} 2", number=950003)
        after = balances(probe)
        assert before[0][1] - after[0][1] == 2 * offer["price"], text
        assert probe.managers[0].goods_num("900001", item_id) == 2
        assert len(module.trade_application.xianshi_get_items()) == 1
        saved = probe.rows(probe.context.paths.game_db, "SELECT user_stamina FROM user_xiuxian WHERE user_id='900001'")
        assert stamina[0][0] - saved[0][0] == 10, (stamina, saved)
        await probe.command("仙肆快速购买", f"仙肆快速购买 {name} 2", number=950003)
        assert balances(probe) == after and probe.managers[0].goods_num("900001", item_id) == 2
        probe.evidence.update(count=2, stamina_cost=10, replay_unchanged=True)

    @case("market-fast-resume-restart", "仙肆快速购买", "实际物品名 数量2", "候选快照和首件购买已提交，重建应用模拟中断恢复",
          "恢复只再买1件，总共扣2件钱/10体力，篡改同事件数量具体拒绝", group="market-fast-recovery")
    async def fast_resume(probe):
        from nonebot_plugin_xiuxian_2.features.trade.application import TradeApplication
        from unittest.mock import patch
        item_id, name, offer = await listing(probe)
        m = trade_module()
        before = balances(probe)
        batch_id = "xianshi-fast-buy-stamina:950004:900001"
        plan = m.trade_application.xianshi_fast_plan(operation_id=batch_id, user_id="900001", names=[name], quantities=[2])
        first = plan[0]
        applied = m.trade_application.purchase(
            operation_id=f"xianshi-buy:950004:{first['id']}:0:0", user_id="900001", listing_id=first["id"],
            quantity=1, stamina_operation_id=batch_id, stamina_cost=10, max_goods_num=1000,
        )
        assert applied.ok
        restarted = TradeApplication(probe.context.paths.game_db, probe.context.paths.trade_db)
        with patch.object(m, "trade_application", restarted):
            text = await probe.command("仙肆快速购买", f"仙肆快速购买 {name} 2", number=950004)
            assert probe.managers[0].goods_num("900001", item_id) == 2, text
            assert before[0][1] - balances(probe)[0][1] == 2 * offer["price"]
            assert probe.rows(probe.context.paths.game_db, "SELECT user_stamina FROM user_xiuxian WHERE user_id='900001'") == [(90,)]
            after, stock = balances(probe), backpack(probe)
            text = await probe.command("仙肆快速购买", f"仙肆快速购买 {name} 3", number=950004)
            assert "请求" in text and "不同内容" in text, text
            assert after == balances(probe) and stock == backpack(probe)
        probe.evidence.update(resumed_count=1, total_count=2, total_stamina=10, altered_request_rejected=True)

    for label, value in (("zero", "0"), ("negative", "-2"), ("bad-count", "abc"),
                         ("extra", "1 extra"), ("excess-list", "1,1"), ("huge-price", "9" * 350)):
        def build(label, value):
            async def run(probe):
                item_id, name, offer = await listing(probe)
                before, stock = balances(probe), backpack(probe)
                offers = trade_module().trade_application.xianshi_get_items()
                command = "仙肆上架" if label == "huge-price" else "仙肆快速购买"
                args = f"{name} {value} 1" if label == "huge-price" else f"{name} {value}"
                text = await probe.command(command, command + " " + args)
                assert before == balances(probe) and stock == backpack(probe), text
                assert offers == trade_module().trade_application.xianshi_get_items(), text
                probe.evidence.update(assets_unchanged=True, listings_unchanged=True)
            return run
        case("market-fast-reject-" + label, "仙肆上架" if label == "huge-price" else "仙肆快速购买",
             "实际物品 " + label, "真实挂单且充足资源", "拒绝非法参数或不够巨额手续费，不写资产/挂单",
             group="market-fast-parameters", outcome="rejection")(build(label, value))

    @case("market-remove-name-count", "仙肆下架", "实际物品名 数量2", "卖家有真实3件挂单与库存空间",
          "只下架2件并退回原卖家，剩1件挂单", group="market-parameters")
    async def remove(probe):
        item_id, name, offer = await listing(probe)
        text = await probe.command("仙肆下架", f"仙肆下架 {name} 2", actor="900002")
        assert probe.managers[0].goods_num("900002", item_id) == 9, text
        rows = trade_module().trade_application.xianshi_get_items()
        assert len(rows) == 1 and rows[0]["quantity"] == 1, rows
        probe.evidence.update(item_id=item_id, remaining_listing=1, seller_stock=9)

    for command in ("仙肆查看", "我的仙肆", "仙肆行情"):
        def build(command):
            async def run(probe):
                item_id, name, offer = await listing(probe)
                before, stock = balances(probe), backpack(probe)
                argument = "药材 " + name + " 1" if command == "仙肆查看" else name + " 1" if command == "我的仙肆" else name
                text = await probe.command(command, command + " " + argument, actor="900002")
                assert name in text, text
                assert balances(probe) == before and backpack(probe) == stock
                probe.evidence.update(listing_id=offer["id"], readonly=True)
            return run
        case("market-read-" + command, command, "实际类型/物品关键词/页码", "真实上架物品作为可筛选数据",
             "显示实际挂单或成交行情，资产不变", group="market-parameters")(build(command))

    @case("market-watch-add-remove", "仙肆关注", "真实物品名 → 取消 物品名", "注册玩家、合法物品",
          "关注持久化并能取消，行情反映关注状态", group="market-parameters")
    async def watch(probe):
        item_id, name = herb(probe)
        module = trade_module()
        text = await probe.command("仙肆关注", "仙肆关注 " + name)
        assert name in module.xianshi_market_application.names("900001"), text
        text = await probe.command("仙肆关注", "仙肆关注 取消 " + name)
        assert not module.xianshi_market_application.names("900001"), text
        probe.evidence.update(item_id=item_id, persisted_add_remove=True)

    for command in ("仙肆购买", "仙肆上架"):
        for label, value in (("zero", "0"), ("negative", "-2"), ("bad-count", "abc"), ("extra", "1 extra"), ("huge", "9" * 350)):
            def build(command, value):
                async def run(probe):
                    item_id, name, offer = await listing(probe)
                    before, stock = balances(probe), backpack(probe)
                    offers = trade_module().trade_application.xianshi_get_items()
                    args = f"{offer['id']} {value}" if command == "仙肆购买" else f"{name} {trade_module().MIN_PRICE} {value}"
                    text = await probe.command(command, command + " " + args, actor="900001" if command == "仙肆购买" else "900002")
                    assert before == balances(probe) and stock == backpack(probe), (text, before, balances(probe))
                    assert offers == trade_module().trade_application.xianshi_get_items(), text
                    if "extra" in value:
                        assert "格式" in text, text
                    probe.evidence.update(assets_unchanged=True, inventory_unchanged=True, listings_unchanged=True)
                return run
            case("market-" + command + "-reject-" + label, command, "实际对象 " + label,
                 "实际挂单、真实库存和充分资源，不能以无对象遮挡错误参数", "明确参数/数量拒绝，资产/库存/挂单不变",
                 group="market-parameters", outcome="rejection")(build(command, value))
