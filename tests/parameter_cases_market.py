"""Bank subcommands and actual multi-argument market consumers."""
import asyncio
from datetime import datetime, timezone
import json
from unittest.mock import patch

from tests.parameter_cases_assets import balances, backpack


def trade_module():
    from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_trade
    return xiuxian_trade


class FixedClock:
    def __init__(self, moment):
        self.moment = moment

    def now(self):
        return self.moment


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
    from tests.parameter_cases_guishi import register as register_guishi
    register_guishi(case)

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

    @case(
        "guishi-order-match-take-real",
        "鬼市求购",
        "物品名 价格 数量 -> 摆摊撮合 -> 周末取回",
        "买卖双方有真实钱包/背包，鬼市账户由命令创建，固定到本地周六21:00",
        "求购冻结灵石、摆摊即时撮合、物品暂存、取回和重复消息均落隔离SQLite",
        group="market-guishi-parameters",
    )
    async def guishi_match_take(probe):
        module = trade_module()
        item_id, name = herb(probe, "900002")
        probe.player(stone=100_000_000)
        probe.player("900002", stone=100_000_000)
        price = module.MIN_PRICE * 10
        database = probe.context.paths.trade_db
        with patch.object(
            module,
            "runtime_clock",
            FixedClock(datetime(2026, 11, 21, 13, 0, tzinfo=timezone.utc)),
        ):
            text = await probe.command("鬼市存灵石", "鬼市存灵石 20000000", number=960001)
            assert "成功存入" in text, text
            text = await probe.command(
                "鬼市求购", f"鬼市求购 {name} {price} 2", number=960002
            )
            assert "成功发布求购订单" in text, text
            order = probe.rows(
                database,
                "SELECT id,quantity,filled_quantity FROM guishi_item "
                "WHERE user_id='900001' AND item_type='qiugou'",
            )
            assert len(order) == 1 and order[0][1:] == (2, 0), (text, order)
            text = await probe.command(
                "鬼市摆摊", f"鬼市摆摊 {name} {price} 2", actor="900002", number=960003
            )
            assert "成功摆摊" in text and "求购成交" in text, text
            buyer_account = probe.rows(
                database, "SELECT stored_stone,items FROM guishi_info WHERE user_id='900001'"
            )[0]
            seller_account = probe.rows(
                database, "SELECT stored_stone,items FROM guishi_info WHERE user_id='900002'"
            )[0]
            assert buyer_account[0] == 8_000_000
            assert json.loads(buyer_account[1]) == {str(item_id): 2}
            assert seller_account[0] == 12_000_000
            assert probe.rows(database, "SELECT COUNT(*) FROM guishi_item")[0][0] == 0
            assert probe.managers[0].goods_num("900002", item_id) == 8
            text = await probe.command("鬼市取物品", f"鬼市取物品 {name}", number=960004)
            assert "成功取出" in text, text
            assert probe.managers[0].goods_num("900001", item_id) == 2
            account = probe.rows(
                database, "SELECT stored_stone,items FROM guishi_info WHERE user_id='900001'"
            )[0]
            assert account[0] == 8_000_000 and json.loads(account[1]) == {}
            await probe.command("鬼市取物品", f"鬼市取物品 {name}", number=960004)
            assert probe.managers[0].goods_num("900001", item_id) == 2
            assert probe.rows(
                probe.context.paths.game_db,
                "SELECT COUNT(*) FROM guishi_take_item_operations",
            )[0][0] == 1
        probe.evidence.update(
            item_id=item_id,
            price=price,
            buyer_stone=8_000_000,
            seller_stone=12_000_000,
            matched_quantity=2,
            take_replayed=True,
        )

    @case(
        "guishi-order-cancel-refund-real",
        "鬼市取消求购",
        "订单ID -> 重复取消",
        "隔离鬼市账户有足够冻结灵石且订单未撮合",
        "取消只退未成交金额，删除订单并写操作回执，重复请求不重复退款",
        group="market-guishi-parameters",
    )
    async def guishi_cancel_refund(probe):
        module = trade_module()
        item_id, name = herb(probe)
        probe.player(stone=100_000_000)
        price = module.MIN_PRICE * 10
        database = probe.context.paths.trade_db
        text = await probe.command("鬼市存灵石", "鬼市存灵石 20000000", number=961001)
        assert "成功存入" in text, text
        await probe.command("鬼市求购", f"鬼市求购 {name} {price} 2", number=961002)
        order_id = probe.rows(
            database,
            "SELECT id FROM guishi_item WHERE user_id='900001' AND item_type='qiugou'",
        )[0][0]
        frozen = probe.rows(
            database, "SELECT stored_stone FROM guishi_info WHERE user_id='900001'"
        )[0][0]
        text = await probe.command(
            "鬼市取消求购", f"鬼市取消求购 {order_id}", number=961003
        )
        assert "成功取消求购" in text and "1200万" in text, text
        assert probe.rows(database, "SELECT COUNT(*) FROM guishi_item")[0][0] == 0
        assert probe.rows(
            database, "SELECT stored_stone FROM guishi_info WHERE user_id='900001'"
        )[0][0] == frozen + price * 2
        await probe.command("鬼市取消求购", f"鬼市取消求购 {order_id}", number=961003)
        assert probe.rows(
            database, "SELECT stored_stone FROM guishi_info WHERE user_id='900001'"
        )[0][0] == frozen + price * 2
        assert probe.rows(database, "SELECT COUNT(*) FROM guishi_order_cancel_operations")[0][0] == 1
        probe.evidence.update(order_id=order_id, refunded=price * 2, replay_unchanged=True)

    @case(
        "guishi-parameter-boundaries",
        "鬼市求购",
        "空白/最低价/非法数字/多余参数",
        "真实物品、背包和足额鬼市账户，所有非法参数必须在订单写入前拒绝",
        "格式、价格和参数数量拒绝，钱包、背包、订单和操作回执零写",
        group="market-guishi-parameters",
        outcome="rejection",
    )
    async def guishi_parameter_boundaries(probe):
        module = trade_module()
        _, name = herb(probe)
        price = module.MIN_PRICE * 10
        probe.player(stone=100_000_000)
        await probe.command("鬼市存灵石", "鬼市存灵石 20000000", number=962200)
        before = balances(probe), backpack(probe)
        database = probe.context.paths.trade_db
        accounts = probe.rows(database, "SELECT * FROM guishi_info ORDER BY user_id")
        inputs = (
            ("鬼市求购", "指令格式"),
            (f"鬼市求购 {name} {price - 1}", "价格过低"),
            (f"鬼市求购 {name} abc", "有效的价格"),
            (f"鬼市求购 {name} {price} 1 extra", "指令格式"),
        )
        for index, (text, marker) in enumerate(inputs, 962000):
            reply = await probe.command("鬼市求购", text, number=index)
            assert marker in reply, (text, reply)
        with patch.object(
            module,
            "runtime_clock",
            FixedClock(datetime(2026, 11, 21, 13, 0, tzinfo=timezone.utc)),
        ):
            reply = await probe.command(
                "鬼市摆摊",
                f"鬼市摆摊 {name} {price} 1 extra",
                actor="900001",
                number=962100,
            )
            assert "指令格式" in reply, reply
        assert before == (balances(probe), backpack(probe))
        assert accounts == probe.rows(database, "SELECT * FROM guishi_info ORDER BY user_id")
        assert probe.rows(database, "SELECT COUNT(*) FROM guishi_item")[0][0] == 0
        assert probe.rows(database, "SELECT COUNT(*) FROM guishi_order_create_operations")[0][0] == 0
        probe.evidence.update(inputs=len(inputs), assets_unchanged=True, no_order_operations=True)

    @case(
        "guishi-stall-scheduled-return-real",
        "鬼市摆摊",
        "本地凌晨01:00摆摊 -> 当天08:00自动退回",
        "真实药材库存、无求购，应用与任务都指向隔离迁移库",
        "任务回调删除未售摊位、原子退回库存并写清理回执，重复执行零额外写入",
        group="market-guishi-parameters",
    )
    async def guishi_stall_scheduled_return(probe):
        from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork
        from nonebot_plugin_xiuxian_2.features.trade.application import TradeApplication

        module = trade_module()
        item_id, name = herb(probe, "900002")
        price = module.MIN_PRICE * 10
        clock = FixedClock(datetime(2026, 11, 21, 17, tzinfo=timezone.utc))
        application = TradeApplication(
            probe.context.paths.game_db, probe.context.paths.trade_db, clock=clock,
            business_timezone=module._trade_scheduler_timezone(),
        )
        with patch.object(module, "runtime_clock", clock), patch.object(module, "trade_application", application):
            text = await probe.command(
                "鬼市摆摊", f"鬼市摆摊  {name}  {price}  3  ", actor="900002", number=963001
            )
            assert "成功摆摊" in text, text
            assert probe.managers[0].goods_num("900002", item_id) == 7
            rows = module.trade_application.guishi_get_orders(user_id="900002", order_type="baitan")
            assert len(rows) == 1 and rows[0]["quantity"] == 3, rows
            text = await probe.command("鬼市信息", actor="900002", number=963002)
            assert "截止：11-22 08:00" in text and "剩余 7小时0分" in text, text
            clock.moment = datetime(2026, 11, 22, 0, tzinfo=timezone.utc)
            await module.clear_expired_baitan_orders_job()
            assert not module.trade_application.guishi_get_orders(user_id="900002", order_type="baitan")
            assert probe.managers[0].goods_num("900002", item_id) == 10
            receipts = probe.rows(
                probe.context.paths.trade_db,
                "SELECT order_id,refunded_quantity FROM guishi_expired_order_operations",
            )
            assert receipts == [(rows[0]["id"], 3)], receipts
            await module.clear_expired_baitan_orders_job()
            assert probe.managers[0].goods_num("900002", item_id) == 10
            assert receipts == probe.rows(
                probe.context.paths.trade_db,
                "SELECT order_id,refunded_quantity FROM guishi_expired_order_operations",
            )
            for database in (probe.context.paths.game_db, probe.context.paths.trade_db):
                with DatabaseUnitOfWork(database, read_only=True) as uow:
                    assert uow.query_all("PRAGMA integrity_check") == [{"integrity_check": "ok"}]
        probe.evidence.update(order_id=rows[0]["id"], returned=3, scheduler_replayed=True)

    @case(
        "guishi-local-deadline-info-sweep-real", "鬼市信息",
        "求购冻结 -> 本地07:59信息 -> 08:00实际调度退款 -> 信息重读",
        "真实命令建单、注入UTC时钟，调度器和应用共用业务时区",
        "未到期零写、到点实际退未成交款且只退一次，创建锚点保持原样",
        group="market-guishi-parameters",
    )
    async def guishi_local_deadline(probe):
        from apscheduler.events import EVENT_JOB_ERROR, EVENT_JOB_EXECUTED
        from apscheduler.schedulers.asyncio import AsyncIOScheduler
        from nonebot_plugin_xiuxian_2.features.trade.application import TradeApplication
        from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork

        module = trade_module()
        _, name = herb(probe)
        probe.player(stone=100_000_000)
        database = probe.context.paths.trade_db
        clock = FixedClock(datetime(2026, 11, 21, 13, tzinfo=timezone.utc))
        application = TradeApplication(
            probe.context.paths.game_db, database, clock=clock,
            business_timezone=module._trade_scheduler_timezone(),
        )
        with patch.object(module, "runtime_clock", clock), patch.object(module, "trade_application", application):
            await probe.command("鬼市存灵石", "鬼市存灵石 20000000", number=964001)
            reply = await probe.command("鬼市求购", f"鬼市求购 {name} {module.MIN_PRICE * 10} 2", number=964002)
            assert "成功发布求购订单" in reply, reply
            anchors = probe.rows(database, "SELECT order_id,created_at FROM guishi_order_create_operations")
            assert len(anchors) == 1 and anchors[0][1] == "2026-11-21T13:00:00+00:00", anchors
            clock.moment = datetime(2026, 11, 21, 23, 59, tzinfo=timezone.utc)
            reply = await probe.command("鬼市信息", number=964003)
            assert "到期：11-22 08:00（剩余 1分）" in reply, reply
            assert probe.rows(database, "SELECT stored_stone FROM guishi_info WHERE user_id='900001'") == [(8_000_000,)]
            assert probe.rows(database, "SELECT COUNT(*) FROM guishi_order_cancel_operations") == [(0,)]

            clock.moment = datetime(2026, 11, 22, 0, tzinfo=timezone.utc)
            scheduler = AsyncIOScheduler(timezone=application.business_timezone)
            outcome = asyncio.get_running_loop().create_future()

            def completed(event):
                if not outcome.done():
                    outcome.set_result(event)

            scheduler.add_listener(completed, EVENT_JOB_EXECUTED | EVENT_JOB_ERROR)
            scheduler.add_job(module.clear_expired_baitan_orders_job, "date", id="clear_expired_baitan_orders")
            try:
                scheduler.start()
                outcome_event = await asyncio.wait_for(outcome, timeout=3)
                assert outcome_event.code == EVENT_JOB_EXECUTED, outcome_event.exception
            finally:
                if scheduler.running:
                    scheduler.shutdown(wait=False)
            assert probe.rows(database, "SELECT stored_stone FROM guishi_info WHERE user_id='900001'") == [(20_000_000,)]
            assert probe.rows(database, "SELECT COUNT(*) FROM guishi_item") == [(0,)]
            receipts = probe.rows(database, "SELECT order_id,refunded_stone FROM guishi_order_cancel_operations")
            assert receipts == [(anchors[0][0], 12_000_000)], receipts
            reply = await probe.command("鬼市信息", number=964004)
            assert "账户余额：2000万" in reply and "求购列表" not in reply, reply
            await module.clear_expired_baitan_orders_job()
            assert probe.rows(database, "SELECT order_id,refunded_stone FROM guishi_order_cancel_operations") == receipts
            assert probe.rows(database, "SELECT order_id,created_at FROM guishi_order_create_operations") == anchors
            for path in (probe.context.paths.game_db, database):
                with DatabaseUnitOfWork(path, read_only=True) as uow:
                    assert uow.query_all("PRAGMA integrity_check") == [{"integrity_check": "ok"}]
        probe.evidence.update(order_id=anchors[0][0], local_deadline="11-22 08:00", refunded=12_000_000, actual_scheduler=True)

    @case(
        "guishi-local-weekend-withdraw-real", "鬼市取灵石",
        "本地周五23:59拒绝 -> 周六00:00取款 -> 周一旧请求回放/新请求拒绝",
        "真实存款和充足余额，命令应用使用注入UTC时钟与调度业务时区",
        "跨日边界不误判，手续费与钱包/账户/回执精确一致，重复请求不重复扣增",
        group="market-guishi-parameters",
    )
    async def guishi_local_weekend_withdraw(probe):
        from nonebot_plugin_xiuxian_2.features.trade.application import TradeApplication

        module = trade_module()
        probe.player(stone=100_000_000)
        database = probe.context.paths.trade_db
        game = probe.context.paths.game_db
        clock = FixedClock(datetime(2026, 11, 20, 15, 59, tzinfo=timezone.utc))
        application = TradeApplication(
            game, database, clock=clock, business_timezone=module._trade_scheduler_timezone(),
        )
        with patch.object(module, "runtime_clock", clock), patch.object(module, "trade_application", application):
            await probe.command("鬼市存灵石", "鬼市存灵石 20000000", number=965001)
            before = balances(probe)
            accounts = probe.rows(database, "SELECT * FROM guishi_info")
            reply = await probe.command("鬼市取灵石", "鬼市取灵石 5000000", number=965002)
            assert "仅在周六和周日" in reply, reply
            assert before == balances(probe) and accounts == probe.rows(database, "SELECT * FROM guishi_info")
            assert probe.rows(game, "SELECT COUNT(*) FROM trade_guishi_withdraw_operations") == [(0,)]
            clock.moment = datetime(2026, 11, 20, 16, tzinfo=timezone.utc)
            reply = await probe.command("鬼市取灵石", "鬼市取灵石 5000000", number=965002)
            assert "成功取出 500万" in reply and "扣除100万" in reply and "实际到账 400万" in reply, reply
            assert probe.rows(game, "SELECT stone FROM user_xiuxian WHERE user_id='900001'") == [(84_000_000,)]
            assert probe.rows(database, "SELECT stored_stone FROM guishi_info WHERE user_id='900001'") == [(15_000_000,)]
            after = balances(probe), probe.rows(database, "SELECT * FROM guishi_info")
            clock.moment = datetime(2026, 11, 22, 16, tzinfo=timezone.utc)
            reply = await probe.command("鬼市取灵石", "鬼市取灵石 5000000", number=965002)
            assert "成功取出 500万" in reply, reply
            reply = await probe.command("鬼市取灵石", "鬼市取灵石 1000000", number=965003)
            assert "仅在周六和周日" in reply, reply
            assert after == (balances(probe), probe.rows(database, "SELECT * FROM guishi_info"))
            assert probe.rows(game, "SELECT amount,fee,actual_amount FROM trade_guishi_withdraw_operations") == [(5_000_000, 1_000_000, 4_000_000)]
        probe.evidence.update(weekend_boundary=True, fee=1_000_000, credited=4_000_000, replay_unchanged=True)
