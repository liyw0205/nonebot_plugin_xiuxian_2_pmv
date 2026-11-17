"""Finite Guishi lifecycle paths using the registered command consumers."""
from datetime import datetime, timezone
import asyncio
from unittest.mock import patch

from tests.parameter_cases_market import FixedClock, herb, trade_module


def state(probe):
    return tuple(probe.rows(probe.context.paths.trade_db, query) for query in (
        "SELECT * FROM guishi_info ORDER BY user_id",
        "SELECT * FROM guishi_item ORDER BY id",
        "SELECT * FROM guishi_order_create_operations ORDER BY operation_id",
        "SELECT * FROM guishi_order_cancel_operations ORDER BY operation_id",
        "SELECT * FROM guishi_immediate_match_requests ORDER BY order_id",
        "SELECT * FROM guishi_match_operations ORDER BY operation_id",
    )) + (
        probe.rows(probe.context.paths.game_db, "SELECT * FROM back ORDER BY user_id,goods_id"),
        probe.rows(probe.context.paths.player_db, "SELECT * FROM statistics ORDER BY user_id"),
    )


def statistics(probe, *keys):
    columns = ",".join('"' + key + '"' for key in keys)
    return probe.rows(probe.context.paths.player_db,
                      "SELECT " + columns + " FROM statistics WHERE user_id='900001'")


def setup(probe):
    from nonebot_plugin_xiuxian_2.features.trade.application import TradeApplication
    module = trade_module()
    clock = FixedClock(datetime(2026, 11, 21, 13, tzinfo=timezone.utc))
    application = TradeApplication(probe.context.paths.game_db, probe.context.paths.trade_db,
                                   clock=clock, business_timezone=module._trade_scheduler_timezone(),
                                   player_database=probe.context.paths.player_db)
    return module, clock, application


def register(case):
    from tests.parameter_cases_guishi_clear import register as register_clear
    register_clear(case)
    from tests.parameter_cases_guishi_return import register as register_return
    register_return(case)
    from tests.parameter_cases_guishi_take import register as register_take
    register_take(case)
    from tests.parameter_cases_guishi_boundaries import register as register_boundaries
    register_boundaries(case)
    from tests.parameter_cases_guishi_recovery import register as register_recovery
    register_recovery(case)
    from tests.parameter_cases_guishi_cutoff import register as register_cutoff
    register_cutoff(case)

    @case("guishi-concurrent-command-reservations", "鬼市摆摊", "同消息并发/不同消息库存竞争",
          "两条真实matcher同时进入handler，10件库存", "同消息只预留一次，不同消息串行扣完剩余库存，不超卖或重复统计",
          group="market-guishi-lifecycle")
    async def concurrent_stalls(probe):
        from tests.command_concurrency import concurrent_commands
        module, clock, application = setup(probe)
        item_id, name = herb(probe)
        price = module.MIN_PRICE * 10
        entry = next(row for row in probe.entries if row["command"] == "鬼市摆摊")
        original = module.assign_bot

        async def pair(quantity, numbers):
            arrived, ready = [], asyncio.Event()
            async def meet(*args, **kwargs):
                arrived.append(kwargs["event"])
                if len(arrived) == 2:
                    ready.set()
                await asyncio.wait_for(ready.wait(), timeout=3)
                return await original(*args, **kwargs)
            text = f"鬼市摆摊 {name} {price} {quantity}"
            with patch.object(module, "assign_bot", meet):
                rows = await concurrent_commands(entry, [(text, "900001", number) for number in numbers])
            assert len(arrived) == 2
            assert all("成功摆摊" in "".join(row["replies"]) for row in rows), rows
            probe.calls.extend({"command": "鬼市摆摊", "status": "pass", "concurrent": True, **row} for row in rows)

        with patch.object(module, "runtime_clock", clock), patch.object(module, "trade_application", application), \
                patch.object(module, "record_trade_event", wraps=module.record_trade_event) as stats:
            await pair(7, [966040, 966040])
            assert probe.managers[0].goods_num("900001", item_id) == 3
            assert stats.call_count == 1, stats.call_args_list
            await pair(2, [966041, 966042])
            assert probe.managers[0].goods_num("900001", item_id) == 0
            assert stats.call_count == 3, stats.call_args_list
            orders = application.guishi_get_orders(user_id="900001", order_type="baitan")
            assert sorted(row["quantity"] for row in orders) == [1, 2, 7], orders
            assert probe.rows(probe.context.paths.trade_db, "SELECT COUNT(*) FROM guishi_order_create_operations") == [(3,)]
            assert statistics(probe, "鬼市摆摊次数", "鬼市摆摊数量") == [(3, 10)]
        probe.evidence.update(real_overlapping_handlers=4, reserved=10, receipts=3, duplicate_zero_write=True)

    @case("guishi-create-replay-conflict-history", "鬼市求购", "创建/重放/参数冲突/历史操作号",
          "足额存款，保留历史回执", "一条消息只冻结一次，旧操作号可回放，冲突具体拒绝且不增加统计",
          group="market-guishi-lifecycle")
    async def create_replay(probe):
        module, clock, application = setup(probe)
        item_id, name = herb(probe)
        price = module.MIN_PRICE * 10
        probe.player(stone=200_000_000)
        with patch.object(module, "runtime_clock", clock), patch.object(module, "trade_application", application), \
                patch.object(module, "record_trade_event", wraps=module.record_trade_event) as stats:
            await probe.command("鬼市存灵石", "鬼市存灵石 100000000")
            text = f"鬼市求购 {name} {price} 2"
            await probe.command("鬼市求购", text, number=966001)
            before, count = state(probe), stats.call_count
            reply = await probe.command("鬼市求购", text, number=966001)
            assert "成功发布求购订单" in reply, reply
            assert before == state(probe) and count == stats.call_count, (reply, count, stats.call_count)
            reply = await probe.command("鬼市求购", f"鬼市求购 {name} {price} 3", number=966001)
            assert "operation_conflict" in reply, reply
            assert before == state(probe) and count == stats.call_count
            # Recreate the exact persisted operation-ID shape of the previous release.
            old_id = f"guishi-order:qiugou:966001:900001:{item_id}:{price}:2"
            probe.seed(probe.context.paths.trade_db, "UPDATE guishi_order_create_operations SET operation_id=?", (old_id,))
            before = state(probe)
            reply = await probe.command("鬼市求购", text, number=966001)
            assert "成功发布求购订单" in reply and before == state(probe), reply
            reply = await probe.command("鬼市求购", f"鬼市求购 {name} {price + 1} 2", number=966001)
            assert "operation_conflict" in reply and before == state(probe), reply
            assert count == stats.call_count
            assert statistics(probe, "鬼市求购次数", "鬼市求购数量", "鬼市求购冻结灵石") == [(1, 2, 2 * price)]
        probe.evidence.update(replay_zero_write=True, payload_conflict=True, historical_receipt=True)

    @case("guishi-stall-replay-stock-window", "鬼市摆摊", "库存截断后重放/日间回放/新请求拒绝",
          "10件真实物品，首单7件，第二单请求7件仅剩3件", "权威数量与回执一致，重放不再扣物或重复统计，闭市仅拒绝新请求",
          group="market-guishi-lifecycle")
    async def stall_replay(probe):
        module, clock, application = setup(probe)
        item_id, name = herb(probe)
        price = module.MIN_PRICE * 10
        with patch.object(module, "runtime_clock", clock), patch.object(module, "trade_application", application), \
                patch.object(module, "record_trade_event", wraps=module.record_trade_event) as stats:
            for number in (966010, 966011):
                reply = await probe.command("鬼市摆摊", f"鬼市摆摊 {name} {price} 7", number=number)
                assert "成功摆摊" in reply, reply
            assert "数量：3" in reply, reply
            assert probe.managers[0].goods_num("900001", item_id) == 0
            before, count = state(probe), stats.call_count
            clock.moment = datetime(2026, 11, 22, 2, tzinfo=timezone.utc)
            for number, quantity in ((966010, 7), (966011, 3)):
                reply = await probe.command("鬼市摆摊", f"鬼市摆摊 {name} {price} 7", number=number)
                assert "成功摆摊" in reply and f"数量：{quantity}" in reply, reply
                assert before == state(probe) and count == stats.call_count
            reply = await probe.command("鬼市摆摊", f"鬼市摆摊 {name} {price} 8", number=966010)
            assert "operation_conflict" in reply, reply
            reply = await probe.command("鬼市摆摊", f"鬼市摆摊 {name} {price} 1", number=966012)
            assert "下次可摆摊时间" in reply and before == state(probe), reply
        probe.evidence.update(capped_quantity=3, depleted_replay=True, closed_replay=True)

    @case("guishi-partial-fill-player-cancel", "鬼市取消求购", "部分成交/非本人/错误类型/取消/重放/冲突",
          "求购3件、真实摆摊1件撮合", "只退未成交2件金额，已得物品保留，具体原因与统计不重复",
          group="market-guishi-lifecycle")
    async def partial_cancel(probe):
        module, clock, application = setup(probe)
        item_id, name = herb(probe, "900002")
        price = module.MIN_PRICE * 10
        probe.player(stone=200_000_000)
        with patch.object(module, "runtime_clock", clock), patch.object(module, "trade_application", application), \
                patch.object(module, "record_trade_event", wraps=module.record_trade_event) as stats:
            await probe.command("鬼市存灵石", "鬼市存灵石 100000000")
            await probe.command("鬼市求购", f"鬼市求购 {name} {price} 3")
            order_id = application.guishi_get_orders(user_id="900001", order_type="qiugou")[0]["id"]
            await probe.command("鬼市摆摊", f"鬼市摆摊 {name} {price} 1", actor="900002")
            before = state(probe)
            reply = await probe.command("鬼市取消求购", f"鬼市取消求购 {order_id}", actor="900002")
            assert "not_owner" in reply and before == state(probe), reply
            reply = await probe.command("鬼市取消求购", f"鬼市取消求购 {order_id}", number=966020)
            assert "成功取消求购" in reply and "1200万" in reply, reply
            stored, goods = application.guishi_get_account("900001")
            assert stored == 100_000_000 - price and goods == {str(item_id): 1}, (stored, goods)
            before, count = state(probe), stats.call_count
            await probe.command("鬼市取消求购", f"鬼市取消求购 {order_id}", number=966020)
            assert before == state(probe) and count == stats.call_count
            reply = await probe.command("鬼市取消求购", "鬼市取消求购 123456", number=966020)
            assert "operation_conflict" in reply and before == state(probe), reply
            await probe.command("鬼市摆摊", f"鬼市摆摊 {name} {price} 1", actor="900002")
            stall = application.guishi_get_orders(user_id="900002", order_type="baitan")[0]["id"]
            before = state(probe)
            reply = await probe.command("鬼市取消求购", f"鬼市取消求购 {stall}", actor="900002")
            assert "not_qiugou" in reply and before == state(probe), reply
            await probe.command("鬼市求购", f"鬼市求购 {name} {price} 2")
            await probe.command("鬼市摆摊", f"鬼市摆摊 {name} {price} 3", actor="900002")
            # One existing listing and one unit of the new listing fill the buy.
            sell = application.guishi_get_orders(user_id="900002", order_type="baitan")
            assert len(sell) == 1 and sell[0]["filled_quantity"] == 1, sell
            stock = probe.managers[0].goods_num("900002", item_id)
            reply = await probe.command("鬼市收摊", actor="900002")
            assert f"{name} x2" in reply, reply
            assert probe.managers[0].goods_num("900002", item_id) == stock + 2
            assert application.guishi_get_account("900001")[1] == {str(item_id): 3}
        probe.evidence.update(refunded=price * 2, matched_items_preserved=1, precise_rejections=True)

    @case("guishi-close-partial-failure-retry", "鬼市收摊", "部分成功/背包满与缺物品/恢复/重复收摊",
          "三条实际摊位，第二件背包满、第三条物品目录暂缺", "只退成功订单，失败保留并逐单说明；恢复再收摊不重复退回或统计",
          group="market-guishi-lifecycle")
    async def close_partial(probe):
        from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork
        module, clock, application = setup(probe)
        item_ids = list(module.items.get_data_by_item_type("药材"))[:3]
        names = [probe.grant_item(int(item_id), 10) for item_id in item_ids]
        price = module.MIN_PRICE * 10
        with patch.object(module, "runtime_clock", clock), patch.object(module, "trade_application", application), \
                patch.object(module, "record_trade_event", wraps=module.record_trade_event) as stats:
            for name in names:
                await probe.command("鬼市摆摊", f"鬼市摆摊 {name} {price} 2")
            orders = application.guishi_get_orders(user_id="900001", order_type="baitan")
            ids = {row["item_name"]: row["id"] for row in orders}
            probe.seed(probe.context.paths.game_db, "UPDATE back SET goods_num=? WHERE user_id='900001' AND goods_id=?",
                       (module.XiuConfig().max_goods_num, int(item_ids[1])))
            original = module.items.get_data_by_item_name
            def catalogue(name):
                return (None, None) if name == names[2] else original(name)
            with patch.object(module.items, "get_data_by_item_name", side_effect=catalogue):
                reply = await probe.command("鬼市收摊", number=966030)
            assert "inventory_full" in reply and "item_missing" in reply, reply
            assert ids[names[1]] in reply and ids[names[2]] in reply, reply
            assert "所有摆摊订单已取消" not in reply and "均已售出" not in reply, reply
            assert len(application.guishi_get_orders(user_id="900001", order_type="baitan")) == 2
            assert probe.managers[0].goods_num("900001", int(item_ids[0])) == 10
            probe.seed(probe.context.paths.game_db, "UPDATE back SET goods_num=8 WHERE user_id='900001' AND goods_id=?", (int(item_ids[1]),))
            reply = await probe.command("鬼市收摊", number=966030)
            assert "所有摆摊订单已取消" in reply, reply
            assert not application.guishi_get_orders(user_id="900001", order_type="baitan")
            assert all(probe.managers[0].goods_num("900001", int(key)) == 10 for key in item_ids)
            before, count = state(probe), stats.call_count
            await probe.command("鬼市收摊", number=966030)
            assert before == state(probe) and count == stats.call_count
            assert probe.rows(probe.context.paths.trade_db, "SELECT COUNT(*) FROM guishi_order_cancel_operations") == [(3,)]
            assert statistics(probe, "鬼市收摊次数", "鬼市收摊退回物品") == [(2, 6)]
            close_stats = [call.args for call in stats.call_args_list if call.args[1] == "鬼市收摊"]
            assert [row[3]["鬼市收摊退回物品"] for row in close_stats] == [2, 4], close_stats
        for database in (probe.context.paths.game_db, probe.context.paths.trade_db):
            with DatabaseUnitOfWork(database, read_only=True) as uow:
                assert uow.query_all("PRAGMA integrity_check") == [{"integrity_check": "ok"}]
        probe.evidence.update(partial_committed=1, failures_preserved=2, recovered=2, integrity="ok")
