"""Cancellation and scheduled expiry recover from rejected writes via real consumers."""
import asyncio
from datetime import datetime, timezone
import sqlite3
from unittest.mock import patch

from tests.parameter_cases_guishi import setup, state as trade_state, statistics
from tests.parameter_cases_market import herb


def state(probe):
    return trade_state(probe) + (probe.rows(probe.context.paths.trade_db,
        'SELECT * FROM guishi_expired_order_operations ORDER BY operation_id'),)


def integrity(probe):
    from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork
    for path in (probe.context.paths.game_db, probe.context.paths.trade_db, probe.context.paths.player_db):
        with DatabaseUnitOfWork(path, read_only=True) as uow:
            assert uow.query_all('PRAGMA integrity_check') == [{'integrity_check': 'ok'}]


def register(case):
    @case('guishi-cancel-write-recovery', '鬼市取消求购', '部分成交/三步拒写/异常/同消息恢复重放',
          '实际成交一件，剩余求购退款三步逐次故障', '失败整单零写，具体原因或安全定位，恢复只退剩余且统计一次',
          group='market-guishi-return')
    async def cancel(probe):
        from nonebot_plugin_xiuxian_2.features.trade.application import TradeApplication
        from tests.command_probe import run_case
        from tests.parameter_exception_watch import watch_handler_catches
        module, clock, application = setup(probe)
        item_id, name = herb(probe, '900002')
        price = module.MIN_PRICE * 10
        database = probe.context.paths.trade_db
        probe.player(stone=200_000_000)
        with patch.object(module, 'runtime_clock', clock), patch.object(module, 'trade_application', application):
            await probe.command('鬼市存灵石', '鬼市存灵石 100000000')
            await probe.command('鬼市求购', f'鬼市求购 {name} {price} 3')
            order = application.guishi_get_orders(user_id='900001', order_type='qiugou')[0]['id']
            await probe.command('鬼市摆摊', f'鬼市摆摊 {name} {price} 1', actor='900002')
            command = f'鬼市取消求购 {order}'
            for target, event, reason in (
                ('guishi_info', 'UPDATE', 'guishi_refund_balance_write_rejected'),
                ('guishi_item', 'DELETE', 'guishi_refund_order_delete_rejected'),
                ('guishi_order_cancel_operations', 'INSERT', 'guishi_refund_receipt_write_rejected'),
            ):
                probe.seed(database, f'CREATE TRIGGER reject_return BEFORE {event} ON {target} '
                           'BEGIN SELECT RAISE(IGNORE); END')
                before = state(probe)
                reply = await probe.command('鬼市取消求购', command, number=970001)
                assert reason in reply and order in reply and '该订单本次返还已撤销' in reply, reply
                assert state(probe) == before
                probe.seed(database, 'DROP TRIGGER reject_return')
            probe.seed(database, "CREATE TRIGGER reject_return BEFORE INSERT ON guishi_order_cancel_operations "
                       "BEGIN SELECT RAISE(ABORT,'isolated cancel fault'); END")
            entry = next(row for row in probe.entries if row['command'] == '鬼市取消求购')
            before = state(probe)
            with patch.object(module, 'report_exception', return_value='处理失败（定位号：cancel-test）') as reported, \
                    watch_handler_catches(entry) as caught:
                result = await run_case(entry, 'parameter', 970001, input_text=command, actor='900001')
            assert result['status'] == 'pass' and len(caught) == 1, (result, caught)
            assert 'sqlite3.IntegrityError: isolated cancel fault' in caught[0]['traceback']
            assert isinstance(reported.call_args.args[1], sqlite3.IntegrityError)
            assert '定位号：cancel-test' in ''.join(result['replies'])
            probe.calls.append({'command': '鬼市取消求购', 'expected_injected_fault': caught, **result})
            assert state(probe) == before
            probe.seed(database, 'DROP TRIGGER reject_return')
            restarted = TradeApplication(probe.context.paths.game_db, database, clock=clock,
                                         player_database=probe.context.paths.player_db)
            with patch.object(module, 'trade_application', restarted):
                reply = await probe.command('鬼市取消求购', command, number=970001)
                assert '成功取消求购' in reply, reply
                assert restarted.guishi_get_account('900001') == (100_000_000 - price, {str(item_id): 1})
                after = state(probe)
                await probe.command('鬼市取消求购', command, number=970001)
                assert state(probe) == after
                assert statistics(probe, '鬼市取消求购次数', '鬼市求购退还灵石') == [(1, price * 2)]
        integrity(probe)
        probe.evidence.update(rejected_writes=3, original_exception=True, refunded_remaining=2,
                              restart_recovery=True, duplicate_zero_write=True, integrity='ok')

    @case('guishi-close-write-partial-recovery', '鬼市收摊', '首单成功/次单拒写/恢复/重放',
          '两条真实摆摊，第二条删除被忽略', '逐单完整回滚，成功保留、失败订单保留且具体回复，重试只退未完成单',
          group='market-guishi-return')
    async def close(probe):
        module, clock, application = setup(probe)
        item_id, name = herb(probe)
        database = probe.context.paths.trade_db
        price = module.MIN_PRICE * 10
        with patch.object(module, 'runtime_clock', clock), patch.object(module, 'trade_application', application):
            for _ in range(2):
                await probe.command('鬼市摆摊', f'鬼市摆摊 {name} {price} 2')
            orders = application.guishi_get_orders(user_id='900001', order_type='baitan')
            rejected = orders[1]['id']
            assert rejected.isdecimal()
            probe.seed(database, f"CREATE TRIGGER reject_return BEFORE DELETE ON guishi_item WHEN OLD.id='{rejected}' "
                       'BEGIN SELECT RAISE(IGNORE); END')
            reply = await probe.command('鬼市收摊', number=970010)
            assert '本次取消1条' in reply and '剩余1条' in reply, reply
            assert 'guishi_refund_order_delete_rejected' in reply and rejected in reply, reply
            assert probe.managers[0].goods_num('900001', item_id) == 8
            assert [row['id'] for row in application.guishi_get_orders(user_id='900001', order_type='baitan')] == [rejected]
            assert statistics(probe, '鬼市收摊次数', '鬼市收摊退回物品') == [(1, 2)]
            probe.seed(database, 'DROP TRIGGER reject_return')
            reply = await probe.command('鬼市收摊', number=970010)
            assert '所有摆摊订单已取消' in reply, reply
            assert probe.managers[0].goods_num('900001', item_id) == 10
            assert statistics(probe, '鬼市收摊次数', '鬼市收摊退回物品') == [(2, 4)]
            after = state(probe)
            await probe.command('鬼市收摊', number=970010)
            assert state(probe) == after
        integrity(probe)
        probe.evidence.update(partial_committed=1, rejected_order_preserved=True, total_returned=4, integrity='ok')

    @case('guishi-expiry-write-recovery', '鬼市求购', '07:59/08:00调度/退款拒写/退物拒写/恢复',
          '两条未匹配的真实求购与摆摊，时钟推进至截止', '实际APScheduler错误保留具体原因及订单，部分已退不重退，重复任务零写',
          group='market-guishi-return')
    async def expiry(probe):
        from apscheduler.events import EVENT_JOB_ERROR, EVENT_JOB_EXECUTED
        from apscheduler.schedulers.asyncio import AsyncIOScheduler
        module, clock, application = setup(probe)
        item_id, name = herb(probe, '900002')
        price = module.MIN_PRICE * 10
        database = probe.context.paths.trade_db
        scheduler = AsyncIOScheduler(timezone=application.business_timezone)
        async def trigger():
            outcome = asyncio.get_running_loop().create_future()
            listener = lambda event: outcome.set_result(event) if not outcome.done() else None
            scheduler.add_listener(listener, EVENT_JOB_ERROR | EVENT_JOB_EXECUTED)
            try:
                scheduler.add_job(module.clear_expired_baitan_orders_job, 'date')
                return await asyncio.wait_for(outcome, timeout=3)
            finally:
                scheduler.remove_listener(listener)
        probe.player(stone=200_000_000)
        try:
            with patch.object(module, 'runtime_clock', clock), patch.object(module, 'trade_application', application):
                await probe.command('鬼市存灵石', '鬼市存灵石 100000000')
                await probe.command('鬼市求购', f'鬼市求购 {name} {price} 2')
                await probe.command('鬼市摆摊', f'鬼市摆摊 {name} {price * 2} 3', actor='900002')
                buyer_order = application.guishi_get_orders(user_id='900001', order_type='qiugou')[0]['id']
                seller_order = application.guishi_get_orders(user_id='900002', order_type='baitan')[0]['id']
                clock.moment = datetime(2026, 11, 21, 23, 59, tzinfo=timezone.utc)
                before = state(probe)
                stats_before = statistics(probe, '鬼市取消求购次数', '鬼市求购退还灵石')
                assert application.guishi_expire_qiugou() == [] and state(probe) == before
                clock.moment = datetime(2026, 11, 22, 0, tzinfo=timezone.utc)
                probe.seed(database, 'CREATE TRIGGER reject_return BEFORE INSERT ON guishi_order_cancel_operations '
                           'BEGIN SELECT RAISE(IGNORE); END')
                scheduler.start()
                event = await trigger()
                assert event.code == EVENT_JOB_ERROR and event.traceback
                assert 'guishi_refund_receipt_write_rejected' in str(event.exception) and buyer_order in str(event.exception)
                assert state(probe) == before
                reply = await probe.command('鬼市信息')
                assert '到期处理未完成' in reply and '鬼市返还回执保存失败' in reply and buyer_order in reply, reply
                assert state(probe) == before
                probe.seed(database, 'DROP TRIGGER reject_return')
                probe.seed(database, 'CREATE TRIGGER reject_return BEFORE INSERT ON guishi_expired_order_operations '
                           'BEGIN SELECT RAISE(IGNORE); END')
                event = await trigger()
                assert event.code == EVENT_JOB_ERROR and event.traceback
                assert 'guishi_refund_receipt_write_rejected' in str(event.exception) and seller_order in str(event.exception)
                assert application.guishi_get_account('900001')[0] == 100_000_000
                assert probe.managers[0].goods_num('900002', item_id) == 7
                assert len(application.guishi_get_orders()) == 1
                assert probe.rows(database, 'SELECT refunded_stone FROM guishi_order_cancel_operations') == [(price * 2,)]
                assert probe.rows(database, 'SELECT * FROM guishi_expired_order_operations') == []
                probe.seed(database, 'DROP TRIGGER reject_return')
                assert (await trigger()).code == EVENT_JOB_EXECUTED
                assert not application.guishi_get_orders()
                assert probe.managers[0].goods_num('900002', item_id) == 10
                assert application.guishi_get_account('900001')[0] == 100_000_000
                assert probe.rows(database, 'SELECT refunded_quantity FROM guishi_expired_order_operations') == [(3,)]
                after = state(probe)
                assert (await trigger()).code == EVENT_JOB_EXECUTED and state(probe) == after
                assert statistics(probe, '鬼市取消求购次数', '鬼市求购退还灵石') == stats_before
                integrity(probe)
                probe.evidence.update(actual_scheduler_errors=2, failures_rolled_back=True,
                                      partial_refund_preserved=True, recovered=True, replay_zero_write=True, integrity='ok')
        finally:
            if scheduler.running:
                scheduler.shutdown(wait=False)
