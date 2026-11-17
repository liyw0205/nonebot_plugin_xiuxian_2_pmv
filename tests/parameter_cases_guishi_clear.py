"""Privileged bulk clear uses isolated assets and real dispatch/permission checks."""
import sqlite3
from unittest.mock import patch

from tests.parameter_cases_guishi import setup
from tests.parameter_cases_guishi_return import integrity, state
from tests.parameter_cases_market import herb


def register(case):
    @case('guishi-clear-permission-partial-retry', '清空鬼市', '权限拒绝/部分成功/具体退款拒绝/恢复/重复',
          '真实求购和高价摊位，不发生撮合，模拟管理员900003',
          '普通玩家零写，管理员逐单退款退物，失败订单保留且回复原因，重试不重退已完成单',
          group='market-guishi-clear')
    async def clear(probe):
        from tests.command_probe import run_case
        module, clock, application = setup(probe)
        item_id, name = herb(probe, '900002')
        price = module.MIN_PRICE * 10
        database = probe.context.paths.trade_db
        probe.player(stone=200_000_000)
        with patch.object(module, 'runtime_clock', clock), patch.object(module, 'trade_application', application):
            await probe.command('鬼市存灵石', '鬼市存灵石 100000000')
            await probe.command('鬼市求购', f'鬼市求购 {name} {price} 2')
            await probe.command('鬼市摆摊', f'鬼市摆摊 {name} {price * 2} 3', actor='900002')
            order = application.guishi_get_orders(order_type='qiugou')[0]['id']
            before = state(probe)
            entry = next(row for row in probe.entries if row['command'] == '清空鬼市')
            denied = await run_case(entry, 'parameter', 971001, input_text='清空鬼市', actor='900001')
            assert denied['status'] == 'blocked' and denied['reason'] == 'permission/rule rejected', denied
            assert not denied['replies'] and not denied['errors'] and state(probe) == before
            probe.calls.append({'command': '清空鬼市', 'expected_permission_rejection': True, **denied})
            probe.seed(database, 'CREATE TRIGGER reject_clear BEFORE INSERT ON guishi_order_cancel_operations '
                       'BEGIN SELECT RAISE(IGNORE); END')
            probe.clear_cooldowns()
            reply = await probe.command('清空鬼市', actor='900003', number=971002)
            assert '本次处理 1 条' in reply and '当前剩余 1 条' in reply and '成功清空鬼市' not in reply, reply
            assert 'guishi_refund_receipt_write_rejected' in reply and order in reply, reply
            assert probe.managers[0].goods_num('900002', item_id) == 10
            assert application.guishi_get_account('900001')[0] == 100_000_000 - price * 2
            assert len(application.guishi_get_orders()) == 1
            assert probe.rows(database, 'SELECT * FROM guishi_order_cancel_operations') == []
            assert probe.rows(database, 'SELECT refunded_quantity FROM guishi_expired_order_operations') == [(3,)]
            probe.seed(database, 'DROP TRIGGER reject_clear')
            probe.clear_cooldowns()
            reply = await probe.command('清空鬼市', actor='900003', number=971002)
            assert '成功清空鬼市' in reply and '本次处理 1 条' in reply, reply
            assert not application.guishi_get_orders()
            assert application.guishi_get_account('900001')[0] == 100_000_000
            assert probe.managers[0].goods_num('900002', item_id) == 10
            after = state(probe)
            probe.clear_cooldowns()
            reply = await probe.command('清空鬼市', actor='900003', number=971002)
            assert '没有订单可供清空' in reply and state(probe) == after, reply
        integrity(probe)
        probe.evidence.update(permission_zero_write=True, partial_committed=1, recovered=1,
                              duplicate_zero_write=True, integrity='ok')

    @case('guishi-clear-unknown-failure-recovery', '清空鬼市', 'SQLite异常/未知订单类型/恢复',
          '真实求购，回执写入ABORT；历史订单类型非法',
          '未知异常记录安全定位不吞，非法类型明确保留订单，解除故障后只退款一次',
          group='market-guishi-clear')
    async def failure(probe):
        from tests.command_probe import run_case
        from tests.parameter_exception_watch import watch_handler_catches
        module, clock, application = setup(probe)
        _item, name = herb(probe)
        price = module.MIN_PRICE * 10
        database = probe.context.paths.trade_db
        probe.player(stone=200_000_000)
        with patch.object(module, 'runtime_clock', clock), patch.object(module, 'trade_application', application):
            await probe.command('鬼市存灵石', '鬼市存灵石 100000000')
            await probe.command('鬼市求购', f'鬼市求购 {name} {price} 2')
            order = application.guishi_get_orders(order_type='qiugou')[0]['id']
            probe.seed(database, "CREATE TRIGGER reject_clear BEFORE INSERT ON guishi_order_cancel_operations "
                       "BEGIN SELECT RAISE(ABORT,'isolated bulk fault'); END")
            before = state(probe)
            entry = next(row for row in probe.entries if row['command'] == '清空鬼市')
            with patch.object(module, 'report_exception', return_value='处理失败（定位号：clear-test）') as reported, \
                    watch_handler_catches(entry) as caught:
                result = await run_case(entry, 'parameter', 971010, input_text='清空鬼市', actor='900003')
            assert result['status'] == 'pass' and len(caught) == 1, (result, caught)
            assert 'sqlite3.IntegrityError: isolated bulk fault' in caught[0]['traceback']
            assert isinstance(reported.call_args.args[1], sqlite3.IntegrityError)
            reply = ''.join(result['replies'])
            assert '定位号：clear-test' in reply and order in reply and '当前剩余 1 条' in reply
            assert '成功清空鬼市' not in reply and state(probe) == before
            probe.calls.append({'command': '清空鬼市', 'expected_injected_fault': caught, **result})
            probe.seed(database, 'DROP TRIGGER reject_clear')
            probe.seed(database, "UPDATE guishi_item SET item_type='invalid' WHERE id=?", (order,))
            before = state(probe)
            probe.clear_cooldowns()
            reply = await probe.command('清空鬼市', actor='900003', number=971010)
            assert 'guishi_order_type_invalid' in reply and order in reply and state(probe) == before, reply
            probe.seed(database, "UPDATE guishi_item SET item_type='求购' WHERE id=?", (order,))
            probe.clear_cooldowns()
            reply = await probe.command('清空鬼市', actor='900003', number=971010)
            assert '成功清空鬼市' in reply, reply
            assert application.guishi_get_account('900001')[0] == 100_000_000
            assert probe.rows(database, 'SELECT refunded_stone FROM guishi_order_cancel_operations') == [(price * 2,)]
        integrity(probe)
        probe.evidence.update(original_exception=True, rollback=True, invalid_type_preserved=True,
                              recovered_legacy_type=True, integrity='ok')
