"""Stored-item commands exercise real time policy, assets, receipts and recovery."""
import asyncio
from datetime import datetime, timezone
import json
import sqlite3
from unittest.mock import patch

from tests.parameter_cases_guishi import setup, statistics
from tests.parameter_cases_guishi_boundaries import transfer_state
from tests.parameter_cases_market import herb


def state(probe):
    return transfer_state(probe) + (probe.rows(probe.context.paths.game_db,
        'SELECT * FROM guishi_take_item_operations ORDER BY operation_id'),)


def seed_storage(probe, item_id, quantity=3):
    probe.seed(probe.context.paths.trade_db, "INSERT INTO guishi_info(user_id,stored_stone,items) VALUES('900001',15,?) "
               "ON CONFLICT(user_id) DO UPDATE SET items=excluded.items", (json.dumps({str(item_id): quantity}),))


def inventory(probe, item_id):
    return probe.rows(probe.context.paths.game_db,
                      "SELECT goods_num,bind_num FROM back WHERE user_id='900001' AND goods_id=?", (item_id,))[0]


def register(case):
    @case('guishi-take-traded-items-time-replay', '鬼市取物品', '成交暂存/周五拒绝/周六全取/周一回放与拒绝新请求',
          '真实买卖成交3件，买家已有背包和绑定数量', '周末全量取回并绑定，旧请求跨周末回放不重复发物或统计',
          group='market-guishi-take')
    async def traded(probe):
        module, clock, application = setup(probe)
        item_id, name = herb(probe, '900002')
        probe.grant_item(item_id, 10)
        probe.seed(probe.context.paths.game_db, "UPDATE back SET bind_num=2 WHERE user_id='900001' AND goods_id=?", (item_id,))
        probe.player(stone=100_000_000)
        with patch.object(module, 'runtime_clock', clock), patch.object(module, 'trade_application', application):
            await probe.command('鬼市存灵石', '鬼市存灵石 20000000')
            await probe.command('鬼市求购', f'鬼市求购 {name} {module.MIN_PRICE * 10} 3')
            await probe.command('鬼市摆摊', f'鬼市摆摊 {name} {module.MIN_PRICE * 10} 3', actor='900002')
            assert application.guishi_get_account('900001')[1] == {str(item_id): 3}
            clock.moment = datetime(2026, 11, 27, 15, 59, tzinfo=timezone.utc)
            before = state(probe)
            reply = await probe.command('鬼市取物品', f'鬼市取物品 {name}', number=969000)
            assert '仅在周六和周日' in reply and state(probe) == before, reply
            clock.moment = datetime(2026, 11, 27, 16, tzinfo=timezone.utc)
            reply = await probe.command('鬼市取物品', f'鬼市取物品  {name}  ', number=969000)
            assert f'成功取出 {name} x3' in reply and inventory(probe, item_id) == (13, 5), reply
            assert application.guishi_get_account('900001')[1] == {}
            after = state(probe)
            clock.moment = datetime(2026, 11, 29, 16, tzinfo=timezone.utc)
            reply = await probe.command('鬼市取物品', f'鬼市取物品 {name}', number=969000)
            assert f'成功取出 {name} x3' in reply and state(probe) == after, reply
            reply = await probe.command('鬼市取物品', f'鬼市取物品 {name}', number=969001)
            assert '仅在周六和周日' in reply and state(probe) == after, reply
            assert statistics(probe, '鬼市取物品次数', '鬼市取物品数量') == [(1, 3)]
        probe.evidence.update(traded_quantity=3, total_and_bound=[13, 5], weekday_replay=True, zero_extra_statistics=True)

    @case('guishi-take-historical-message-conflict', '鬼市取物品', '旧带物品操作号/同消息换物品/多条旧回执冲突',
          '保留旧格式成功回执和新寄存', '只读重放历史原行，不能通过换参数发另一件，不猜多条旧回执',
          group='market-guishi-take')
    async def history(probe):
        module, clock, application = setup(probe)
        item_id, name = herb(probe)
        other_id = next(int(key) for key in module.items.get_data_by_item_type('药材') if int(key) != item_id)
        other_name = module.items.get_data_by_item_id(other_id)['name']
        seed_storage(probe, item_id)
        with patch.object(module, 'runtime_clock', clock), patch.object(module, 'trade_application', application):
            await probe.command('鬼市取物品', f'鬼市取物品 {name}', number=969010)
            old_id = f'guishi-item-take:969010:900001:{item_id}'
            probe.seed(probe.context.paths.game_db, 'UPDATE guishi_take_item_operations SET operation_id=?', (old_id,))
            seed_storage(probe, other_id, 2)
            before = state(probe)
            reply = await probe.command('鬼市取物品', f'鬼市取物品 {name}', number=969010)
            assert '成功取出' in reply and state(probe) == before, reply
            reply = await probe.command('鬼市取物品', f'鬼市取物品 {other_name}', number=969010)
            assert '该操作号已用于不同内容' in reply and state(probe) == before, reply
            probe.seed(probe.context.paths.game_db,
                       'INSERT INTO guishi_take_item_operations(operation_id,user_id,goods_id,item_name,goods_type,quantity) VALUES(?,?,?,?,?,?)',
                       (f'guishi-item-take:969010:900001:{other_id}', '900001', other_id, other_name, '药材', 2))
            before = state(probe)
            reply = await probe.command('鬼市取物品', f'鬼市取物品 {name}', number=969010)
            assert '该操作号已用于不同内容' in reply and state(probe) == before, reply
        probe.evidence.update(legacy_receipt_unchanged=True, payload_conflict=True, ambiguous_history_rejected=True)

    @case('guishi-take-cap-invalid-storage', '鬼市取物品', '空参/多参/未知物品/坏暂存/坏库存/上限相邻',
          '真实暂存3件与历史坏条目、库存数量', '已知原因和主体明确，拒绝三库零写，恰好达到上限时全取并绑定',
          group='market-guishi-take')
    async def boundaries(probe):
        module, clock, application = setup(probe)
        item_id, name = herb(probe)
        seed_storage(probe, item_id)
        with patch.object(module, 'runtime_clock', clock), patch.object(module, 'trade_application', application):
            before = state(probe)
            for text, expected in (('鬼市取物品', '请输入要取出'), ('鬼市取物品 no-such-item', '不存在'),
                                   (f'鬼市取物品 {name} 2', '不存在')):
                reply = await probe.command('鬼市取物品', text)
                assert expected in reply and state(probe) == before, reply
            raw_cases = ((f'{{"{item_id}":3,"12345":"bad"}}', '暂存物品ID:12345', '数量不是有效'),
                         ('{bad', '本人账户', '记录格式损坏'),
                         (f'{{"{item_id}":3,"{item_id}":4}}', '本人账户', '重复物品编号'))
            for raw, subject, expected in raw_cases:
                probe.seed(probe.context.paths.trade_db, 'UPDATE guishi_info SET items=?', (raw,))
                before = state(probe)
                reply = await probe.command('鬼市取物品', f'鬼市取物品 {name}')
                assert expected in reply and subject in reply and state(probe) == before, reply
            seed_storage(probe, item_id)
            probe.seed(probe.context.paths.game_db, "UPDATE back SET goods_num=3,bind_num=4 WHERE user_id='900001' AND goods_id=?", (item_id,))
            before = state(probe)
            reply = await probe.command('鬼市取物品', f'鬼市取物品 {name}')
            assert '背包总数或绑定数记录不合法' in reply and name in reply and state(probe) == before, reply
            cap = module.XiuConfig().max_goods_num
            probe.seed(probe.context.paths.game_db, "UPDATE back SET goods_num=?,bind_num=7 WHERE user_id='900001' AND goods_id=?", (cap - 2, item_id))
            before = state(probe)
            reply = await probe.command('鬼市取物品', f'鬼市取物品 {name}', number=969020)
            assert '背包空间不足' in reply and state(probe) == before, reply
            probe.seed(probe.context.paths.game_db, "UPDATE back SET goods_num=? WHERE user_id='900001' AND goods_id=?", (cap - 3, item_id))
            reply = await probe.command('鬼市取物品', f'鬼市取物品 {name}', number=969020)
            assert '成功取出' in reply and inventory(probe, item_id) == (cap, 10), reply
            before = state(probe)
            reply = await probe.command('鬼市取物品', f'鬼市取物品 {name}', number=969021)
            assert '没有暂存物品' in reply and state(probe) == before, reply
        probe.evidence.update(rejections_zero_write=True, cap_exact=True, preserved_bound=7, received_bound=3)

    @case('guishi-take-write-retry-restart', '鬼市取物品', '三处IGNORE/未知SQLite异常/重建应用/同消息恢复',
          '真实暂存与背包，逐次注入存储故障', '失败完整回滚，具体原因或安全定位，恢复后同消息只发一次',
          group='market-guishi-take')
    async def failures(probe):
        from nonebot_plugin_xiuxian_2.features.trade.application import TradeApplication
        from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork
        from tests.command_probe import run_case
        from tests.parameter_exception_watch import watch_handler_catches
        module, clock, application = setup(probe)
        item_id, name = herb(probe)
        with patch.object(module, 'runtime_clock', clock), patch.object(module, 'trade_application', application):
            for index, target in enumerate(('storage', 'inventory', 'receipt', 'exception')):
                seed_storage(probe, item_id)
                database = probe.context.paths.trade_db if target == 'storage' else probe.context.paths.game_db
                table = {'storage': 'guishi_info', 'inventory': 'back', 'receipt': 'guishi_take_item_operations',
                         'exception': 'guishi_take_item_operations'}[target]
                event = 'UPDATE' if target in ('storage', 'inventory') else 'INSERT'
                fault = "RAISE(ABORT,'isolated take fault')" if target == 'exception' else 'RAISE(IGNORE)'
                probe.seed(database, f'CREATE TRIGGER reject_take BEFORE {event} ON {table} BEGIN SELECT {fault}; END')
                before = state(probe)
                if target == 'exception':
                    entry = next(row for row in probe.entries if row['command'] == '鬼市取物品')
                    with patch.object(module, 'report_exception', return_value='处理失败，请查询结果（定位号：take-test）') as reported, \
                            watch_handler_catches(entry) as caught:
                        result = await run_case(entry, 'parameter', 969030 + index,
                                                input_text=f'鬼市取物品 {name}', actor='900001')
                    assert result['status'] == 'pass' and len(caught) == 1, (result, caught)
                    assert 'sqlite3.IntegrityError: isolated take fault' in caught[0]['traceback'], caught
                    assert isinstance(reported.call_args.args[1], sqlite3.IntegrityError)
                    reply = '\n'.join(result['replies'])
                    assert '定位号：take-test' in reply and 'isolated take fault' not in reply, reply
                    probe.calls.append({'command': '鬼市取物品', 'expected_injected_fault': caught, **result})
                else:
                    reply = await probe.command('鬼市取物品', f'鬼市取物品 {name}', number=969030 + index)
                    expected = {'storage': '暂存扣除保存失败', 'inventory': '背包写入失败', 'receipt': '取物回执保存失败'}[target]
                    assert expected in reply and '本人账户' in reply and '本次取物已撤销' in reply, reply
                assert state(probe) == before
                probe.seed(database, 'DROP TRIGGER reject_take')
                restarted = TradeApplication(probe.context.paths.game_db, probe.context.paths.trade_db,
                                             clock=clock, player_database=probe.context.paths.player_db)
                with patch.object(module, 'trade_application', restarted):
                    reply = await probe.command('鬼市取物品', f'鬼市取物品 {name}', number=969030 + index)
                    assert f'成功取出 {name} x3' in reply, reply
                    after = state(probe)
                    await probe.command('鬼市取物品', f'鬼市取物品 {name}', number=969030 + index)
                    assert state(probe) == after
            assert inventory(probe, item_id) == (22, 12)
            assert statistics(probe, '鬼市取物品次数', '鬼市取物品数量') == [(4, 12)]
            for database in (probe.context.paths.game_db, probe.context.paths.trade_db, probe.context.paths.player_db):
                with DatabaseUnitOfWork(database, read_only=True) as uow:
                    assert uow.query_all('PRAGMA integrity_check') == [{'integrity_check': 'ok'}]
        probe.evidence.update(rollback_cases=4, recovered_after_restart=4, received_once=12, integrity='ok')

    @case('guishi-take-concurrent-messages', '鬼市取物品', '同消息并发/不同消息争取同批寄存',
          '两条真实matcher在handler入口会合', '同消息回放、不同消息只有一笔全取，资产绑定与统计只增加一次',
          group='market-guishi-take')
    async def concurrent(probe):
        from tests.command_concurrency import concurrent_commands
        module, clock, application = setup(probe)
        item_id, name = herb(probe)
        entry = next(row for row in probe.entries if row['command'] == '鬼市取物品')
        original = module.assign_bot
        with patch.object(module, 'runtime_clock', clock), patch.object(module, 'trade_application', application):
            for numbers in ((969040, 969040), (969041, 969042)):
                seed_storage(probe, item_id)
                arrived, ready = [], asyncio.Event()
                async def meet(*args, **kwargs):
                    arrived.append(kwargs['event'])
                    if len(arrived) == 2:
                        ready.set()
                    await asyncio.wait_for(ready.wait(), timeout=3)
                    return await original(*args, **kwargs)
                with patch.object(module, 'assign_bot', meet):
                    rows = await concurrent_commands(entry, [(f'鬼市取物品 {name}', '900001', number) for number in numbers])
                replies = [''.join(row['replies']) for row in rows]
                assert sum('成功取出' in reply for reply in replies) == (2 if numbers[0] == numbers[1] else 1), replies
                if numbers[0] != numbers[1]:
                    assert sum('没有暂存物品' in reply for reply in replies) == 1, replies
                probe.calls.extend({'command': '鬼市取物品', 'status': 'pass', 'concurrent': True, **row} for row in rows)
            assert inventory(probe, item_id) == (16, 6)
            assert statistics(probe, '鬼市取物品次数', '鬼市取物品数量') == [(2, 6)]
            assert probe.rows(probe.context.paths.game_db, 'SELECT COUNT(*) FROM guishi_take_item_operations') == [(2,)]
        probe.evidence.update(overlapping_handlers=4, granted=6, receipt_count=2, statistics_once=True)
