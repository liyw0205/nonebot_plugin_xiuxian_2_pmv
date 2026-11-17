"""Real dispatch across auction startup, bidding, deadline and restart settlement."""
import asyncio
from contextlib import contextmanager, ExitStack
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import patch

from tests.parameter_cases_market import trade_module, FixedClock, herb
from tests.parameter_cases_guishi_return import integrity


@contextmanager
def setup(probe):
    from nonebot_plugin_xiuxian_2.features.auction.application import AuctionBidApplication
    from nonebot_plugin_xiuxian_2.features.auction.query_application import AuctionQueryApplication
    from nonebot_plugin_xiuxian_2.features.auction.session_start_application import AuctionSessionStartApplication
    from nonebot_plugin_xiuxian_2.features.auction.settlement import AuctionSettlementApplication
    from nonebot_plugin_xiuxian_2.compatibility.auction_bid_effects import LegacyAuctionBidEffects
    from nonebot_plugin_xiuxian_2.compatibility.auction_settlement_effects import LegacyAuctionSettlementEffects
    module = trade_module()
    clock = FixedClock(datetime(2026, 11, 17, 16, tzinfo=timezone.utc))
    paths = probe.context.paths
    start = AuctionSessionStartApplication(paths.game_db, paths.trade_db, clock=clock)
    query = AuctionQueryApplication(paths.game_db)
    bid = AuctionBidApplication(paths.game_db, effects=LegacyAuctionBidEffects(paths.player_db))
    settlement = AuctionSettlementApplication(paths.game_db, effects=LegacyAuctionSettlementEffects(paths.player_db))
    config = dict(module.auction_config.get_auction_schedule(), enabled=True, duration_hours=2, last_auto_start_date='')
    def save(section, value, key):
        config[key] = value
    with ExitStack() as stack:
        for name, value in (('runtime_clock', clock), ('_auction_session_start_application', lambda: start),
                            ('_auction_query_application', lambda: query), ('_auction_bid_application', lambda: bid),
                            ('_auction_settlement_application', lambda: settlement)):
            stack.enter_context(patch.object(module, name, value))
        stack.enter_context(patch.object(module.auction_config, 'get_system_items', return_value={}))
        stack.enter_context(patch.object(module.auction_config, 'get_auction_schedule', return_value=config))
        stack.enter_context(patch.object(module.auction_config, 'set_auction_config_value', side_effect=save))
        yield SimpleNamespace(module=module, clock=clock, start=start, query=query, config=config)


def assets(probe):
    game, market, player = probe.context.paths.game_db, probe.context.paths.trade_db, probe.context.paths.player_db
    return (probe.rows(game, 'SELECT user_id,stone FROM user_xiuxian ORDER BY user_id'),
            probe.rows(game, 'SELECT user_id,goods_id,goods_num,bind_num FROM back ORDER BY user_id,goods_id'),
            probe.rows(game, 'SELECT * FROM auction_current ORDER BY id'),
            probe.rows(game, 'SELECT * FROM auction_sessions ORDER BY session_id'),
            probe.rows(game, 'SELECT * FROM auction_history ORDER BY id'),
            probe.rows(game, 'SELECT * FROM auction_bid_operations ORDER BY operation_id'),
            probe.rows(game, 'SELECT * FROM auction_session_operations ORDER BY operation_id'),
            probe.rows(game, 'SELECT * FROM auction_pending_items ORDER BY id'),
            probe.rows(game, 'SELECT * FROM auction_pending_claim_operations ORDER BY operation_id'),
            probe.rows(game, 'SELECT * FROM auction_queue_operations ORDER BY operation_id'),
            probe.rows(game, 'SELECT * FROM domain_outbox ORDER BY event_id'),
            probe.rows(market, 'SELECT * FROM auction_player_upload ORDER BY user_id,item_id'),
            probe.rows(player, 'SELECT * FROM statistics ORDER BY user_id'))


async def fire(callback):
    from apscheduler.events import EVENT_JOB_ERROR, EVENT_JOB_EXECUTED
    from apscheduler.schedulers.asyncio import AsyncIOScheduler
    scheduler = AsyncIOScheduler(timezone='Asia/Shanghai')
    event = asyncio.get_running_loop().create_future()
    scheduler.add_listener(lambda result: event.set_result(result), EVENT_JOB_ERROR | EVENT_JOB_EXECUTED)
    scheduler.add_job(callback, 'date')
    scheduler.start()
    try:
        return await asyncio.wait_for(event, timeout=3)
    finally:
        scheduler.shutdown(wait=False)
        await asyncio.sleep(0)


def register(case):
    @case('auction-bid-write-recovery', '拍卖竞拍', '前次竞价/替换退款/四步拒写/重试/本人加价/到期',
          '实际上架开场及前次扣款，四步IGNORE含副作用与回执ABORT',
          '拒绝不扣款或提前退款；同请求恢复一次，实际统计/最高价/到期成交一致',
          group='market-auction-bid-write')
    async def bid_write(probe):
        import sqlite3
        from apscheduler.events import EVENT_JOB_EXECUTED
        from nonebot_plugin_xiuxian_2.features.auction.application import AuctionBidApplication
        from nonebot_plugin_xiuxian_2.compatibility.auction_bid_effects import LegacyAuctionBidEffects
        from tests.command_probe import run_case
        from tests.parameter_exception_watch import watch_handler_catches
        with setup(probe) as state:
            module, game = state.module, probe.context.paths.game_db
            item_id, name = herb(probe, '900003')
            for uid in ('900001', '900002', '900003'):
                probe.player(uid, stone=100_000_000)
            price = module.auction_config.get_auction_rules()['min_price']
            await probe.command('拍卖上架', f'拍卖上架 {name} {price}', actor='900003')
            assert (await fire(module.auto_start_auction_job)).code == EVENT_JOB_EXECUTED
            lot = state.query.get_current_auction()[0]
            reply = await probe.command('拍卖竞拍', f"拍卖竞拍 {lot['id']} {price}", actor='900002')
            assert '竞拍成功' in reply, reply
            text = f"拍卖竞拍 {lot['id']} {price * 2}"
            for target, table, event, condition in (
                ('debit', 'user_xiuxian', 'UPDATE', "WHEN NEW.user_id='900001'"),
                ('refund', 'user_xiuxian', 'UPDATE', "WHEN NEW.user_id='900002'"),
                ('item', 'auction_current', 'UPDATE', ''),
                ('receipt', 'auction_bid_operations', 'INSERT', ''),
            ):
                probe.seed(game, f'CREATE TRIGGER refuse_bid BEFORE {event} ON {table} {condition} '
                           "BEGIN UPDATE user_xiuxian SET stone=0 WHERE user_id='900003'; SELECT RAISE(IGNORE); END")
                before = assets(probe)
                reply = await probe.command('拍卖竞拍', text, number=980420)
                assert f'auction_bid_{target}_write_rejected' in reply and '主体：' in reply, reply
                assert '竞拍成功' not in reply and assets(probe) == before
                probe.seed(game, 'DROP TRIGGER refuse_bid')
            probe.seed(game, "CREATE TRIGGER refuse_bid BEFORE INSERT ON auction_bid_operations "
                       "BEGIN SELECT RAISE(ABORT,'isolated bid command fault'); END")
            entry = next(row for row in probe.entries if row['command'] == '拍卖竞拍')
            before = assets(probe)
            with patch.object(module, 'report_exception', return_value='处理失败（定位号：auction-bid-test）') as reported, \
                    watch_handler_catches(entry) as caught:
                result = await run_case(entry, 'parameter', 980420, input_text=text, actor='900001')
            assert result['status'] == 'pass' and len(caught) == 1, (result, caught)
            assert 'sqlite3.IntegrityError: isolated bid command fault' in caught[0]['traceback']
            assert isinstance(reported.call_args.args[1], sqlite3.IntegrityError)
            assert '定位号：auction-bid-test' in ''.join(result['replies'])
            probe.calls.append({'command': '拍卖竞拍', 'expected_injected_fault': caught, **result})
            assert assets(probe) == before
            probe.seed(game, 'DROP TRIGGER refuse_bid')
            restarted = AuctionBidApplication(game, effects=LegacyAuctionBidEffects(probe.context.paths.player_db))
            with patch.object(module, '_auction_bid_application', return_value=restarted):
                state.clock.moment += timedelta(minutes=1)
                reply = await probe.command('拍卖竞拍', text, number=980420)
                assert '竞拍成功' in reply and '已退还' in reply, reply
                assert probe.rows(game, "SELECT stone FROM user_xiuxian WHERE user_id='900001'") == [(100_000_000-price*2,)]
                assert probe.rows(game, "SELECT stone FROM user_xiuxian WHERE user_id='900002'") == [(100_000_000,)]
                after = assets(probe)
                await probe.command('拍卖竞拍', text, number=980420)
                assert assets(probe) == after
                reply = await probe.command('拍卖竞拍', f"拍卖竞拍 {lot['id']} {price * 3}")
                assert '竞拍成功' in reply and '已退还' not in reply, reply
                assert probe.rows(game, "SELECT stone FROM user_xiuxian WHERE user_id='900001'") == [(100_000_000-price*3,)]
                assert state.query.get_current_auction(lot['id'])['bids'] == {'900001': price*3}
                assert probe.rows(probe.context.paths.player_db,
                    'SELECT "拍卖出价次数","拍卖出价灵石" FROM statistics WHERE user_id=\'900001\'') == [(2, price*5)]
                state.clock.moment += timedelta(hours=2)
                assert (await fire(module.check_auction_end_job)).code == EVENT_JOB_EXECUTED
                history = state.query.get_auction_history(lot['id'])[0]
                assert history['winner_id'] == '900001' and history['final_price'] == price*3
                assert probe.managers[0].goods_num('900001', item_id) == 1
                assert probe.rows(game, "SELECT stone FROM user_xiuxian WHERE user_id='900002'") == [(100_000_000,)]
                assert probe.rows(game, "SELECT stone FROM user_xiuxian WHERE user_id='900003'") == [(100_000_000+history['seller_earnings'],)]
                after = assets(probe)
                await fire(module.check_auction_end_job)
                await probe.command('拍卖竞拍', text, number=980420)
                assert assets(probe) == after
        integrity(probe)
        probe.evidence.update(real_dispatch=True, rejected_steps=4, full_rollback=True, original_abort=True,
                              restart_recovery=True, previous_refund_once=True, same_bidder_difference_only=True,
                              actual_settlement=True, statistics_once=True, integrity='ok')

    @case('auction-queue-handoff-recovery', '拍卖上架', '上架/下架三步拒写/原消息恢复/重放/流拍',
          '真实库存绑定/装备，隔离三库，三步IGNORE和回执ABORT',
          '拒绝零写保留具体原因，重建后同消息恢复；绑定返还/统计各一次，剩余拍品实际到期返还',
          group='market-auction-queue')
    async def queue_handoff(probe):
        import sqlite3
        from apscheduler.events import EVENT_JOB_EXECUTED
        from nonebot_plugin_xiuxian_2.features.auction.queue_application import AuctionQueueApplication
        from tests.command_probe import run_case
        from tests.parameter_exception_watch import watch_handler_catches
        with setup(probe) as state:
            module, game, market = state.module, probe.context.paths.game_db, probe.context.paths.trade_db
            item_id, name = herb(probe, '900002')
            probe.player('900002', stone=100_000_000)
            price = module.auction_config.get_auction_rules()['min_price']
            probe.seed(game, 'UPDATE back SET bind_num=2,state=1 WHERE user_id=? AND goods_id=?', ('900002', item_id))
            app = AuctionQueueApplication(game, market, 10, clock=state.clock)
            with patch.object(module, '_auction_queue_application', return_value=app) as provider:
                before = assets(probe)
                for text in ('拍卖上架', f'拍卖上架 {name}', f'拍卖上架 {name} abc',
                             f'拍卖上架 {name} -1', f'拍卖上架 {name} {price} extra'):
                    reply = await probe.command('拍卖上架', text, actor='900002')
                    assert '格式' in reply or '整数' in reply or '起拍价' in reply, reply
                    assert assets(probe) == before
                for action, command, text, number in (
                    ('enqueue', '拍卖上架', f'拍卖上架 {name} {price}', 980400),
                    ('dequeue', '拍卖下架', f'拍卖下架 {name}', 980401),
                ):
                    for target, table, database, event in (
                        ('inventory', 'back', game, 'UPDATE' if action == 'enqueue' else 'INSERT'),
                        ('queue', 'auction_player_upload', market, 'INSERT' if action == 'enqueue' else 'DELETE'),
                        ('receipt', 'auction_queue_operations', game, 'INSERT'),
                    ):
                        side_effect = ("UPDATE auction_player_upload SET user_name='wrong';" if target == 'queue'
                                       else "UPDATE back SET create_time='wrong' WHERE user_id='900002';")
                        probe.seed(database, f'CREATE TRIGGER refuse_queue BEFORE {event} ON {table} '
                                   f'BEGIN {side_effect} SELECT RAISE(IGNORE); END')
                        before = assets(probe)
                        reply = await probe.command(command, text, number=number, actor='900002')
                        assert f'auction_queue_{target}_write_rejected' in reply and '主体：' in reply, reply
                        assert '成功' not in reply and assets(probe) == before
                        probe.seed(database, 'DROP TRIGGER refuse_queue')
                    probe.seed(game, "CREATE TRIGGER refuse_queue BEFORE INSERT ON auction_queue_operations "
                               "BEGIN SELECT RAISE(ABORT,'isolated queue command fault'); END")
                    entry = next(row for row in probe.entries if row['command'] == command)
                    before = assets(probe)
                    with patch.object(module, 'report_exception', return_value='处理失败（定位号：auction-queue-test）') as reported, \
                            watch_handler_catches(entry) as caught:
                        result = await run_case(entry, 'parameter', number, input_text=text, actor='900002')
                    assert result['status'] == 'pass' and len(caught) == 1, (result, caught)
                    assert 'sqlite3.IntegrityError: isolated queue command fault' in caught[0]['traceback']
                    assert isinstance(reported.call_args.args[1], sqlite3.IntegrityError)
                    assert '定位号：auction-queue-test' in ''.join(result['replies'])
                    probe.calls.append({'command': command, 'expected_injected_fault': caught, **result})
                    assert assets(probe) == before
                    probe.seed(game, 'DROP TRIGGER refuse_queue')
                    provider.return_value = AuctionQueueApplication(game, market, 10, clock=state.clock)
                    state.clock.moment += timedelta(minutes=1)
                    reply = await probe.command(command, text, number=number, actor='900002')
                    assert '成功' in reply, reply
                    assert probe.rows(game, 'SELECT goods_num,bind_num,state FROM back WHERE user_id=? AND goods_id=?',
                                      ('900002', item_id)) == [(9, 2, 1) if action == 'enqueue' else (10, 3, 1)]
                    after = assets(probe)
                    await probe.command(command, text, number=number, actor='900002')
                    assert assets(probe) == after
                    if action == 'enqueue':
                        reply = await probe.command(command, f'拍卖上架 {name} {price * 2}', number=number, actor='900002')
                        assert 'operation_conflict' in reply and assets(probe) == after, reply
                        reply = await probe.command('拍卖下架', f'拍卖下架 {name}', actor='900001')
                        assert 'queue_missing' in reply and assets(probe) == after, reply
                assert probe.rows(probe.context.paths.player_db,
                    'SELECT "拍卖上架次数","拍卖下架次数" FROM statistics WHERE user_id=\'900002\'') == [(1, 1)]
                await probe.command('拍卖上架', f'拍卖上架 {name} {price}', actor='900002')
                assert (await fire(module.auto_start_auction_job)).code == EVENT_JOB_EXECUTED
                state.clock.moment += timedelta(hours=2)
                assert (await fire(module.check_auction_end_job)).code == EVENT_JOB_EXECUTED
                assert probe.rows(game, 'SELECT goods_num,bind_num,state FROM back WHERE user_id=? AND goods_id=?',
                                  ('900002', item_id)) == [(10, 4, 1)]
                after = assets(probe)
                await fire(module.check_auction_end_job)
                assert assets(probe) == after
        integrity(probe)
        probe.evidence.update(real_dispatch=True, rejected_steps=6, original_abort=True, full_rollback=True,
                              same_message_recovery=True, binding_and_equipment=True, statistics_once=True,
                              actual_expiry_return=True, integrity='ok')

    @case('auction-queue-limits-and-inventory', '拍卖下架', '队列上限/库存不足/满包/同消息恢复',
          '本人一件等待区、另一件绑定装备库存、可控上限和三库快照',
          '非法和业务边界零写且具体原因，满包恢复仅退本人一件绑定物品', group='market-auction-queue')
    async def queue_limits(probe):
        from nonebot_plugin_xiuxian_2.features.auction.queue_application import AuctionQueueApplication
        with setup(probe) as state:
            module, game, market = state.module, probe.context.paths.game_db, probe.context.paths.trade_db
            item_id, name = herb(probe, '900002')
            other_id = next(int(key) for key in module.items.get_data_by_item_type('药材') if int(key) != item_id)
            other_name = probe.grant_item(other_id, 2, '900002')
            rules = dict(module.auction_config.get_auction_rules(), max_user_items=1)
            price = rules['min_price']
            app = AuctionQueueApplication(game, market, 10, clock=state.clock)
            with patch.object(module, '_auction_queue_application', return_value=app), \
                    patch.object(module.auction_config, 'get_auction_rules', return_value=rules):
                probe.seed(game, 'UPDATE back SET bind_num=1,state=1 WHERE user_id=? AND goods_id=?', ('900002', other_id))
                before = assets(probe)
                reply = await probe.command('拍卖上架', f'拍卖上架 {other_name} {price}', actor='900002')
                assert 'stock_insufficient' in reply and f'900002:{other_id}' in reply and assets(probe) == before, reply
                await probe.command('拍卖上架', f'拍卖上架 {name} {price}', actor='900002', number=980410)
                before = assets(probe)
                reply = await probe.command('拍卖上架', f'拍卖上架 {other_name} {price}', actor='900002')
                assert 'limit_reached' in reply and assets(probe) == before, reply
                probe.seed(game, 'UPDATE back SET goods_num=10 WHERE user_id=? AND goods_id=?', ('900002', item_id))
                before = assets(probe)
                reply = await probe.command('拍卖下架', f'拍卖下架 {name}', actor='900002', number=980411)
                assert 'inventory_full' in reply and assets(probe) == before, reply
                probe.seed(game, 'UPDATE back SET goods_num=9 WHERE user_id=? AND goods_id=?', ('900002', item_id))
                reply = await probe.command('拍卖下架', f'拍卖下架 {name}', actor='900002', number=980411)
                assert '成功' in reply, reply
                after = assets(probe)
                await probe.command('拍卖下架', f'拍卖下架 {name}', actor='900002', number=980411)
                assert assets(probe) == after
                assert probe.rows(game, 'SELECT goods_num,bind_num FROM back WHERE user_id=? AND goods_id=?',
                                  ('900002', item_id)) == [(10, 1)]
                assert probe.rows(probe.context.paths.player_db,
                    'SELECT "拍卖上架次数","拍卖下架次数" FROM statistics WHERE user_id=\'900002\'') == [(1, 1)]
        integrity(probe)
        probe.evidence.update(ownership=True, capacity=True, stock=True, queue_limit=True,
                              full_rollback=True, same_message_recovery=True, statistics_once=True, integrity='ok')

    @case('auction-scheduled-bid-restart-lifecycle', '拍卖竞拍', '上架/自动开启/参数/竞拍/截止/启动恢复',
          '真实玩家拍品和可控业务时钟，结束回执注入SQLite ABORT',
          '截止前扣款、截止零写，故障整场回滚，重启重试及重放只结算一次', group='market-auction-lifecycle')
    async def lifecycle(probe):
        from apscheduler.events import EVENT_JOB_ERROR, EVENT_JOB_EXECUTED
        with setup(probe) as state:
            module, clock, query = state.module, state.clock, state.query
            item_id, name = herb(probe, '900002')
            price = module.auction_config.get_auction_rules()['min_price']
            probe.player(stone=100_000_000)
            probe.player('900002', stone=100_000_000)
            await probe.command('拍卖上架', f'拍卖上架 {name} {price}', actor='900002')
            assert probe.managers[0].goods_num('900002', item_id) == 9
            event = await fire(module.auto_start_auction_job)
            assert event.code == EVENT_JOB_EXECUTED, str(event.exception)
            assert state.config['last_auto_start_date'] == '2026-11-18'
            session = state.start.get_active_session()
            lot = query.get_current_auction()[0]
            reply = await probe.command('拍卖活动')
            assert '今日 00:00' in reply and '今日 02:00' in reply, reply
            before = assets(probe)
            for text in ('拍卖竞拍', f"拍卖竞拍 {lot['id']} -1", f"拍卖竞拍 {lot['id']} abc"):
                reply = await probe.command('拍卖竞拍', text)
                assert '格式' in reply or '正整数' in reply, reply
                assert assets(probe) == before
            clock.moment += timedelta(hours=2, microseconds=-500000)
            await module.recover_orphan_auction_on_startup()
            event = await fire(module.check_auction_end_job)
            assert event.code == EVENT_JOB_EXECUTED and state.start.get_active_session() == session
            reply = await probe.command('拍卖竞拍', f"拍卖竞拍 {lot['id']} {price}", number=980001)
            assert '竞拍成功' in reply, reply
            game = probe.context.paths.game_db
            assert probe.rows(game, "SELECT stone FROM user_xiuxian WHERE user_id='900001'") == [(100_000_000-price,)]
            after_bid = assets(probe)
            await probe.command('拍卖竞拍', f"拍卖竞拍 {lot['id']} {price}", number=980001)
            assert assets(probe) == after_bid
            clock.moment += timedelta(microseconds=500000)
            reply = await probe.command('拍卖活动')
            assert '竞拍已截止，等待结算' in reply and '今日 02:00' in reply, reply
            reply = await probe.command('拍卖竞拍', f"拍卖竞拍 ID:{lot['id']} {price * 2}", number=980002)
            assert 'auction_expired' in reply and session['session_id'] in reply, reply
            assert assets(probe) == after_bid
            probe.seed(game, "CREATE TRIGGER reject_finish BEFORE INSERT ON auction_session_operations "
                       "WHEN NEW.action='finish' BEGIN SELECT RAISE(ABORT,'isolated restart fault'); END")
            event = await fire(module.recover_orphan_auction_on_startup)
            assert event.code == EVENT_JOB_ERROR and 'isolated restart fault' in str(event.exception)
            assert assets(probe) == after_bid
            probe.seed(game, 'DROP TRIGGER reject_finish')
            clock.moment += timedelta(minutes=5)
            event = await fire(module.recover_orphan_auction_on_startup)
            assert event.code == EVENT_JOB_EXECUTED, str(event.exception)
            assert state.start.get_active_session() is None
            assert probe.managers[0].goods_num('900001', item_id) == 1
            assert probe.managers[0].goods_num('900002', item_id) == 9
            history = query.get_auction_history(lot['id'])[0]
            assert history['winner_id'] == '900001' and history['final_price'] == price
            assert probe.rows(game, "SELECT stone FROM user_xiuxian WHERE user_id='900002'") == [(100_000_000+history['seller_earnings'],)]
            assert probe.rows(probe.context.paths.player_db,
                'SELECT "拍卖出价次数","拍卖出价灵石","拍卖成交次数","拍卖消费灵石" FROM statistics WHERE user_id=\'900001\'') == [(1, price, 1, price)]
            after = assets(probe)
            await module.recover_orphan_auction_on_startup()
            await fire(module.check_auction_end_job)
            await fire(module.auto_start_auction_job)
            await probe.command('拍卖竞拍', f"拍卖竞拍 {lot['id']} {price * 2}")
            reply = await probe.command('拍卖查看', f"拍卖查看 {lot['id']}")
            assert '成交' in reply and assets(probe) == after, reply
        integrity(probe)
        probe.evidence.update(real_dispatch=True, boundary_seconds=0.5, rejected_at_deadline=True,
                              original_startup_exception=True, recovered_after_clock_advance=True,
                              settlement_count=1, statistics_count=1, integrity='ok')

    @case('auction-orphan-start-errors', '开启拍卖', '孤儿拍品/自动开启/管理员拒绝/恢复',
          '隔离场次记录丢失而拍品仍存在', '任务ERROR，玩家和管理员保留具体原因，恢复原场次后正常继续',
          group='market-auction-lifecycle')
    async def orphan(probe):
        from apscheduler.events import EVENT_JOB_ERROR, EVENT_JOB_EXECUTED
        with setup(probe) as state:
            module = state.module
            item_id, name = herb(probe, '900002')
            price = module.auction_config.get_auction_rules()['min_price']
            await probe.command('拍卖上架', f'拍卖上架 {name} {price}', actor='900002')
            state.start.start('test-start', system_items_config={}, duration_hours=2)
            lot = state.query.get_current_auction()[0]
            game = probe.context.paths.game_db
            probe.seed(game, "UPDATE auction_sessions SET status='broken'")
            before = assets(probe)
            event = await fire(module.auto_start_auction_job)
            assert event.code == EVENT_JOB_ERROR and 'auction_session_missing' in str(event.exception)
            reply = await probe.command('拍卖竞拍', f"拍卖竞拍 {lot['id']} {price}")
            assert 'auction_session_missing' in reply and lot['id'] in reply, reply
            with patch.object(module.auction_config, 'update_schedule'):
                reply = await probe.command('开启拍卖', actor='900003')
            assert 'auction_session_missing' in reply and lot['id'] in reply, reply
            assert assets(probe) == before
            probe.seed(game, "UPDATE auction_sessions SET status='active'")
            event = await fire(module.recover_orphan_auction_on_startup)
            assert event.code == EVENT_JOB_EXECUTED
            assert state.query.count_current_auctions() == 1
        integrity(probe)
        probe.evidence.update(orphan_error=True, command_reason_preserved=True, no_new_session=True, integrity='ok')

    @case('auction-start-write-recovery', '开启拍卖', '真实上架/四步拒写/同消息恢复/流拍返还',
          '玩家一件物品已进入等待区，四张表逐一IGNORE且产生触发器副作用',
          '具体原因/主体和整笔回滚；恢复原消息只开一场，实际任务只返还一次',
          group='market-auction-start-write')
    async def start_write(probe):
        from apscheduler.events import EVENT_JOB_EXECUTED
        with setup(probe) as state, patch.object(state.module.auction_config, 'update_schedule'):
            module = state.module
            item_id, name = herb(probe, '900002')
            price = module.auction_config.get_auction_rules()['min_price']
            await probe.command('拍卖上架', f'拍卖上架 {name} {price}', actor='900002')
            game, market = probe.context.paths.game_db, probe.context.paths.trade_db
            for table, reason in (
                ('auction_current', 'auction_start_item_write_rejected'),
                ('auction_player_upload', 'auction_start_queue_write_rejected'),
                ('auction_sessions', 'auction_start_session_write_rejected'),
                ('auction_session_operations', 'auction_start_receipt_write_rejected'),
            ):
                database = market if table == 'auction_player_upload' else game
                event = 'DELETE' if table == 'auction_player_upload' else 'INSERT'
                effect = ("UPDATE auction_player_upload SET user_name='trigger-effect';" if database == market
                          else "UPDATE user_xiuxian SET stone=1 WHERE user_id='900002';")
                probe.seed(database, f'CREATE TRIGGER reject_start BEFORE {event} ON {table} '
                           f'BEGIN {effect} SELECT RAISE(IGNORE); END')
                before = assets(probe)
                reply = await probe.command('开启拍卖', actor='900003', number=980100)
                assert reason in reply and '主体：' in reply and '拍卖已开启' not in reply, reply
                assert assets(probe) == before and state.config['last_auto_start_date'] == ''
                probe.seed(database, 'DROP TRIGGER reject_start')
            state.clock.moment += timedelta(minutes=1)
            reply = await probe.command('开启拍卖', actor='900003', number=980100)
            assert '拍卖已开启' in reply and '02:01' in reply, reply
            assert state.query.count_current_auctions() == 1
            assert not probe.rows(market, 'SELECT * FROM auction_player_upload')
            assert probe.managers[0].goods_num('900002', item_id) == 9
            after = assets(probe)
            await probe.command('开启拍卖', actor='900003', number=980100)
            assert assets(probe) == after
            state.clock.moment += timedelta(hours=2)
            assert (await fire(module.check_auction_end_job)).code == EVENT_JOB_EXECUTED
            assert probe.managers[0].goods_num('900002', item_id) == 10
            assert state.query.count_current_auctions() == 0 and state.start.get_active_session() is None
            after = assets(probe)
            await fire(module.check_auction_end_job)
            assert assets(probe) == after
        integrity(probe)
        probe.evidence.update(rejected_steps=4, two_database_rollback=True, trigger_effects_rolled_back=True,
                              same_message_recovery=True, started_count=1, returned_unsold_once=True, integrity='ok')

    @case('auction-settlement-write-recovery', '结束拍卖', '真实竞拍/六步拒写/任务与管理员/重建恢复',
          '真实玩家成交，六张表逐步IGNORE并产生副作用，另注入SQLite ABORT',
          '具体原因和整场回滚，任务ERROR，同消息恢复只发物/入账/历史/统计一次',
          group='market-auction-settlement-write')
    async def settlement_write(probe):
        import sqlite3
        from apscheduler.events import EVENT_JOB_ERROR, EVENT_JOB_EXECUTED
        from nonebot_plugin_xiuxian_2.features.auction.settlement import AuctionSettlementApplication
        from nonebot_plugin_xiuxian_2.compatibility.auction_settlement_effects import LegacyAuctionSettlementEffects
        from tests.command_probe import run_case
        from tests.parameter_exception_watch import watch_handler_catches
        with setup(probe) as state:
            module = state.module
            item_id, name = herb(probe, '900002')
            price = module.auction_config.get_auction_rules()['min_price']
            probe.player(stone=100_000_000)
            probe.player('900002', stone=100_000_000)
            await probe.command('拍卖上架', f'拍卖上架 {name} {price}', actor='900002')
            assert (await fire(module.auto_start_auction_job)).code == EVENT_JOB_EXECUTED
            lot = state.query.get_current_auction()[0]
            reply = await probe.command('拍卖竞拍', f"拍卖竞拍 {lot['id']} {price}")
            assert '竞拍成功' in reply, reply
            state.clock.moment += timedelta(hours=2)
            game = probe.context.paths.game_db
            for table, event, reason in (
                ('back', 'INSERT', 'auction_settlement_inventory_write_rejected'),
                ('user_xiuxian', 'UPDATE', 'auction_settlement_balance_write_rejected'),
                ('auction_history', 'INSERT', 'auction_settlement_history_write_rejected'),
                ('auction_current', 'DELETE', 'auction_settlement_item_delete_rejected'),
                ('auction_sessions', 'UPDATE', 'auction_settlement_session_write_rejected'),
                ('auction_session_operations', 'INSERT', 'auction_settlement_receipt_write_rejected'),
            ):
                # A backpack side effect also has to disappear with the refused write.
                probe.seed(game, f'CREATE TRIGGER reject_settlement BEFORE {event} ON {table} '
                           "BEGIN UPDATE back SET bind_num=1 WHERE user_id='900002'; SELECT RAISE(IGNORE); END")
                before = assets(probe)
                result = await fire(module.check_auction_end_job)
                assert result.code == EVENT_JOB_ERROR and reason in str(result.exception), str(result.exception)
                reply = await probe.command('结束拍卖', actor='900003', number=980200)
                assert reason in reply and '主体：' in reply and '拍卖已结束' not in reply, reply
                assert assets(probe) == before
                probe.seed(game, 'DROP TRIGGER reject_settlement')
            probe.seed(game, "CREATE TRIGGER reject_settlement BEFORE INSERT ON auction_session_operations "
                       "BEGIN SELECT RAISE(ABORT,'isolated settlement command fault'); END")
            before = assets(probe)
            entry = next(row for row in probe.entries if row['command'] == '结束拍卖')
            with patch.object(module, 'report_exception', return_value='处理失败（定位号：auction-settle-test）') as reported, \
                    watch_handler_catches(entry) as caught:
                result = await run_case(entry, 'parameter', 980200, input_text='结束拍卖', actor='900003')
            assert result['status'] == 'pass' and len(caught) == 1, (result, caught)
            assert 'sqlite3.IntegrityError: isolated settlement command fault' in caught[0]['traceback']
            assert isinstance(reported.call_args.args[1], sqlite3.IntegrityError)
            assert '定位号：auction-settle-test' in ''.join(result['replies'])
            probe.calls.append({'command': '结束拍卖', 'expected_injected_fault': caught, **result})
            assert assets(probe) == before
            probe.seed(game, 'DROP TRIGGER reject_settlement')
            state.clock.moment += timedelta(minutes=5)
            restarted = AuctionSettlementApplication(game, effects=LegacyAuctionSettlementEffects(probe.context.paths.player_db))
            with patch.object(module, '_auction_settlement_application', return_value=restarted):
                reply = await probe.command('结束拍卖', actor='900003', number=980200)
                assert '拍卖已结束' in reply and '成交价' in reply, reply
                assert probe.managers[0].goods_num('900001', item_id) == 1
                assert probe.managers[0].goods_num('900002', item_id) == 9
                history = state.query.get_auction_history(lot['id'])[0]
                assert history['winner_id'] == '900001' and history['final_price'] == price
                assert probe.rows(game, "SELECT stone FROM user_xiuxian WHERE user_id='900001'") == [(100_000_000-price,)]
                assert probe.rows(game, "SELECT stone FROM user_xiuxian WHERE user_id='900002'") == [(100_000_000+history['seller_earnings'],)]
                assert probe.rows(probe.context.paths.player_db,
                    'SELECT "拍卖成交次数","拍卖消费灵石" FROM statistics WHERE user_id=\'900001\'') == [(1, price)]
                assert probe.rows(probe.context.paths.player_db,
                    'SELECT "拍卖售出次数","拍卖收入灵石" FROM statistics WHERE user_id=\'900002\'') == [(1, history['seller_earnings'])]
                after = assets(probe)
                await probe.command('结束拍卖', actor='900003', number=980200)
                await fire(module.check_auction_end_job)
                await fire(module.recover_orphan_auction_on_startup)
                assert assets(probe) == after
        integrity(probe)
        probe.evidence.update(rejected_steps=6, scheduler_errors=True, original_unknown_exception=True,
                              full_rollback=True, restart_recovery=True, assets_and_statistics_once=True, integrity='ok')

    @case('auction-settlement-pending-recovery', '结束拍卖', '成交与流拍/满背包寄存拒写/真实命令恢复',
          '两位真实卖家各上架一件，成交买家与流拍卖家背包达到注入上限',
          '寄存失败整场回滚，解除后各保留一件待领取，回复不把成交误报流拍',
          group='market-auction-settlement-write')
    async def pending_write(probe):
        from nonebot_plugin_xiuxian_2.features.auction.settlement import AuctionSettlementApplication
        from nonebot_plugin_xiuxian_2.compatibility.auction_settlement_effects import LegacyAuctionSettlementEffects
        with setup(probe) as state:
            module = state.module
            item_id, name = herb(probe)
            herb(probe, '900002')
            herb(probe, '900003')
            probe.player(stone=100_000_000)
            price = module.auction_config.get_auction_rules()['min_price']
            for seller in ('900002', '900003'):
                reply = await probe.command('拍卖上架', f'拍卖上架 {name} {price}', actor=seller)
                assert '成功' in reply, reply
            await fire(module.auto_start_auction_job)
            lot = next(row for row in state.query.get_current_auction() if row['seller_id'] == '900002')
            reply = await probe.command('拍卖竞拍', f"拍卖竞拍 {lot['id']} {price}")
            assert '竞拍成功' in reply, reply
            game = probe.context.paths.game_db
            app = AuctionSettlementApplication(game, max_goods_num=9,
                                               effects=LegacyAuctionSettlementEffects(probe.context.paths.player_db))
            with patch.object(module, '_auction_settlement_application', return_value=app):
                state.clock.moment += timedelta(hours=2)
                probe.seed(game, 'CREATE TRIGGER reject_pending BEFORE INSERT ON auction_pending_items '
                           'BEGIN SELECT RAISE(IGNORE); END')
                before = assets(probe)
                reply = await probe.command('结束拍卖', actor='900003', number=980210)
                assert 'auction_settlement_pending_write_rejected' in reply and '主体：' in reply, reply
                assert assets(probe) == before
                probe.seed(game, 'DROP TRIGGER reject_pending')
                state.clock.moment += timedelta(minutes=5)
                reply = await probe.command('结束拍卖', actor='900003', number=980210)
                assert '拍卖已结束' in reply and '成交价' in reply and reply.count('待领取') == 2, reply
                assert probe.rows(game, 'SELECT owner_id,role,claimed_at FROM auction_pending_items ORDER BY owner_id') == [
                    ('900001', 'winner', None), ('900003', 'seller', None)]
                assert probe.managers[0].goods_num('900001', item_id) == 10
                assert probe.managers[0].goods_num('900003', item_id) == 9
                assert probe.rows(game, 'SELECT COUNT(*) FROM auction_history') == [(2,)]
                assert probe.rows(probe.context.paths.player_db,
                    'SELECT "拍卖成交次数" FROM statistics WHERE user_id=\'900001\'') == [(1,)]
                assert probe.rows(probe.context.paths.player_db,
                    'SELECT "拍卖流拍次数" FROM statistics WHERE user_id=\'900003\'') == [(1,)]
                after = assets(probe)
                await probe.command('结束拍卖', actor='900003', number=980210)
                await fire(module.check_auction_end_job)
                assert assets(probe) == after
        integrity(probe)
        probe.evidence.update(pending_refusal_rollback=True, pending_winner_and_seller=True,
                              accurate_command_reply=True, pending_items=2, integrity='ok')

    @case('auction-claim-real-settlement-recovery', '我的拍卖', '真实成交寄存/全领与指定/部分满背包/拒写恢复',
          '两件成交与一件流拍通过真实到期任务寄存，玩家背包部分释放空间',
          '三步拒写与未知异常整笔回滚，重建同消息只发一次，归属/旧满包回放/绑定数正确',
          group='market-auction-claim')
    async def claim_lifecycle(probe):
        import sqlite3
        from apscheduler.events import EVENT_JOB_EXECUTED
        from nonebot_plugin_xiuxian_2.features.auction.pending import AuctionPendingApplication
        from nonebot_plugin_xiuxian_2.features.auction.settlement import AuctionSettlementApplication
        from nonebot_plugin_xiuxian_2.compatibility.auction_settlement_effects import LegacyAuctionSettlementEffects
        from tests.command_probe import run_case
        from tests.parameter_exception_watch import watch_handler_catches
        with setup(probe) as state:
            module = state.module
            item_id, name = herb(probe)
            herb(probe, '900002')
            herb(probe, '900003')
            other_id = next(int(key) for key in module.items.get_data_by_item_type('药材') if int(key) != item_id)
            other_name = probe.grant_item(other_id, 10, '900001')
            probe.grant_item(other_id, 10, '900002')
            probe.player(stone=100_000_000)
            price = module.auction_config.get_auction_rules()['min_price']
            for seller, label in (('900002', name), ('900002', other_name), ('900003', name)):
                reply = await probe.command('拍卖上架', f'拍卖上架 {label} {price}', actor=seller)
                assert '成功' in reply, reply
            assert (await fire(module.auto_start_auction_job)).code == EVENT_JOB_EXECUTED
            lots = state.query.get_current_auction()
            sold = {int(row['item_id']): row['id'] for row in lots if row['seller_id'] == '900002'}
            unsold = next(row['id'] for row in lots if row['seller_id'] == '900003')
            assert len(sold) == 2
            for auction_id in sold.values():
                reply = await probe.command('拍卖竞拍', f'拍卖竞拍 {auction_id} {price}')
                assert '竞拍成功' in reply, reply
            game = probe.context.paths.game_db
            settlement = AuctionSettlementApplication(game, max_goods_num=9,
                effects=LegacyAuctionSettlementEffects(probe.context.paths.player_db))
            app = AuctionPendingApplication(game, max_goods_num=10, clock=state.clock)
            with patch.object(module, '_auction_settlement_application', return_value=settlement), \
                    patch.object(module, '_auction_pending_application', return_value=app):
                state.clock.moment += timedelta(hours=2)
                assert (await fire(module.check_auction_end_job)).code == EVENT_JOB_EXECUTED
                assert probe.rows(game, 'SELECT COUNT(*) FROM auction_pending_items WHERE claimed_at IS NULL') == [(3,)]
                reply = await probe.command('我的拍卖')
                assert '拍卖待领取' in reply and all(auction_id in reply for auction_id in sold.values()), reply
                reply = await probe.command('我的拍卖', '我的拍卖 领取', number=980300)
                assert '背包已满' in reply, reply
                probe.seed(game, "UPDATE back SET goods_num=9 WHERE user_id='900001' AND goods_id=?", (item_id,))
                before = assets(probe)
                reply = await probe.command('我的拍卖', '我的拍卖 领取', number=980300)
                assert '此前因背包已满' in reply and '已领取 0' not in reply and assets(probe) == before, reply
                reply = await probe.command('我的拍卖', f'我的拍卖 领取 {unsold}')
                assert '未找到' in reply and assets(probe) == before, reply
                for table, event, reason in (
                    ('back', 'INSERT', 'auction_claim_inventory_write_rejected'),
                    ('auction_pending_items', 'UPDATE', 'auction_claim_pending_write_rejected'),
                    ('auction_pending_claim_operations', 'INSERT', 'auction_claim_receipt_write_rejected'),
                ):
                    probe.seed(game, f'CREATE TRIGGER reject_claim BEFORE {event} ON {table} '
                               "BEGIN UPDATE back SET bind_num=1 WHERE user_id='900003'; SELECT RAISE(IGNORE); END")
                    before = assets(probe)
                    reply = await probe.command('我的拍卖', '我的拍卖 领取', number=980301)
                    assert reason in reply and '主体：' in reply and '已领取' not in reply, reply
                    assert assets(probe) == before
                    probe.seed(game, 'DROP TRIGGER reject_claim')
                probe.seed(game, "CREATE TRIGGER reject_claim BEFORE INSERT ON auction_pending_claim_operations "
                           "BEGIN SELECT RAISE(ABORT,'isolated claim command fault'); END")
                entry = next(row for row in probe.entries if row['command'] == '我的拍卖')
                before = assets(probe)
                with patch.object(module, 'report_exception', return_value='处理失败（定位号：auction-claim-test）') as reported, \
                        watch_handler_catches(entry) as caught:
                    result = await run_case(entry, 'parameter', 980301, input_text='我的拍卖 领取', actor='900001')
                assert result['status'] == 'pass' and len(caught) == 1, (result, caught)
                assert 'sqlite3.IntegrityError: isolated claim command fault' in caught[0]['traceback']
                assert isinstance(reported.call_args.args[1], sqlite3.IntegrityError)
                assert '定位号：auction-claim-test' in ''.join(result['replies'])
                probe.calls.append({'command': '我的拍卖', 'expected_injected_fault': caught, **result})
                assert assets(probe) == before
                probe.seed(game, 'DROP TRIGGER reject_claim')
                restarted = AuctionPendingApplication(game, max_goods_num=10, clock=state.clock)
                with patch.object(module, '_auction_pending_application', return_value=restarted):
                    state.clock.moment += timedelta(days=1)
                    reply = await probe.command('我的拍卖', '我的拍卖 领取', number=980301)
                    assert '已领取 1 件' in reply and '背包已满未领取' in reply and '仍有 1 件' in reply, reply
                    assert probe.rows(game, 'SELECT goods_num,bind_num FROM back WHERE user_id=? AND goods_id=?',
                                      ('900001', item_id)) == [(10, 1)]
                    assert probe.managers[0].goods_num('900001', other_id) == 10
                    after = assets(probe)
                    await probe.command('我的拍卖', '我的拍卖 领取', number=980301)
                    assert assets(probe) == after
                    probe.seed(game, "UPDATE back SET goods_num=9 WHERE user_id='900001' AND goods_id=?", (other_id,))
                    reply = await probe.command('我的拍卖', f'我的拍卖 领取 {sold[other_id]}', number=980302)
                    assert '已领取 1 件' in reply and sold[other_id] in reply, reply
                    assert probe.rows(game, 'SELECT goods_num,bind_num FROM back WHERE user_id=? AND goods_id=?',
                                      ('900001', other_id)) == [(10, 1)]
                    after = assets(probe)
                    await probe.command('我的拍卖', f'我的拍卖 领取 {sold[other_id]}', number=980302)
                    assert assets(probe) == after
                    reply = await probe.command('我的拍卖', f'我的拍卖 领取 {unsold}', actor='900003')
                    assert '流拍' in reply and '已领取 1 件' in reply, reply
                    assert probe.rows(game, 'SELECT goods_num,bind_num FROM back WHERE user_id=? AND goods_id=?',
                                      ('900003', item_id)) == [(10, 1)]
                    assert not probe.rows(game, 'SELECT id FROM auction_pending_items WHERE claimed_at IS NULL')
                    assert probe.rows(probe.context.paths.player_db,
                        'SELECT "拍卖成交次数" FROM statistics WHERE user_id=\'900001\'') == [(2,)]
        integrity(probe)
        probe.evidence.update(real_settlement_pending=3, rejected_steps=3, old_blocked_replay_preserved=True,
                              full_rollback=True, original_unknown_exception=True, restart_recovery=True,
                              partial_and_scoped_claim=True, ownership_and_bindings=True, integrity='ok')
