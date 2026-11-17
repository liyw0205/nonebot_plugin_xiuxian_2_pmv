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
