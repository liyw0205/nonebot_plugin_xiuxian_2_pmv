"""Auction jobs, durable state and bid windows share a controllable clock."""
import asyncio
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import patch
from zoneinfo import ZoneInfo

import pytest
from apscheduler.events import EVENT_JOB_ERROR, EVENT_JOB_EXECUTED
from apscheduler.schedulers.asyncio import AsyncIOScheduler

from tests.test_guishi_schedule import trade, FixedClock
from nonebot_plugin_xiuxian_2.bootstrap import legacy
from nonebot_plugin_xiuxian_2.compatibility.legacy_jobs import _TARGETS
from nonebot_plugin_xiuxian_2.features.auction.domain import session_window
from nonebot_plugin_xiuxian_2.features.auction.application import AuctionBidApplication
from nonebot_plugin_xiuxian_2.features.auction.migrations import (
    apply_auction_settlement, apply_auction_bid_operations,
    apply_auction_player_queue, apply_auction_pending_delivery,
)
from nonebot_plugin_xiuxian_2.features.auction.query_application import AuctionQueryApplication
from nonebot_plugin_xiuxian_2.features.auction.session_start_application import AuctionSessionStartApplication
from nonebot_plugin_xiuxian_2.features.auction.settlement import AuctionSettlementApplication
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork
from nonebot_plugin_xiuxian_2.plugin import apply_platform_schema


@pytest.fixture
def auction(tmp_path):
    game, market = tmp_path / 'game.db', tmp_path / 'trade.db'
    with DatabaseUnitOfWork(game) as uow:
        apply_platform_schema(uow)
        apply_auction_settlement(uow)
        apply_auction_bid_operations(uow)
        apply_auction_pending_delivery(uow)
        uow.execute('CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,user_name TEXT,stone INTEGER)')
        uow.execute("INSERT INTO user_xiuxian VALUES('buyer','buyer',1000)")
        uow.execute('CREATE TABLE back(user_id TEXT,goods_id INTEGER,goods_name TEXT,goods_type TEXT,'
                    'goods_num INTEGER,create_time TEXT,update_time TEXT,bind_num INTEGER DEFAULT 0,'
                    'UNIQUE(user_id,goods_id))')
    with DatabaseUnitOfWork(market) as uow:
        apply_auction_player_queue(uow)
    clock = FixedClock(datetime(2026, 11, 17, 16, tzinfo=timezone.utc))
    start = AuctionSessionStartApplication(game, market, clock=clock)
    query = AuctionQueryApplication(game)
    config = {'enabled': True, 'duration_hours': 2, 'last_auto_start_date': ''}
    system = {'herb': {'id': 1001, 'start_price': 100}}
    def save(section, value, key):
        config[key] = value
    with patch.object(trade, 'runtime_clock', clock), \
            patch.object(trade, 'scheduler', SimpleNamespace(timezone=ZoneInfo('Asia/Shanghai'))), \
            patch.object(trade, '_auction_session_start_application', return_value=start), \
            patch.object(trade, '_auction_session_service', return_value=start), \
            patch.object(trade, '_auction_query_application', return_value=query), \
            patch.object(trade, '_auction_settlement_application', return_value=AuctionSettlementApplication(game)), \
            patch.object(trade.auction_config, 'get_system_items', return_value=system), \
            patch.object(trade.auction_config, 'get_auction_schedule', return_value=config), \
            patch.object(trade.auction_config, 'set_auction_config_value', side_effect=save), \
            patch.object(trade.items, 'get_data_by_item_id', return_value={'type': '药材'}):
        yield SimpleNamespace(game=game, market=market, clock=clock, start=start, query=query,
                              config=config, system=system, bid=AuctionBidApplication(game))


async def fire(callback):
    scheduler = AsyncIOScheduler(timezone='Asia/Shanghai')
    event = asyncio.get_running_loop().create_future()
    scheduler.add_listener(lambda value: event.set_result(value), EVENT_JOB_ERROR | EVENT_JOB_EXECUTED)
    scheduler.add_job(callback, 'date')
    scheduler.start()
    try:
        return await asyncio.wait_for(event, timeout=3)
    finally:
        scheduler.shutdown(wait=False)
        await asyncio.sleep(0)


def start_session(auction, operation='start'):
    return auction.start.start(operation, system_items_config=auction.system, duration_hours=2)


def test_live_job_registration_cron_and_startup_callback():
    bridge = trade.scheduler
    if hasattr(bridge, 'activate'):
        bridge.activate()
    scheduler = getattr(bridge, '_scheduler', bridge)
    for job_id, callback in (('auto_start_auction', 'auto_start_auction_job'),
                             ('check_auction_end', 'check_auction_end_job')):
        assert _TARGETS[job_id] == ('xiuxian.xiuxian_trade', callback)
        assert scheduler.get_job(job_id) is not None
    assert trade.recover_orphan_auction_on_startup in legacy.startup_callbacks()
    config = trade.auction_config.get_auction_schedule()
    boundary = datetime(2026, 11, 18, config['start_hour'], config['start_minute'], tzinfo=scheduler.timezone)
    trigger = scheduler.get_job('auto_start_auction').trigger
    assert trigger.get_next_fire_time(None, boundary - timedelta(microseconds=1)) == boundary
    assert trigger.get_next_fire_time(boundary, boundary) == boundary + timedelta(days=1)
    assert scheduler.get_job('check_auction_end').trigger.interval == timedelta(minutes=5)


def test_end_window_does_not_round_positive_fraction_down_to_expired():
    now = datetime(2026, 11, 17, tzinfo=timezone.utc)
    session = {'end_time': (now + timedelta(seconds=0.5)).timestamp()}
    assert session_window(session, now)['expired'] is False
    assert session_window(session, now + timedelta(seconds=0.5))['expired'] is True


def test_auto_start_uses_scheduler_day_and_replays_without_extra_lots(auction):
    event = asyncio.run(fire(trade.auto_start_auction_job))
    assert event.code == EVENT_JOB_EXECUTED
    assert auction.config['last_auto_start_date'] == '2026-11-18'
    assert auction.start.get_start_operation('auction-session:auto-start:2026-11-18').succeeded
    session = auction.start.get_active_session()
    asyncio.run(fire(trade.auto_start_auction_job))
    assert auction.start.get_active_session() == session and auction.query.count_current_auctions() == 1


def test_empty_and_disabled_start_are_normal_skips(auction):
    auction.system.clear()
    assert asyncio.run(fire(trade.auto_start_auction_job)).code == EVENT_JOB_EXECUTED
    assert auction.start.get_active_session() is None and auction.config['last_auto_start_date'] == ''
    auction.system['herb'] = {'id': 1001, 'start_price': 100}
    auction.config['enabled'] = False
    assert asyncio.run(fire(trade.auto_start_auction_job)).code == EVENT_JOB_EXECUTED
    assert auction.start.get_active_session() is None


def test_auto_start_database_error_rolls_back_queue_and_recovers(auction):
    with DatabaseUnitOfWork(auction.market) as uow:
        uow.execute("INSERT INTO auction_player_upload VALUES('buyer',1002,'herb2',200,'buyer')")
    with DatabaseUnitOfWork(auction.game) as uow:
        uow.execute("CREATE TRIGGER reject_start BEFORE INSERT ON auction_session_operations "
                    "BEGIN SELECT RAISE(ABORT,'isolated start fault'); END")
    event = asyncio.run(fire(trade.auto_start_auction_job))
    assert event.code == EVENT_JOB_ERROR and 'isolated start fault' in str(event.exception)
    assert auction.start.get_active_session() is None and auction.query.count_current_auctions() == 0
    assert auction.config['last_auto_start_date'] == ''
    with DatabaseUnitOfWork(auction.market, read_only=True) as uow:
        assert uow.query_one('SELECT COUNT(*) AS n FROM auction_player_upload')['n'] == 1
    with DatabaseUnitOfWork(auction.game) as uow:
        assert uow.query_one('SELECT COUNT(*) AS n FROM auction_session_operations')['n'] == 0
        uow.execute('DROP TRIGGER reject_start')
    auction.clock.moment += timedelta(minutes=1)
    assert asyncio.run(fire(trade.auto_start_auction_job)).code == EVENT_JOB_EXECUTED
    assert auction.query.count_current_auctions() == 2
    with DatabaseUnitOfWork(auction.market, read_only=True) as uow:
        assert uow.query_one('SELECT COUNT(*) AS n FROM auction_player_upload')['n'] == 0


def test_end_check_reports_orphan_lots_as_scheduler_error(auction):
    start_session(auction)
    with DatabaseUnitOfWork(auction.game) as uow:
        uow.execute('DELETE FROM auction_sessions')
    event = asyncio.run(fire(trade.check_auction_end_job))
    assert event.code == EVENT_JOB_ERROR and 'auction_session_missing' in str(event.exception)
    assert auction.query.count_current_auctions() == 1


@pytest.mark.parametrize('callback', ('check_auction_end_job', 'recover_orphan_auction_on_startup'))
def test_active_session_without_lots_does_not_report_success(auction, callback):
    result = start_session(auction)
    with DatabaseUnitOfWork(auction.game) as uow:
        uow.execute('DELETE FROM auction_current')
    event = asyncio.run(fire(getattr(trade, callback)))
    assert event.code == EVENT_JOB_ERROR and 'auction_items_missing' in str(event.exception)
    assert result.session_id in str(event.exception)


def test_web_bid_uses_server_clock_not_supplied_past_timestamp(auction):
    from nonebot_plugin_xiuxian_2.bootstrap import build_runtime_context
    from nonebot_plugin_xiuxian_2.adapters.web.app import create_app
    session = start_session(auction)
    lot = auction.query.get_current_auction()[0]
    context = build_runtime_context(data_dir=auction.game.parent / 'web', legacy_startup=False)
    context.clock = auction.clock
    context.services = {'auction': auction.bid}
    client = create_app(context=context).test_client()
    token = client.get('/api/v1/csrf').get_json()['data']['token']
    auction.clock.moment += timedelta(hours=2)
    response = client.post('/api/v1/auction/bids', headers={
        'X-User-ID': 'buyer', 'X-CSRF-Token': token, 'Idempotency-Key': 'late-web-bid',
    }, json={'auction_id': lot['id'], 'bidder_id': 'buyer', 'bid_price': 100,
             'expected_price': 100, 'expected_bids': {}, 'bid_time': session.start_time})
    assert response.status_code == 409
    assert response.get_json()['data']['data']['reason'] == 'auction_expired'
    with DatabaseUnitOfWork(auction.game, read_only=True) as uow:
        assert uow.query_one('SELECT stone FROM user_xiuxian')['stone'] == 1000


def test_startup_failure_reaches_lifecycle_bridge_and_retries(auction):
    result = start_session(auction)
    with DatabaseUnitOfWork(auction.game) as uow:
        uow.execute("UPDATE auction_sessions SET status='broken'")
    with patch.object(legacy, '_startup', [trade.recover_orphan_auction_on_startup]), \
            patch.object(legacy, '_shutdown', []), patch.object(legacy, '_started', False):
        with pytest.raises(Exception, match='auction_session_missing'):
            asyncio.run(legacy.run_legacy_startup())
        assert legacy._started is False
        with DatabaseUnitOfWork(auction.game) as uow:
            uow.execute("UPDATE auction_sessions SET status='active'")
        asyncio.run(legacy.run_legacy_startup())
        assert legacy._started is True
    assert auction.start.get_active_session()['session_id'] == result.session_id


def test_previous_finished_start_receipt_is_error_not_empty_auction(auction):
    auction.clock.moment = datetime(2026, 11, 18, 12, tzinfo=timezone.utc)
    operation = 'auction-session:auto-start:2026-11-18'
    start_session(auction, operation)
    with DatabaseUnitOfWork(auction.game) as uow:
        uow.execute("UPDATE auction_sessions SET status='settled'")
        uow.execute('DELETE FROM auction_current')
    event = asyncio.run(fire(trade.auto_start_auction_job))
    assert event.code == EVENT_JOB_ERROR and 'auction_start_session_inactive' in str(event.exception)
    assert auction.query.count_current_auctions() == 0


@pytest.mark.parametrize('offset,expected', ((-0.5, 'bid'), (0, 'auction_expired'), (0.5, 'auction_expired')))
def test_bid_checks_durable_deadline_before_debit(auction, offset, expected):
    session = start_session(auction)
    lot = auction.query.get_current_auction()[0]
    outcome = auction.bid.place_bid(operation_id='bid', auction_id=lot['id'], bidder_id='buyer',
        bid_price=100, expected_price=100, expected_bids={}, bid_time=session.end_time + offset)
    assert outcome.data['status'] == expected
    with DatabaseUnitOfWork(auction.game, read_only=True) as uow:
        assert uow.query_one("SELECT stone FROM user_xiuxian")['stone'] == (900 if expected == 'bid' else 1000)
        assert uow.query_one('SELECT COUNT(*) AS n FROM auction_bid_operations')['n'] == (expected == 'bid')


@pytest.mark.parametrize('state,reason', (('future', 'auction_not_started'), ('missing', 'auction_session_missing'),
                                        ('text', 'auction_session_time_invalid'), ('infinite', 'auction_session_time_invalid'),
                                        ('invalid', 'auction_session_time_invalid')))
def test_bid_unusable_session_is_explicit_and_does_not_debit(auction, state, reason):
    session = start_session(auction)
    lot = auction.query.get_current_auction()[0]
    with DatabaseUnitOfWork(auction.game) as uow:
        if state == 'missing':
            uow.execute('DELETE FROM auction_sessions')
        elif state == 'invalid':
            uow.execute('UPDATE auction_sessions SET end_time=start_time')
        elif state in {'text', 'infinite'}:
            uow.execute('UPDATE auction_sessions SET end_time=?', ('bad-time' if state == 'text' else float('inf'),))
    outcome = auction.bid.place_bid(operation_id='bid', auction_id=lot['id'], bidder_id='buyer',
        bid_price=100, expected_price=100, expected_bids={}, bid_time=session.start_time - 0.5)
    assert outcome.code == reason and outcome.data['reason'] == reason
    assert outcome.data['subject'] == (lot['id'] if state == 'missing' else session.session_id)
    with DatabaseUnitOfWork(auction.game, read_only=True) as uow:
        assert uow.query_one('SELECT stone FROM user_xiuxian')['stone'] == 1000
        assert uow.query_one('SELECT COUNT(*) AS n FROM auction_bid_operations')['n'] == 0


def test_successful_bid_receipt_replays_after_deadline_without_new_debit(auction):
    session = start_session(auction)
    lot = auction.query.get_current_auction()[0]
    kwargs = dict(operation_id='bid', auction_id=lot['id'], bidder_id='buyer', bid_price=100,
                  expected_price=100, expected_bids={})
    assert auction.bid.place_bid(**kwargs, bid_time=session.start_time).ok
    replay = auction.bid.place_bid(**kwargs, bid_time=session.end_time + 1)
    assert replay.ok and replay.replayed
    with DatabaseUnitOfWork(auction.game, read_only=True) as uow:
        assert uow.query_one('SELECT stone FROM user_xiuxian')['stone'] == 900
        assert uow.query_one('SELECT COUNT(*) AS n FROM auction_bid_operations')['n'] == 1


def test_scheduled_finish_recovers_after_database_failure_and_clock_advances(auction):
    session = start_session(auction)
    auction.clock.moment += timedelta(hours=2)
    with DatabaseUnitOfWork(auction.game) as uow:
        uow.execute("CREATE TRIGGER reject_finish BEFORE INSERT ON auction_session_operations "
                    "WHEN NEW.action='finish' BEGIN SELECT RAISE(ABORT,'isolated finish fault'); END")
    event = asyncio.run(fire(trade.check_auction_end_job))
    assert event.code == EVENT_JOB_ERROR and 'isolated finish fault' in str(event.exception)
    assert auction.start.get_active_session()['session_id'] == session.session_id
    assert auction.query.count_current_auctions() == 1
    with DatabaseUnitOfWork(auction.game) as uow:
        assert uow.query_one('SELECT COUNT(*) AS n FROM auction_history')['n'] == 0
        uow.execute('DROP TRIGGER reject_finish')
    auction.clock.moment += timedelta(minutes=5)
    event = asyncio.run(fire(trade.check_auction_end_job))
    assert event.code == EVENT_JOB_EXECUTED, str(event.exception)
    assert auction.start.get_active_session() is None and auction.query.count_current_auctions() == 0
    asyncio.run(trade.recover_orphan_auction_on_startup())
    asyncio.run(fire(trade.check_auction_end_job))
    with DatabaseUnitOfWork(auction.game, read_only=True) as uow:
        assert uow.query_one('SELECT COUNT(*) AS n FROM auction_history')['n'] == 1
        assert uow.query_all('PRAGMA integrity_check') == [{'integrity_check': 'ok'}]
