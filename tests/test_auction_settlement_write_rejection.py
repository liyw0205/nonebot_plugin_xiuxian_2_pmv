"""Settlement must survive rejected writes without closing an unpaid session."""
import asyncio
import json
import sqlite3
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import patch

import pytest
from apscheduler.events import EVENT_JOB_ERROR, EVENT_JOB_EXECUTED

from tests.test_auction_settlement_delivery import AuctionDeliveryFixture
from tests.test_auction_schedule import trade, fire
from nonebot_plugin_xiuxian_2.features.auction.query_application import AuctionQueryApplication
from nonebot_plugin_xiuxian_2.features.auction.session_start_application import AuctionSessionStartApplication
from nonebot_plugin_xiuxian_2.features.auction.settlement import AuctionSettlementApplication
from nonebot_plugin_xiuxian_2.features.auction.settlement_repository import AuctionSettlementSqlRepository
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork
from nonebot_plugin_xiuxian_2.plugin import apply_platform_schema


TARGETS = (
    ('back', 'auction_settlement_inventory_write_rejected'),
    ('seller', 'auction_settlement_balance_write_rejected'),
    ('loser', 'auction_settlement_balance_write_rejected'),
    ('auction_pending_items', 'auction_settlement_pending_write_rejected'),
    ('auction_history', 'auction_settlement_history_write_rejected'),
    ('auction_current', 'auction_settlement_item_delete_rejected'),
    ('auction_sessions', 'auction_settlement_session_write_rejected'),
    ('auction_session_operations', 'auction_settlement_receipt_write_rejected'),
)


@pytest.fixture
def settlement(tmp_path):
    fixture = AuctionDeliveryFixture(tmp_path)
    fixture.add_auction(bids={'winner': 900, 'loser': 700})
    with DatabaseUnitOfWork(fixture.database) as uow:
        apply_platform_schema(uow)
    return fixture


def snapshot(fixture):
    with DatabaseUnitOfWork(fixture.database, read_only=True) as uow:
        return [uow.query_all(f'SELECT * FROM {table} ORDER BY rowid') for table in (
            'user_xiuxian', 'back', 'auction_pending_items', 'auction_history', 'auction_current',
            'auction_sessions', 'auction_session_operations', 'domain_outbox')]


def install_fault(fixture, target, mode):
    if target == 'auction_pending_items':
        fixture.give('winner', 1001, 99)
    table = 'user_xiuxian' if target in {'seller', 'loser'} else target
    event = 'UPDATE' if target in {'seller', 'loser', 'auction_sessions'} else 'DELETE' if target == 'auction_current' else 'INSERT'
    condition = f"WHEN NEW.user_id='{target}'" if table == 'user_xiuxian' else ''
    if mode == 'ignore':
        body = "UPDATE user_xiuxian SET stone=1 WHERE user_id='winner'; SELECT RAISE(IGNORE);"
        timing = 'BEFORE'
    else:
        timing = 'AFTER'
        body = {
            'back': 'UPDATE back SET goods_num=goods_num+1 WHERE user_id=NEW.user_id AND goods_id=NEW.goods_id;',
            'seller': "UPDATE user_xiuxian SET stone=stone+1 WHERE user_id='seller';",
            'loser': "UPDATE user_xiuxian SET stone=stone+1 WHERE user_id='loser';",
            'auction_pending_items': "UPDATE auction_pending_items SET owner_id='other' WHERE id=NEW.id;",
            'auction_history': "UPDATE auction_history SET final_price=1 WHERE id=NEW.id;",
            'auction_current': 'INSERT INTO auction_current VALUES(OLD.id,OLD.item_id,OLD.name,OLD.start_price,OLD.current_price,OLD.seller_id,OLD.seller_name,OLD.bids,OLD.bid_times,OLD.is_system,OLD.last_bid_time);',
            'auction_sessions': "UPDATE auction_sessions SET status='active' WHERE session_id=NEW.session_id;",
            'auction_session_operations': "UPDATE auction_session_operations SET result='{}' WHERE operation_id=NEW.operation_id;",
        }[target] + " UPDATE user_xiuxian SET stone=1 WHERE user_id='winner';"
    with DatabaseUnitOfWork(fixture.database) as uow:
        uow.execute(f'CREATE TRIGGER reject_settlement {timing} {event} ON {table} {condition} BEGIN {body} END')


@pytest.mark.parametrize('target,reason', TARGETS)
@pytest.mark.parametrize('mode', ('ignore', 'rewrite'))
def test_whole_session_rolls_back_on_rejected_write_and_recovers(settlement, target, reason, mode):
    install_fault(settlement, target, mode)
    repository = AuctionSettlementSqlRepository(settlement.database, max_goods_num=99)
    before = snapshot(settlement)
    result = repository.settle_active('settle', end_time=300, fee_rate=0.1, item_types={1001: '装备'})
    assert not result.succeeded and getattr(result, 'reason', '') == reason, result
    assert result.subject and snapshot(settlement) == before
    with DatabaseUnitOfWork(settlement.database) as uow:
        uow.execute('DROP TRIGGER reject_settlement')
    result = repository.settle_active('settle', end_time=300, fee_rate=0.1, item_types={1001: '装备'})
    assert result.succeeded and settlement.status() == 'settled'
    assert settlement.stone('seller') == 810 and settlement.stone('loser') == 1200
    assert settlement.stone('winner') == 1000
    assert settlement.copies('winner', 1001) == (99 if target == 'auction_pending_items' else 1)
    assert len(settlement.pending()) == (target == 'auction_pending_items')
    after = snapshot(settlement)
    assert repository.settle_active('settle', end_time=500, fee_rate=0.1, item_types={1001: '装备'}).status == 'duplicate'
    assert snapshot(settlement) == after
    with DatabaseUnitOfWork(settlement.database, read_only=True) as uow:
        assert uow.query_all('PRAGMA integrity_check') == [{'integrity_check': 'ok'}]


@pytest.mark.parametrize('callback', ('check_auction_end_job', 'recover_orphan_auction_on_startup'))
def test_rejected_task_retains_reason_and_retryable_ledger(settlement, callback):
    install_fault(settlement, 'auction_session_operations', 'ignore')
    clock = SimpleNamespace(now=lambda: datetime.fromtimestamp(300, tz=timezone.utc))
    start = AuctionSessionStartApplication(settlement.database, settlement.root / 'trade.db')
    app = AuctionSettlementApplication(settlement.database)
    before = snapshot(settlement)
    with patch.object(trade, 'runtime_clock', clock), \
            patch.object(trade, '_auction_session_service', return_value=start), \
            patch.object(trade, '_auction_query_application', return_value=AuctionQueryApplication(settlement.database)), \
            patch.object(trade, '_auction_settlement_application', return_value=app), \
            patch.object(trade.items, 'get_data_by_item_id', return_value={'type': '装备'}), \
            patch.object(trade.auction_config, 'get_auction_rules', return_value={'fee_rate': 0.1}):
        event = asyncio.run(fire(getattr(trade, callback)))
        assert event.code == EVENT_JOB_ERROR and 'auction_settlement_receipt_write_rejected' in str(event.exception)
        assert snapshot(settlement) == before
        with DatabaseUnitOfWork(settlement.database) as uow:
            ledger = uow.query_one('SELECT status,result_json FROM operation_ledger')
            assert ledger['status'] == 'failed' and 'auction_settlement_receipt_write_rejected' in ledger['result_json']
            uow.execute('DROP TRIGGER reject_settlement')
        clock.now = lambda: datetime.fromtimestamp(600, tz=timezone.utc)
        event = asyncio.run(fire(getattr(trade, callback)))
        assert event.code == EVENT_JOB_EXECUTED, str(event.exception)
        assert settlement.status() == 'settled' and settlement.stone('seller') == 810
        after = snapshot(settlement)
        asyncio.run(fire(getattr(trade, callback)))
        assert snapshot(settlement) == after


def test_later_item_failure_rolls_back_prior_assets_then_accumulates_once(settlement):
    settlement.add_auction(auction_id='auction-2', bids={'winner': 900, 'loser': 700})
    settlement.give('winner', 1001, 2)
    with DatabaseUnitOfWork(settlement.database) as uow:
        uow.execute("UPDATE back SET bind_num=1")
        uow.execute("CREATE TRIGGER reject_settlement BEFORE INSERT ON auction_history "
                    "WHEN NEW.auction_id='auction-2' BEGIN SELECT RAISE(IGNORE); END")
    before = snapshot(settlement)
    repository = AuctionSettlementSqlRepository(settlement.database)
    result = repository.settle_active('settle', end_time=300, fee_rate=0.1, item_types={1001: '装备'})
    assert result.reason == 'auction_settlement_history_write_rejected' and result.subject == 'auction-2'
    assert snapshot(settlement) == before
    with DatabaseUnitOfWork(settlement.database) as uow:
        uow.execute('DROP TRIGGER reject_settlement')
    assert repository.settle_active('settle', end_time=300, fee_rate=0.1, item_types={1001: '装备'}).succeeded
    assert settlement.stone('seller') == 1620 and settlement.stone('loser') == 1900
    with DatabaseUnitOfWork(settlement.database, read_only=True) as uow:
        assert uow.query_one("SELECT goods_num,bind_num FROM back") == {'goods_num': 4, 'bind_num': 3}
    assert len(settlement.history()) == 2


@pytest.mark.parametrize('target,reason,mutation', (
    ('back', 'auction_settlement_inventory_write_rejected', 'UPDATE back SET bind_num=0;'),
    ('seller', 'auction_settlement_balance_write_rejected', "UPDATE user_xiuxian SET stone=1 WHERE user_id='seller';"),
    ('auction_history', 'auction_settlement_history_write_rejected', 'UPDATE auction_history SET final_price=1;'),
    ('auction_pending_items', 'auction_settlement_pending_write_rejected', 'UPDATE auction_pending_items SET claimed_at=CURRENT_TIMESTAMP;'),
))
def test_last_write_cannot_corrupt_earlier_settlement(settlement, target, reason, mutation):
    if target == 'auction_pending_items':
        settlement.give('winner', 1001, 99)
    with DatabaseUnitOfWork(settlement.database) as uow:
        uow.execute(f'CREATE TRIGGER reject_settlement AFTER INSERT ON auction_session_operations BEGIN {mutation} END')
    before = snapshot(settlement)
    result = AuctionSettlementSqlRepository(settlement.database, max_goods_num=99).settle_active(
        'settle', end_time=300, fee_rate=0.1, item_types={1001: '装备'})
    assert result.reason == reason and result.subject
    assert snapshot(settlement) == before


@pytest.mark.parametrize('full', (False, True))
def test_unsold_return_refusal_preserves_sellers_item(settlement, full):
    with DatabaseUnitOfWork(settlement.database) as uow:
        uow.execute("UPDATE auction_current SET bids='{}'")
    if full:
        settlement.give('seller', 1001, 99)
    target = 'auction_pending_items' if full else 'back'
    install_fault(settlement, target, 'ignore')
    before = snapshot(settlement)
    repository = AuctionSettlementSqlRepository(settlement.database, max_goods_num=99)
    result = repository.settle_active('settle', end_time=300, fee_rate=0.1, item_types={1001: '装备'})
    assert result.reason == f"auction_settlement_{'pending' if full else 'inventory'}_write_rejected"
    assert snapshot(settlement) == before
    with DatabaseUnitOfWork(settlement.database) as uow:
        uow.execute('DROP TRIGGER reject_settlement')
    assert repository.settle_active('settle', end_time=300, fee_rate=0.1, item_types={1001: '装备'}).succeeded
    assert settlement.copies('seller', 1001) == (99 if full else 1)
    assert settlement.stone('seller') == 0
    assert len(settlement.pending()) == int(full)
    if full:
        assert settlement.pending()[0]['role'] == 'seller'


def test_missing_losing_bidder_is_distinct_from_write_refusal_and_stays_rejected(settlement):
    with DatabaseUnitOfWork(settlement.database) as uow:
        uow.execute("DELETE FROM user_xiuxian WHERE user_id='loser'")
    before = snapshot(settlement)
    app = AuctionSettlementApplication(settlement.database)
    outcome = app.settle_active(operation_id='settle', end_time=300, fee_rate=0.1, item_types={1001: '装备'})
    assert outcome.status == 'rejected' and outcome.code == 'participant_missing'
    assert outcome.data['subject'] == 'loser' and snapshot(settlement) == before
    with DatabaseUnitOfWork(settlement.database) as uow:
        uow.execute("INSERT INTO user_xiuxian VALUES('loser','restored',500)")
    restored = snapshot(settlement)
    replay = app.settle_active(operation_id='settle', end_time=300, fee_rate=0.1, item_types={1001: '装备'})
    assert replay.replayed and replay.status == 'rejected' and replay.data['subject'] == 'loser'
    assert snapshot(settlement) == restored


def test_unknown_sql_error_keeps_original_exception_and_retry(settlement):
    with DatabaseUnitOfWork(settlement.database) as uow:
        uow.execute("CREATE TRIGGER reject_settlement BEFORE INSERT ON auction_session_operations "
                    "BEGIN SELECT RAISE(ABORT,'isolated settlement fault'); END")
    before = snapshot(settlement)
    app = AuctionSettlementApplication(settlement.database)
    with pytest.raises(sqlite3.IntegrityError, match='isolated settlement fault'):
        app.settle_active(operation_id='settle', end_time=300, fee_rate=0.1, item_types={1001: '装备'})
    assert snapshot(settlement) == before
    with DatabaseUnitOfWork(settlement.database) as uow:
        uow.execute('DROP TRIGGER reject_settlement')
    assert app.settle_active(operation_id='settle', end_time=300, fee_rate=0.1, item_types={1001: '装备'}).ok


def test_legacy_finish_dto_preserves_refusal_subject(settlement):
    from nonebot_plugin_xiuxian_2.compatibility.legacy_trade_auction_sessions import AuctionSessionService
    install_fault(settlement, 'seller', 'ignore')
    before = snapshot(settlement)
    result = AuctionSessionService(settlement.database, settlement.root / 'trade.db', max_goods_num=99).finish(
        'settle', 'session', end_time=300, fee_rate=0.1, item_types={1001: '装备'})
    assert result.reason == 'auction_settlement_balance_write_rejected' and result.subject == 'seller'
    assert snapshot(settlement) == before


def test_failed_write_receipt_retains_reason_without_effect_events(settlement):
    install_fault(settlement, 'seller', 'ignore')
    app = AuctionSettlementApplication(settlement.database)
    outcome = app.settle_active(operation_id='settle', end_time=300, fee_rate=0.1, item_types={1001: '装备'})
    assert outcome.status == 'failed' and outcome.data['subject'] == 'seller'
    with DatabaseUnitOfWork(settlement.database, read_only=True) as uow:
        ledger = json.loads(uow.query_one('SELECT result_json FROM operation_ledger')['result_json'])
        assert ledger['data']['reason'] == outcome.code == 'auction_settlement_balance_write_rejected'
        assert ledger['data']['subject'] == 'seller'
        assert not uow.query_all('SELECT * FROM domain_outbox')


@pytest.mark.parametrize('known', (True, False))
def test_diagnostic_failure_cannot_replace_original_settlement_failure(settlement, known):
    if known:
        install_fault(settlement, 'seller', 'ignore')
    else:
        with DatabaseUnitOfWork(settlement.database) as uow:
            uow.execute("CREATE TRIGGER reject_settlement BEFORE INSERT ON auction_session_operations "
                        "BEGIN SELECT RAISE(ABORT,'isolated settlement fault'); END")
    with DatabaseUnitOfWork(settlement.database) as uow:
        uow.execute("CREATE TRIGGER reject_diagnostic BEFORE UPDATE ON operation_ledger "
                    "BEGIN SELECT RAISE(ABORT,'isolated diagnostic fault'); END")
    clock = SimpleNamespace(now=lambda: datetime.fromtimestamp(300, tz=timezone.utc))
    app = AuctionSettlementApplication(settlement.database)
    before = snapshot(settlement)
    with patch.object(trade, 'runtime_clock', clock), \
            patch.object(trade, '_auction_session_service', return_value=AuctionSessionStartApplication(settlement.database, settlement.root / 'trade.db')), \
            patch.object(trade, '_auction_query_application', return_value=AuctionQueryApplication(settlement.database)), \
            patch.object(trade, '_auction_settlement_application', return_value=app), \
            patch.object(trade.items, 'get_data_by_item_id', return_value={'type': '装备'}), \
            patch.object(trade.auction_config, 'get_auction_rules', return_value={'fee_rate': 0.1}):
        event = asyncio.run(fire(trade.check_auction_end_job))
        assert event.code == EVENT_JOB_ERROR
        if known:
            assert 'auction_settlement_balance_write_rejected' in str(event.exception)
            assert event.exception.subject == 'seller'
            assert 'isolated diagnostic fault' in str(event.exception.__cause__.__cause__)
        else:
            assert isinstance(event.exception, sqlite3.IntegrityError)
            assert str(event.exception) == 'isolated settlement fault'
            assert 'isolated diagnostic fault' in str(event.exception.__cause__)
        assert snapshot(settlement) == before
        with DatabaseUnitOfWork(settlement.database) as uow:
            ledger = uow.query_one('SELECT status,result_json FROM operation_ledger')
            assert ledger == {'status': 'started', 'result_json': None}
            uow.execute('DROP TRIGGER reject_settlement')
            uow.execute('DROP TRIGGER reject_diagnostic')
        clock.now = lambda: datetime.fromtimestamp(600, tz=timezone.utc)
        assert asyncio.run(fire(trade.check_auction_end_job)).code == EVENT_JOB_EXECUTED
        assert settlement.stone('seller') == 810 and settlement.status() == 'settled'
