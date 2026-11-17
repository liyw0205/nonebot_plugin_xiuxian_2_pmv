"""An auction start must durably hand off every queue item before reporting success."""
import asyncio
from datetime import timedelta

import pytest
from apscheduler.events import EVENT_JOB_ERROR, EVENT_JOB_EXECUTED

from tests.test_auction_schedule import auction, fire, trade
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork


TARGETS = (
    ('auction_current', 'auction_start_item_write_rejected'),
    ('auction_player_upload', 'auction_start_queue_write_rejected'),
    ('auction_sessions', 'auction_start_session_write_rejected'),
    ('auction_session_operations', 'auction_start_receipt_write_rejected'),
)


def seed_queue(auction):
    with DatabaseUnitOfWork(auction.market) as uow:
        uow.execute("INSERT INTO auction_player_upload VALUES('buyer',1002,'herb2',200,'buyer')")


def snapshot(auction):
    with DatabaseUnitOfWork(auction.game, read_only=True) as uow:
        game = [uow.query_all(f'SELECT * FROM {table} ORDER BY rowid') for table in
                ('auction_current', 'auction_sessions', 'auction_session_operations', 'user_xiuxian')]
    with DatabaseUnitOfWork(auction.market, read_only=True) as uow:
        return game, uow.query_all('SELECT * FROM auction_player_upload ORDER BY user_id,item_id')


def install_fault(auction, table, mode):
    database = auction.market if table == 'auction_player_upload' else auction.game
    event = 'DELETE' if table == 'auction_player_upload' else 'INSERT'
    side_effect = ("UPDATE auction_player_upload SET user_name='trigger-effect';" if table == 'auction_player_upload'
                   else 'UPDATE user_xiuxian SET stone=1;')
    if mode == 'ignore':
        body = side_effect + ' SELECT RAISE(IGNORE);'
        timing = 'BEFORE'
    else:
        timing = 'AFTER'
        body = {
            'auction_current': 'UPDATE auction_current SET current_price=current_price+1 WHERE id=NEW.id;',
            'auction_player_upload': 'INSERT INTO auction_player_upload VALUES(OLD.user_id,OLD.item_id,OLD.item_name,OLD.start_price,OLD.user_name);',
            'auction_sessions': 'UPDATE auction_sessions SET end_time=end_time+1 WHERE session_id=NEW.session_id;',
            'auction_session_operations': "UPDATE auction_session_operations SET result='{}' WHERE operation_id=NEW.operation_id;",
        }[table] + side_effect
    with DatabaseUnitOfWork(database) as uow:
        uow.execute(f'CREATE TRIGGER reject_start {timing} {event} ON {table} BEGIN {body} END')
    return database


@pytest.mark.parametrize('table,reason', TARGETS)
@pytest.mark.parametrize('mode', ('ignore', 'rewrite'))
def test_rejected_handoff_rolls_back_both_databases_and_trigger_effects(auction, table, reason, mode):
    seed_queue(auction)
    fault_db = install_fault(auction, table, mode)
    before = snapshot(auction)
    result = auction.start.start('write-test', system_items_config=auction.system, duration_hours=2)
    assert not result.succeeded and result.reason == reason, result
    assert result.subject
    assert snapshot(auction) == before
    with DatabaseUnitOfWork(fault_db) as uow:
        uow.execute('DROP TRIGGER reject_start')
    auction.clock.moment += timedelta(minutes=1)
    recovered = auction.start.start('write-test', system_items_config=auction.system, duration_hours=2)
    assert recovered.succeeded and recovered.items_count == 2
    after = snapshot(auction)
    replay = auction.start.start('write-test', system_items_config=auction.system, duration_hours=2)
    assert replay.status == 'duplicate' and snapshot(auction) == after
    for database in (auction.game, auction.market):
        with DatabaseUnitOfWork(database, read_only=True) as uow:
            assert uow.query_all('PRAGMA integrity_check') == [{'integrity_check': 'ok'}]


@pytest.mark.parametrize('table,reason', TARGETS)
def test_actual_start_job_reports_rejection_and_can_retry(auction, table, reason):
    seed_queue(auction)
    fault_db = install_fault(auction, table, 'ignore')
    before = snapshot(auction)
    event = asyncio.run(fire(trade.auto_start_auction_job))
    assert event.code == EVENT_JOB_ERROR and reason in str(event.exception)
    assert snapshot(auction) == before and auction.config['last_auto_start_date'] == ''
    with DatabaseUnitOfWork(fault_db) as uow:
        uow.execute('DROP TRIGGER reject_start')
    assert asyncio.run(fire(trade.auto_start_auction_job)).code == EVENT_JOB_EXECUTED
    assert auction.start.get_active_session()['items_count'] == 2


def test_final_receipt_trigger_corrupting_earlier_lot_is_detected(auction):
    seed_queue(auction)
    with DatabaseUnitOfWork(auction.game) as uow:
        uow.execute('CREATE TRIGGER reject_start AFTER INSERT ON auction_session_operations '
                    'BEGIN UPDATE auction_current SET current_price=current_price+1; '
                    'UPDATE user_xiuxian SET stone=1; END')
    before = snapshot(auction)
    result = auction.start.start('late-corruption', system_items_config=auction.system, duration_hours=2)
    assert result.reason == 'auction_start_item_write_rejected'
    assert snapshot(auction) == before
