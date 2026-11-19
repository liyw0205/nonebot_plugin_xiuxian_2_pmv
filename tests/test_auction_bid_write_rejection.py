"""Bid handoffs must preserve funds, snapshots and retryable causes."""
import sqlite3
from unittest.mock import patch

import pytest

from nonebot_plugin_xiuxian_2.features.auction.bid_repository import AuctionBidSqlRepository
from nonebot_plugin_xiuxian_2.features.auction.migrations import apply_auction_bid_operations, apply_auction_settlement
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork
from nonebot_plugin_xiuxian_2.features.auction.application import AuctionBidApplication
from nonebot_plugin_xiuxian_2.core.errors import DomainError
from nonebot_plugin_xiuxian_2.core.result import OperationOutcome
from nonebot_plugin_xiuxian_2.plugin import apply_platform_schema


@pytest.fixture
def database(tmp_path):
    path = tmp_path / 'bid.db'
    with DatabaseUnitOfWork(path) as uow:
        uow.execute('CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,stone INTEGER)')
        uow.execute("INSERT INTO user_xiuxian VALUES('buyer',500),('previous',50),('seller',0)")
        uow.execute('CREATE TABLE audit(trace TEXT)')
        apply_auction_settlement(uow)
        apply_auction_bid_operations(uow)
        apply_platform_schema(uow)
        uow.execute("INSERT INTO auction_sessions(session_id,status,start_time,end_time,items_count,start_operation_id) "
                    "VALUES('session','active',1,100,1,'start')")
        uow.execute("INSERT INTO auction_current(id,item_id,name,start_price,current_price,seller_id,seller_name,bids,bid_times,last_bid_time) "
                    "VALUES('lot',1001,'herb',100,200,'seller','seller',?, '{}',1)", ('{"previous": 200}',))
    return path


def snapshot(database):
    with DatabaseUnitOfWork(database, read_only=True) as uow:
        return [uow.query_all(f'SELECT * FROM {table} ORDER BY rowid') for table in
                ('user_xiuxian', 'auction_current', 'auction_bid_operations', 'audit')]


def bid(database):
    return AuctionBidSqlRepository(database).place_auction_bid('operation', 'lot', 'buyer', 300, 200, {'previous': 200}, 2)


@pytest.mark.parametrize('target', ('debit', 'refund', 'item', 'receipt'))
@pytest.mark.parametrize('mode', ('ignore', 'rewrite'))
def test_bid_refused_handoff_never_commits_partial_assets(database, target, mode):
    table = {'debit': 'user_xiuxian', 'refund': 'user_xiuxian',
             'item': 'auction_current', 'receipt': 'auction_bid_operations'}[target]
    event = 'INSERT' if target == 'receipt' else 'UPDATE'
    condition = "WHEN NEW.user_id='buyer'" if target == 'debit' else "WHEN NEW.user_id='previous'" if target == 'refund' else ''
    mutation = {
        'debit': "UPDATE user_xiuxian SET stone=201 WHERE user_id='buyer';",
        'refund': "UPDATE user_xiuxian SET stone=51 WHERE user_id='previous';",
        'item': "UPDATE auction_current SET current_price=201 WHERE id='lot';",
        'receipt': "UPDATE auction_bid_operations SET debit=1 WHERE operation_id=NEW.operation_id;",
    }[target]
    timing, body = ('BEFORE', 'SELECT RAISE(IGNORE);') if mode == 'ignore' else ('AFTER', mutation)
    with DatabaseUnitOfWork(database) as uow:
        uow.execute(f"CREATE TRIGGER refuse_bid {timing} {event} ON {table} {condition} "
                    f"BEGIN INSERT INTO audit VALUES('effect'); {body} END")
    before = snapshot(database)
    result = bid(database)
    after = snapshot(database)
    assert after == before, (result, 'partial assets or trigger effects committed', before, after)
    assert result.reason == f'auction_bid_{target}_write_rejected' and result.subject, result
    with DatabaseUnitOfWork(database) as uow:
        uow.execute('DROP TRIGGER refuse_bid')
    assert bid(database).status == 'bid'
    after = snapshot(database)
    assert bid(database).status == 'duplicate' and snapshot(database) == after
    with DatabaseUnitOfWork(database, read_only=True) as uow:
        assert uow.query_all('PRAGMA integrity_check') == [{'integrity_check': 'ok'}]


def test_missing_previous_participant_cannot_commit_new_bid_debit(database):
    with DatabaseUnitOfWork(database) as uow:
        uow.execute("DELETE FROM user_xiuxian WHERE user_id='previous'")
    before = snapshot(database)
    result = bid(database)
    assert snapshot(database) == before, (result, 'missing participant committed buyer debit')
    assert result.reason == 'participant_missing' and result.subject == 'previous'
    with DatabaseUnitOfWork(database) as uow:
        uow.execute("INSERT INTO user_xiuxian VALUES('previous',50)")
    assert bid(database).status == 'bid'


def test_compatibility_repository_preserves_refusal_reason(database):
    from nonebot_plugin_xiuxian_2.features.auction.repository import LegacyTradeRepository
    with DatabaseUnitOfWork(database) as uow:
        uow.execute('CREATE TRIGGER refuse_bid BEFORE UPDATE ON auction_current BEGIN SELECT RAISE(IGNORE); END')
    before = snapshot(database)
    result = LegacyTradeRepository(str(database)).place_auction_bid(
        'operation', 'lot', 'buyer', 300, 200, {'previous': 200}, 2,
    )
    assert result.reason == 'auction_bid_item_write_rejected' and result.subject == 'lot'
    assert snapshot(database) == before


@pytest.mark.parametrize('target', ('debit', 'refund', 'item', 'receipt'))
def test_unknown_abort_is_original_and_fully_rolled_back(database, target):
    table = {'debit': 'user_xiuxian', 'refund': 'user_xiuxian', 'item': 'auction_current', 'receipt': 'auction_bid_operations'}[target]
    event = 'INSERT' if target == 'receipt' else 'UPDATE'
    condition = "WHEN NEW.user_id='buyer'" if target == 'debit' else "WHEN NEW.user_id='previous'" if target == 'refund' else ''
    with DatabaseUnitOfWork(database) as uow:
        uow.execute(f"CREATE TRIGGER refuse_abort BEFORE {event} ON {table} {condition} "
                    "BEGIN SELECT RAISE(ABORT,'isolated bid abort'); END")
    before = snapshot(database)
    with pytest.raises(sqlite3.IntegrityError, match='isolated bid abort'):
        bid(database)
    assert snapshot(database) == before
    with DatabaseUnitOfWork(database) as uow:
        uow.execute('DROP TRIGGER refuse_abort')
    assert bid(database).status == 'bid'


@pytest.mark.parametrize('target', ('debit', 'refund', 'item'))
def test_final_receipt_cannot_change_previous_writes(database, target):
    mutation = {'debit': "UPDATE user_xiuxian SET stone=0 WHERE user_id='buyer';",
                'refund': "UPDATE user_xiuxian SET stone=0 WHERE user_id='previous';",
                'item': "UPDATE auction_current SET bids='{}' WHERE id='lot';"}[target]
    with DatabaseUnitOfWork(database) as uow:
        uow.execute(f'CREATE TRIGGER alter_previous AFTER INSERT ON auction_bid_operations BEGIN {mutation} END')
    before = snapshot(database)
    result = bid(database)
    assert result.reason == f'auction_bid_{target}_write_rejected' and snapshot(database) == before


@pytest.mark.parametrize('user', ('buyer', 'previous'))
@pytest.mark.parametrize('amount,reason', (('broken', 'stone_invalid_numeric_value'), (-1, 'auction_bid_balance_invalid')))
def test_bad_balances_keep_specific_reason_without_debit(database, user, amount, reason):
    with DatabaseUnitOfWork(database) as uow:
        uow.execute('UPDATE user_xiuxian SET stone=? WHERE user_id=?', (amount, user))
    before = snapshot(database)
    result = bid(database)
    assert result.reason == reason and result.subject == user and snapshot(database) == before


def test_same_bidder_increase_keeps_other_funds_and_exact_large_balance(database):
    balance = 10**35
    with DatabaseUnitOfWork(database) as uow:
        uow.execute("UPDATE user_xiuxian SET stone=xiuxian_int_store(?) WHERE user_id='previous'", (str(balance),))
    result = AuctionBidSqlRepository(database).place_auction_bid('same', 'lot', 'previous', 300, 200, {'previous': 200}, 2)
    assert result.status == 'bid' and result.debit == 100 and result.refunded_amount == 0
    from nonebot_plugin_xiuxian_2.core.numeric import require_exact_integer
    with DatabaseUnitOfWork(database, read_only=True) as uow:
        row = uow.query_one("SELECT stone FROM user_xiuxian WHERE user_id='previous'")
        assert require_exact_integer(row['stone'], field='stone', subject='previous') == balance - 100
        assert uow.query_one("SELECT stone FROM user_xiuxian WHERE user_id='buyer'")['stone'] == 500


def request(app):
    return app.place_bid(operation_id='operation', auction_id='lot', bidder_id='buyer', bid_price=300,
                         expected_price=200, expected_bids={'previous': 200}, bid_time=2)


@pytest.mark.parametrize('target', ('debit', 'refund', 'item', 'receipt'))
def test_application_rejection_is_retryable_with_reason_and_no_effects(database, target):
    table = {'debit': 'user_xiuxian', 'refund': 'user_xiuxian', 'item': 'auction_current', 'receipt': 'auction_bid_operations'}[target]
    event = 'INSERT' if target == 'receipt' else 'UPDATE'
    condition = "WHEN NEW.user_id='buyer'" if target == 'debit' else "WHEN NEW.user_id='previous'" if target == 'refund' else ''
    with DatabaseUnitOfWork(database) as uow:
        uow.execute(f'CREATE TRIGGER refuse_bid BEFORE {event} ON {table} {condition} BEGIN SELECT RAISE(IGNORE); END')
    before = snapshot(database)
    outcome = request(AuctionBidApplication(database))
    assert outcome.status == 'failed' and outcome.code == f'auction_bid_{target}_write_rejected'
    assert outcome.data['reason'] == outcome.code and outcome.data['subject']
    assert snapshot(database) == before
    with DatabaseUnitOfWork(database) as uow:
        assert not uow.query_all('SELECT * FROM domain_outbox')
        assert uow.query_one('SELECT status FROM operation_ledger')['status'] == 'failed'
        uow.execute('DROP TRIGGER refuse_bid')
    app = AuctionBidApplication(database)
    assert request(app).status == 'applied'
    after = snapshot(database)
    assert request(app).replayed and snapshot(database) == after


def test_old_rejected_ledger_keeps_original_replay(database):
    app = AuctionBidApplication(database)
    payload = dict(auction_id='lot', bidder_id='buyer', bid_price=300, expected_price=200, expected_bids={'previous': 200})
    with DatabaseUnitOfWork(database) as uow:
        app.ledger.begin(uow, 'operation', app.action, payload)
        app.ledger.finish(uow, OperationOutcome.rejected('operation', app.action, 'old rejected', code='participant_missing'))
    before = snapshot(database)
    outcome = request(app)
    assert outcome.code == 'participant_missing' and outcome.replayed
    assert snapshot(database) == before


@pytest.mark.parametrize('known', (True, False))
def test_diagnostic_failure_cannot_replace_original_cause(database, known):
    mode = 'IGNORE' if known else "ABORT,'isolated bid abort'"
    with DatabaseUnitOfWork(database) as uow:
        uow.execute(f'CREATE TRIGGER refuse_bid BEFORE INSERT ON auction_bid_operations BEGIN SELECT RAISE({mode}); END')
    app = AuctionBidApplication(database)
    original = snapshot(database)
    diagnostic = RuntimeError('isolated diagnostic failure')
    with patch.object(app.ledger, 'finish' if known else 'record_failure', side_effect=diagnostic):
        with pytest.raises(DomainError if known else sqlite3.IntegrityError) as caught:
            request(app)
    assert caught.value.__cause__ is diagnostic
    if known:
        assert caught.value.details['reason'] == 'auction_bid_receipt_write_rejected'
        assert caught.value.details['subject'] == 'operation'
    else:
        assert 'isolated bid abort' in str(caught.value)
    assert snapshot(database) == original
    with DatabaseUnitOfWork(database) as uow:
        assert uow.query_one('SELECT status FROM operation_ledger')['status'] == 'started'
        uow.execute('DROP TRIGGER refuse_bid')
    assert request(AuctionBidApplication(database)).ok
