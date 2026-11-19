"""A claim commits inventory, pending marks and its receipt together."""
import sqlite3
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from tests.test_auction_pending_claim import PendingClaimFixture
from nonebot_plugin_xiuxian_2.features.auction.pending import AuctionPendingApplication
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork


TARGETS = (
    ('back', 'INSERT', 'auction_claim_inventory_write_rejected', 'buyer:1001'),
    ('auction_pending_items', 'UPDATE', 'auction_claim_pending_write_rejected', 'sale'),
    ('auction_pending_claim_operations', 'INSERT', 'auction_claim_receipt_write_rejected', 'claim'),
)


@pytest.fixture
def pending(tmp_path):
    fixture = PendingClaimFixture(tmp_path)
    fixture.park(auction_id='sale', owner_id='buyer')
    return fixture


def snapshot(fixture):
    with DatabaseUnitOfWork(fixture.database, read_only=True) as uow:
        return [uow.query_all(f'SELECT * FROM {table} ORDER BY rowid') for table in (
            'back', 'auction_pending_items', 'auction_pending_claim_operations')]


def claim(fixture, operation='claim'):
    clock = SimpleNamespace(now=lambda: datetime(2026, 11, 18, 2, tzinfo=timezone.utc))
    return AuctionPendingApplication(fixture.database, max_goods_num=10, clock=clock).claim(
        operation_id=operation, user_id='buyer')


@pytest.mark.parametrize('table,event,reason,subject', TARGETS)
@pytest.mark.parametrize('mode', ('ignore', 'rewrite'))
def test_claim_rejected_writes_roll_back_and_retry(pending, table, event, reason, subject, mode):
    mutation = {
        'back': 'UPDATE back SET bind_num=0 WHERE user_id=NEW.user_id AND goods_id=NEW.goods_id;',
        'auction_pending_items': "UPDATE auction_pending_items SET claim_operation_id='wrong' WHERE id=NEW.id;",
        'auction_pending_claim_operations': "UPDATE auction_pending_claim_operations SET claimed_count=0 WHERE operation_id=NEW.operation_id;",
    }[table]
    timing, body = ('BEFORE', 'SELECT RAISE(IGNORE);') if mode == 'ignore' else ('AFTER', mutation)
    with DatabaseUnitOfWork(pending.database) as uow:
        uow.execute(f'CREATE TRIGGER reject_claim {timing} {event} ON {table} BEGIN {body} END')
    before = snapshot(pending)
    result = claim(pending)
    assert not result.succeeded and result.reason == reason and result.subject == subject
    assert snapshot(pending) == before
    with DatabaseUnitOfWork(pending.database) as uow:
        uow.execute('DROP TRIGGER reject_claim')
    assert claim(pending).status == 'claimed'
    assert pending.copies('buyer', 1001) == pending.binds('buyer', 1001) == 1
    after = snapshot(pending)
    assert claim(pending).status == 'duplicate' and snapshot(pending) == after
    with DatabaseUnitOfWork(pending.database, read_only=True) as uow:
        assert uow.query_one('SELECT claimed_at FROM auction_pending_items')['claimed_at'] == '2026-11-18T02:00:00+00:00'
        assert uow.query_all('PRAGMA integrity_check') == [{'integrity_check': 'ok'}]


@pytest.mark.parametrize('mutation,reason', (
    ('UPDATE back SET goods_num=2;', 'auction_claim_inventory_write_rejected'),
    ("UPDATE auction_pending_items SET owner_id='other';", 'auction_claim_pending_write_rejected'),
))
def test_late_receipt_trigger_cannot_rewrite_earlier_claim(pending, mutation, reason):
    with DatabaseUnitOfWork(pending.database) as uow:
        uow.execute(f'CREATE TRIGGER reject_claim AFTER INSERT ON auction_pending_claim_operations BEGIN {mutation} END')
    before = snapshot(pending)
    assert claim(pending).reason == reason and snapshot(pending) == before


def test_later_pending_failure_rolls_back_earlier_claim_and_trigger_effects(pending):
    pending.park(auction_id='second', owner_id='buyer')
    pending.give('buyer', 1001, 2)
    with DatabaseUnitOfWork(pending.database) as uow:
        uow.execute('UPDATE back SET bind_num=1')
        uow.execute("CREATE TRIGGER reject_claim BEFORE UPDATE ON auction_pending_items "
                    "WHEN NEW.auction_id='second' BEGIN UPDATE auction_pending_items SET owner_name='effect'; SELECT RAISE(IGNORE); END")
    before = snapshot(pending)
    assert claim(pending).reason == 'auction_claim_pending_write_rejected'
    assert snapshot(pending) == before
    with DatabaseUnitOfWork(pending.database) as uow:
        uow.execute('DROP TRIGGER reject_claim')
    assert len(claim(pending).claimed) == 2
    assert pending.copies('buyer', 1001) == 4 and pending.binds('buyer', 1001) == 3


def test_unknown_sql_failure_rolls_back_and_keeps_original(pending):
    with DatabaseUnitOfWork(pending.database) as uow:
        uow.execute("CREATE TRIGGER reject_claim BEFORE INSERT ON auction_pending_claim_operations "
                    "BEGIN SELECT RAISE(ABORT,'isolated claim fault'); END")
    before = snapshot(pending)
    with pytest.raises(sqlite3.IntegrityError, match='isolated claim fault'):
        claim(pending)
    assert snapshot(pending) == before
    with DatabaseUnitOfWork(pending.database) as uow:
        uow.execute('DROP TRIGGER reject_claim')
    assert claim(pending).succeeded


@pytest.mark.parametrize('quantity,bound,reason', (
    (-1, 0, 'auction_claim_inventory_invalid'),
    (1, 2, 'auction_claim_inventory_invalid'),
    ('bad', 0, 'goods_num_invalid_numeric_value'),
))
def test_invalid_inventory_keeps_specific_reason_and_pending(pending, quantity, bound, reason):
    pending.give('buyer', 1001, 1)
    with DatabaseUnitOfWork(pending.database) as uow:
        uow.execute('UPDATE back SET goods_num=?,bind_num=?', (quantity, bound))
    before = snapshot(pending)
    result = claim(pending)
    assert result.reason == reason and result.subject == 'buyer:1001'
    assert snapshot(pending) == before


def test_old_blocked_receipt_is_not_a_success_or_a_fresh_claim(pending):
    pending.give('buyer', 1001, 10)
    assert claim(pending).status == 'blocked'
    with DatabaseUnitOfWork(pending.database) as uow:
        uow.execute('UPDATE back SET goods_num=9')
    before = snapshot(pending)
    replay = claim(pending)
    assert replay.status == 'duplicate' and not replay.succeeded and not replay.claimed
    assert replay.blocked and snapshot(pending) == before
    assert claim(pending, 'new-message').succeeded
    assert pending.copies('buyer', 1001) == 10 and pending.binds('buyer', 1001) == 1


def test_same_auction_roles_cannot_mark_another_owners_item(pending):
    pending.park(auction_id='sale', owner_id='seller', role='seller')
    result = claim(pending)
    assert result.succeeded and pending.unclaimed('seller') == 1
    assert pending.unclaimed('buyer') == 0 and pending.copies('seller', 1001) == 0


@pytest.mark.parametrize('mode', ('ignore', 'rewrite'))
def test_existing_stack_update_refusal_is_detected(pending, mode):
    pending.give('buyer', 1001, 3)
    timing, body = (('BEFORE', 'SELECT RAISE(IGNORE);') if mode == 'ignore' else
                    ('AFTER', "UPDATE back SET goods_num=3 WHERE user_id=NEW.user_id AND goods_id=NEW.goods_id;"))
    with DatabaseUnitOfWork(pending.database) as uow:
        uow.execute(f'CREATE TRIGGER reject_claim {timing} UPDATE ON back BEGIN {body} END')
    before = snapshot(pending)
    assert claim(pending).reason == 'auction_claim_inventory_write_rejected'
    assert snapshot(pending) == before


def test_refused_blocked_receipt_does_not_commit_an_invented_history(pending):
    pending.give('buyer', 1001, 10)
    with DatabaseUnitOfWork(pending.database) as uow:
        uow.execute('CREATE TRIGGER reject_claim BEFORE INSERT ON auction_pending_claim_operations '
                    'BEGIN SELECT RAISE(IGNORE); END')
    before = snapshot(pending)
    result = claim(pending)
    assert result.reason == 'auction_claim_receipt_write_rejected'
    assert snapshot(pending) == before


def test_receipt_cannot_destroy_a_blocked_pending_item(pending):
    pending.give('buyer', 1001, 10)
    with DatabaseUnitOfWork(pending.database) as uow:
        uow.execute("CREATE TRIGGER reject_claim AFTER INSERT ON auction_pending_claim_operations "
                    "BEGIN DELETE FROM auction_pending_items; END")
    before = snapshot(pending)
    assert claim(pending).reason == 'auction_claim_pending_write_rejected'
    assert snapshot(pending) == before


def test_successful_historical_receipt_replays_before_live_inventory_schema(pending):
    assert claim(pending).succeeded
    with DatabaseUnitOfWork(pending.database) as uow:
        uow.execute('DROP TABLE back')
    assert claim(pending).status == 'duplicate'
