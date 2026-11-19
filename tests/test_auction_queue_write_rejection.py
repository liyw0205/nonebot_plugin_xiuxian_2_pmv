"""Player queue handoff must never acknowledge missing inventory or receipts."""
import sqlite3
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from nonebot_plugin_xiuxian_2.features.auction.migrations import apply_auction_player_queue, apply_auction_queue_operations
from nonebot_plugin_xiuxian_2.features.auction.queue_application import AuctionQueueApplication
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork


@pytest.fixture
def queue(tmp_path):
    game, market = tmp_path / 'game.db', tmp_path / 'trade.db'
    with DatabaseUnitOfWork(game) as uow:
        uow.execute('CREATE TABLE back(user_id TEXT,goods_id INTEGER,goods_name TEXT,goods_type TEXT,'
                    'goods_num INTEGER,bind_num INTEGER,state INTEGER,create_time TEXT,update_time TEXT,'
                    'UNIQUE(user_id,goods_id))')
        uow.execute("INSERT INTO back VALUES('seller',1001,'herb','药材',3,1,0,'old','old')")
        apply_auction_queue_operations(uow)
    with DatabaseUnitOfWork(market) as uow:
        apply_auction_player_queue(uow)
    clock = SimpleNamespace(now=lambda: datetime(2026, 11, 18, tzinfo=timezone.utc))
    return SimpleNamespace(game=game, market=market, clock=clock,
                           app=AuctionQueueApplication(game, market, 99, clock=clock))


def snapshot(queue):
    with DatabaseUnitOfWork(queue.game, read_only=True) as uow:
        game = [uow.query_all(f'SELECT * FROM {table} ORDER BY rowid') for table in ('back', 'auction_queue_operations')]
    with DatabaseUnitOfWork(queue.market, read_only=True) as uow:
        return game, uow.query_all('SELECT * FROM auction_player_upload ORDER BY user_id,item_id')


def enqueue(queue, operation='enqueue'):
    return queue.app.enqueue(operation, 'seller', 1001, 'herb', 1000, 'seller', max_user_items=3)


def invoke(queue, action):
    return enqueue(queue) if action == 'enqueue' else queue.app.dequeue('dequeue', 'seller', 1001, '药材')


@pytest.mark.parametrize('action', ('enqueue', 'dequeue'))
@pytest.mark.parametrize('target', ('inventory', 'queue', 'receipt'))
@pytest.mark.parametrize('mode', ('ignore', 'rewrite'))
def test_queue_write_refusal_rolls_back_both_databases_and_recovers(queue, action, target, mode):
    if action == 'dequeue':
        assert enqueue(queue).succeeded
    database = queue.market if target == 'queue' else queue.game
    table = {'inventory': 'back', 'queue': 'auction_player_upload', 'receipt': 'auction_queue_operations'}[target]
    event = {'inventory': 'UPDATE' if action == 'enqueue' else 'INSERT',
             'queue': 'INSERT' if action == 'enqueue' else 'DELETE', 'receipt': 'INSERT'}[target]
    if mode == 'rewrite' and target == 'inventory' and action == 'dequeue':
        event = 'UPDATE'
    side_effect = ("UPDATE auction_player_upload SET user_name='effect';" if target == 'queue'
                   else "UPDATE back SET create_time='effect';")
    mutation = {
        'inventory': "UPDATE back SET goods_num=goods_num+1 WHERE user_id='seller' AND goods_id=1001;",
        'queue': ("UPDATE auction_player_upload SET start_price=1;" if action == 'enqueue' else
                  'INSERT INTO auction_player_upload VALUES(OLD.user_id,OLD.item_id,OLD.item_name,OLD.start_price,OLD.user_name);'),
        'receipt': "UPDATE auction_queue_operations SET user_name='wrong' WHERE operation_id=NEW.operation_id;",
    }[target]
    timing, body = ('BEFORE', side_effect + 'SELECT RAISE(IGNORE);') if mode == 'ignore' else ('AFTER', side_effect + mutation)
    with DatabaseUnitOfWork(database) as uow:
        uow.execute(f'CREATE TRIGGER reject_queue {timing} {event} ON {table} BEGIN {body} END')
    before = snapshot(queue)
    result = invoke(queue, action)
    assert not result.succeeded and result.reason == f'auction_queue_{target}_write_rejected', result
    assert result.subject and snapshot(queue) == before
    with DatabaseUnitOfWork(database) as uow:
        uow.execute('DROP TRIGGER reject_queue')
    assert invoke(queue, action).succeeded
    after = snapshot(queue)
    assert invoke(queue, action).status == 'duplicate' and snapshot(queue) == after
    with DatabaseUnitOfWork(queue.game, read_only=True) as uow:
        assert uow.query_one('SELECT goods_num,bind_num,state FROM back') == {
            'goods_num': 2 if action == 'enqueue' else 3, 'bind_num': 1 if action == 'enqueue' else 2, 'state': 0}
    for path in (queue.game, queue.market):
        with DatabaseUnitOfWork(path, read_only=True) as uow:
            assert uow.query_all('PRAGMA integrity_check') == [{'integrity_check': 'ok'}]


@pytest.mark.parametrize('action', ('enqueue', 'dequeue'))
@pytest.mark.parametrize('target', ('inventory', 'queue'))
def test_final_receipt_cannot_change_earlier_handoff(queue, action, target):
    if action == 'dequeue':
        assert enqueue(queue).succeeded
    # A receipt trigger can change earlier inventory; a queue trigger can mutate
    # the queue during deletion/insertion even after the inventory was checked.
    if target == 'inventory':
        database = queue.game
        table, event = 'auction_queue_operations', 'INSERT'
        body = "UPDATE back SET bind_num=0 WHERE user_id='seller';"
    else:
        database = queue.market
        table = 'auction_player_upload'
        event = 'INSERT' if action == 'enqueue' else 'DELETE'
        body = ("UPDATE auction_player_upload SET item_name='wrong';" if action == 'enqueue' else
                'INSERT INTO auction_player_upload VALUES(OLD.user_id,OLD.item_id,OLD.item_name,OLD.start_price,OLD.user_name);')
    with DatabaseUnitOfWork(database) as uow:
        uow.execute(f'CREATE TRIGGER change_previous AFTER {event} ON {table} BEGIN {body} END')
    before = snapshot(queue)
    result = invoke(queue, action)
    assert result.reason == f'auction_queue_{target}_write_rejected'
    assert snapshot(queue) == before


@pytest.mark.parametrize('action', ('enqueue', 'dequeue'))
@pytest.mark.parametrize('target', ('inventory', 'queue', 'receipt'))
def test_unknown_abort_keeps_original_error_and_rolls_back(queue, action, target):
    if action == 'dequeue':
        assert enqueue(queue).succeeded
    database = queue.market if target == 'queue' else queue.game
    table = {'inventory': 'back', 'queue': 'auction_player_upload', 'receipt': 'auction_queue_operations'}[target]
    event = {'inventory': 'UPDATE' if action == 'enqueue' else 'INSERT',
             'queue': 'INSERT' if action == 'enqueue' else 'DELETE', 'receipt': 'INSERT'}[target]
    with DatabaseUnitOfWork(database) as uow:
        uow.execute(f"CREATE TRIGGER reject_abort BEFORE {event} ON {table} "
                    "BEGIN SELECT RAISE(ABORT,'isolated queue abort'); END")
    before = snapshot(queue)
    with pytest.raises(sqlite3.IntegrityError, match='isolated queue abort'):
        invoke(queue, action)
    assert snapshot(queue) == before
    with DatabaseUnitOfWork(database) as uow:
        uow.execute('DROP TRIGGER reject_abort')
    assert invoke(queue, action).applied


@pytest.mark.parametrize('action', ('enqueue', 'dequeue'))
@pytest.mark.parametrize('field,value,reason', (
    ('goods_num', 'broken', 'goods_num_invalid_numeric_value'),
    ('bind_num', 0.5, 'bind_num_invalid_numeric_value'),
    ('state', -1, 'auction_queue_inventory_invalid'),
    ('bind_num', 90, 'auction_queue_inventory_invalid'),
))
def test_invalid_inventory_is_specific_and_unchanged(queue, action, field, value, reason):
    if action == 'dequeue':
        assert enqueue(queue).succeeded
    with DatabaseUnitOfWork(queue.game) as uow:
        uow.execute(f'UPDATE back SET {field}=?', (value,))
    before = snapshot(queue)
    result = invoke(queue, action)
    assert result.reason == reason and result.subject == 'seller:1001'
    assert snapshot(queue) == before


def test_replay_precedes_limits_and_price_conflicts_preserve_history(queue):
    first = enqueue(queue)
    assert first.applied
    with DatabaseUnitOfWork(queue.game) as uow:
        uow.execute('UPDATE back SET goods_num=0,bind_num=0')
    before = snapshot(queue)
    assert queue.app.enqueue('enqueue', 'seller', 1001, 'new-name', 1000, 'new-user', max_user_items=1).status == 'duplicate'
    conflict = queue.app.enqueue('enqueue', 'seller', 1001, 'herb', 2000, 'seller', max_user_items=1)
    assert conflict.reason == 'operation_conflict' and conflict.subject == 'enqueue'
    assert snapshot(queue) == before


def test_dequeue_new_stack_bound_ownership_and_compatibility_reason(queue):
    from nonebot_plugin_xiuxian_2.compatibility.legacy_trade_auction_queue import AuctionQueueService
    assert enqueue(queue).applied
    before = snapshot(queue)
    result = queue.app.dequeue('foreign', 'another', 1001, '药材')
    assert result.reason == 'queue_missing' and result.subject == 'another:1001'
    assert snapshot(queue) == before
    with DatabaseUnitOfWork(queue.game) as uow:
        uow.execute('DELETE FROM back')
        uow.execute("CREATE TRIGGER refuse_new BEFORE INSERT ON back BEGIN SELECT RAISE(IGNORE); END")
    service = AuctionQueueService(queue.game, queue.market, 99)
    before = snapshot(queue)
    result = service.dequeue('dequeue', 'seller', 1001, '药材')
    assert result.reason == 'auction_queue_inventory_write_rejected' and result.subject == 'seller:1001'
    assert snapshot(queue) == before
    with DatabaseUnitOfWork(queue.game) as uow:
        uow.execute('DROP TRIGGER refuse_new')
    assert service.dequeue('dequeue', 'seller', 1001, '药材').applied
    with DatabaseUnitOfWork(queue.game, read_only=True) as uow:
        assert uow.query_one('SELECT goods_num,bind_num FROM back') == {'goods_num': 1, 'bind_num': 1}
    after = snapshot(queue)
    assert service.dequeue('dequeue', 'seller', 1001, '药材').status == 'duplicate'
    assert snapshot(queue) == after


def test_new_stack_trigger_cannot_equip_returned_item(queue):
    assert enqueue(queue).applied
    with DatabaseUnitOfWork(queue.game) as uow:
        uow.execute('DELETE FROM back')
        uow.execute("CREATE TRIGGER equip_return AFTER INSERT ON back BEGIN UPDATE back SET state=1; END")
    before = snapshot(queue)
    result = invoke(queue, 'dequeue')
    assert result.reason == 'auction_queue_inventory_write_rejected'
    assert snapshot(queue) == before
