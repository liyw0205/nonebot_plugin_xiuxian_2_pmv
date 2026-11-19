"""Daily task handoffs must commit verified state and durable receipts together."""
import sqlite3
import asyncio
import os
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch
from zoneinfo import ZoneInfo

import pytest
import nonebot
from nonebot.exception import FinishedException, ProcessException
from nonebot_plugin_xiuxian_2.core.errors import DomainError

from nonebot_plugin_xiuxian_2.features.sect.application import SectApplication
from nonebot_plugin_xiuxian_2.features.sect.migrations import (
    apply_sect_task_state, apply_sect_task_claim_operations,
    apply_sect_task_settlement_operations, apply_sect_task_weekly_event_status,
)
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork

nonebot.init()

DAY = '2026-11-18'
TASK = {'type': 2, 'cost': 150, 'give': 0.01, 'sect': 25, 'desc': 'isolated task'}


@pytest.fixture
def state(tmp_path):
    database = tmp_path / 'tasks.db'
    with DatabaseUnitOfWork(database) as uow:
        apply_sect_task_state(uow)
        apply_sect_task_claim_operations(uow)
        apply_sect_task_settlement_operations(uow)
        apply_sect_task_weekly_event_status(uow)
        uow.execute('CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,sect_id INTEGER,'
                    'stone INTEGER,hp INTEGER,exp INTEGER,sect_task INTEGER,sect_contribution INTEGER)')
        uow.execute("INSERT INTO user_xiuxian VALUES('user',1,1000,500,2000,0,20)")
        uow.execute('CREATE TABLE sects(sect_id INTEGER PRIMARY KEY,sect_used_stone INTEGER,'
                    'sect_scale INTEGER,sect_materials INTEGER)')
        uow.execute('INSERT INTO sects VALUES(1,50,100,200)')
        uow.execute('CREATE TABLE isolated_effect(value TEXT)')
    clock = SimpleNamespace(now=lambda: datetime(2026, 11, 18, 12, tzinfo=timezone.utc))
    app = SectApplication(database, clock=clock)
    return SimpleNamespace(database=database, app=app, clock=clock)


def snapshot(state):
    with DatabaseUnitOfWork(state.database, read_only=True) as uow:
        return tuple(uow.query_all(f'SELECT * FROM {table} ORDER BY rowid') for table in
                     ('user_xiuxian', 'sects', 'sect_task_state', 'sect_task_claim_operations',
                      'sect_task_settlement_operations', 'isolated_effect'))


def accept(state):
    assert state.app.claim_task('initial', 'user', 1, {'old': TASK}, 3).applied


def fault(state, table, verb, action, rewrite):
    body = ('SELECT RAISE(IGNORE);' if action == 'ignore' else
            "SELECT RAISE(ABORT,'isolated task abort');" if action == 'abort' else rewrite)
    timing = 'BEFORE' if action in {'ignore', 'abort'} else 'AFTER'
    with DatabaseUnitOfWork(state.database) as uow:
        uow.execute(f'CREATE TRIGGER refuse_task {timing} {verb} ON {table} '
                    "BEGIN INSERT INTO isolated_effect VALUES('effect'); " + body + ' END')


@pytest.mark.parametrize('mode', ['claim', 'refresh'])
@pytest.mark.parametrize('step', ['state', 'receipt'])
@pytest.mark.parametrize('action', ['ignore', 'rewrite', 'late', 'abort'])
def test_claim_refresh_verify_both_writes_and_rollback_side_effects(state, mode, step, action):
    if mode == 'refresh':
        accept(state)
    current = state.app.get_active_task('user')
    table, verb, rewrite = (
        ('sect_task_state', 'UPDATE' if mode == 'refresh' else 'INSERT',
         "UPDATE sect_task_state SET task_key='corrupted';") if step == 'state' else
        ('sect_task_claim_operations', 'INSERT', "UPDATE sect_task_claim_operations SET task_key='corrupted';")
    )
    if action == 'late':
        table, verb = 'sect_task_claim_operations', 'INSERT'
    fault(state, table, verb, action, rewrite)
    before = snapshot(state)
    def execute(app):
        if mode == 'claim':
            return app.claim_task('request', 'user', 1, {'new': TASK}, 3)
        return app.refresh_task('request', 'user', 1, current, {'new': TASK}, 3)
    if action == 'abort':
        with pytest.raises(sqlite3.IntegrityError, match='isolated task abort'):
            execute(state.app)
    else:
        result = execute(state.app)
        assert not result.applied, result
        assert result['reason'] == f'sect_task_{mode}_{step}_write_rejected', result
        assert result['subject'] == ('user' if step == 'state' else 'request')
    assert snapshot(state) == before
    with DatabaseUnitOfWork(state.database) as uow:
        uow.execute('DROP TRIGGER refuse_task')
    restarted = SectApplication(state.database, clock=state.clock)
    assert execute(restarted).applied
    saved = snapshot(state)
    assert restarted.get_active_task('user')['任务名称'] == 'new'
    assert execute(restarted).status == 'duplicate' and snapshot(state) == saved


@pytest.mark.parametrize('step', ['user', 'sect', 'state', 'receipt'])
@pytest.mark.parametrize('action', ['ignore', 'rewrite', 'late', 'abort'])
def test_settlement_verifies_all_writes_and_recovers_once(state, step, action):
    accept(state)
    table, verb, rewrite = {
        'user': ('user_xiuxian', 'UPDATE', 'UPDATE user_xiuxian SET stone=0;'),
        'sect': ('sects', 'UPDATE', 'UPDATE sects SET sect_materials=0;'),
        'state': ('sect_task_state', 'UPDATE', "UPDATE sect_task_state SET status='accepted';"),
        'receipt': ('sect_task_settlement_operations', 'INSERT', 'UPDATE sect_task_settlement_operations SET cost=999;'),
    }[step]
    if action == 'late':
        table, verb = 'sect_task_settlement_operations', 'INSERT'
    fault(state, table, verb, action, rewrite)
    before = snapshot(state)
    def execute(app):
        return app.settle_task('settle', 'user', 1, DAY, 'stone', 150, 300, 25, 'old', TASK)
    if action == 'abort':
        with pytest.raises(sqlite3.IntegrityError, match='isolated task abort'):
            execute(state.app)
    else:
        result = execute(state.app)
        assert not result.applied, result
        assert result['reason'] == f'sect_task_settlement_{step}_write_rejected', result
        assert result['subject'] == {'user': 'user', 'sect': '1', 'state': 'user', 'receipt': 'settle'}[step]
    assert snapshot(state) == before
    with DatabaseUnitOfWork(state.database) as uow:
        uow.execute('DROP TRIGGER refuse_task')
    restarted = SectApplication(state.database, clock=state.clock)
    assert execute(restarted).status == 'settled'
    saved = snapshot(state)
    assert saved[0][0]['stone'] == 850 and saved[0][0]['sect_task'] == 1
    assert saved[1][0]['sect_materials'] == 450
    assert execute(restarted).status == 'duplicate' and snapshot(state) == saved
    with DatabaseUnitOfWork(state.database, read_only=True) as uow:
        assert uow.query_one('PRAGMA integrity_check') == {'integrity_check': 'ok'}


def test_task_business_day_uses_configured_zone_not_host_timezone(state):
    previous = os.environ.get('TZ')
    try:
        os.environ['TZ'] = 'UTC'
        time.tzset()
        state.clock.now = lambda: datetime(2026, 11, 17, 16, 1, tzinfo=timezone.utc)
        assert state.app.current_task_period() == DAY
        accept(state)
        assert state.app.get_active_task('user')['accepted_at'] == DAY+' 00:01:00'
        state.clock.now = lambda: datetime(2026, 11, 18, 0, 1, tzinfo=ZoneInfo('Asia/Shanghai'))
        assert state.app.get_active_task('user')['period'] == DAY
        custom = SectApplication(state.database, clock=state.clock, task_timezone=ZoneInfo('America/New_York'))
        assert custom.current_task_period() == '2026-11-17'
        state.clock.now = lambda: datetime(2026, 11, 19, 0, 1)
        assert state.app.current_task_period() == '2026-11-19'
    finally:
        if previous is None:
            os.environ.pop('TZ', None)
        else:
            os.environ['TZ'] = previous
        time.tzset()


def test_old_receipts_read_without_mutating_current_task_or_new_day(state):
    accept(state)
    current = state.app.get_active_task('user')
    assert state.app.refresh_task('refresh', 'user', 1, current, {'new': TASK}, 3).applied
    assert state.app.settle_task('settle', 'user', 1, DAY, 'hp', 100, 300, 25).applied
    state.clock.now = lambda: datetime(2026, 11, 19, 12)
    assert state.app.claim_task('next', 'user', 1, {'tomorrow': TASK}, 3).applied
    saved = snapshot(state)
    assert state.app.task_claim_receipt('initial', 'user')['task_key'] == 'old'
    assert state.app.task_claim_receipt('refresh', 'user')['task_key'] == 'new'
    assert state.app.task_settlement_receipt('settle', 'user')['cost'] == 100
    assert snapshot(state) == saved
    for call in (state.app.task_claim_receipt, state.app.task_settlement_receipt):
        with pytest.raises(DomainError) as caught:
            call('initial' if call == state.app.task_claim_receipt else 'settle', 'other')
        assert caught.value.code == 'operation_conflict'
    assert state.app.refresh_task('stale', 'user', 1, current, {'bad': TASK}, 3)['reason'] == 'sect_task_period_changed'
    assert snapshot(state) == saved


def test_stale_completion_day_rejects_but_historical_success_still_replays(state):
    accept(state)
    state.clock.now = lambda: datetime(2026, 11, 19, 0, 1)
    before = snapshot(state)
    result = state.app.settle_task('stale', 'user', 1, DAY, 'stone', 150, 300, 25, enforce_current_period=True)
    assert result['reason'] == 'sect_task_period_changed' and snapshot(state) == before
    state.clock.now = lambda: datetime(2026, 11, 18, 23, 59)
    assert state.app.settle_task('done', 'user', 1, DAY, 'stone', 150, 300, 25, enforce_current_period=True).applied
    state.clock.now = lambda: datetime(2026, 11, 19, 0, 1)
    before = snapshot(state)
    assert state.app.settle_task('done', 'user', 1, DAY, 'stone', 999, 999, 999, enforce_current_period=True).status == 'duplicate'
    assert snapshot(state) == before


def test_historical_scientific_real_rewards_replay_without_rewriting(state):
    with DatabaseUnitOfWork(state.database) as uow:
        uow.execute("INSERT INTO sect_task_settlement_operations(operation_id,user_id,sect_id,period,cost_type,cost,exp_reward,sect_reward,materials_reward) "
                    "VALUES('old','user',1,?,'stone',10,1e25,1e24,1e25)", (DAY,))
    before = snapshot(state)
    receipt = state.app.task_settlement_receipt('old', 'user')
    assert receipt['exp_reward'] == int(1e25) and receipt['sect_reward'] == int(1e24)
    assert snapshot(state) == before


def test_existing_fractional_hp_exp_and_scale_keep_their_arithmetic(state):
    accept(state)
    with DatabaseUnitOfWork(state.database) as uow:
        uow.execute('UPDATE user_xiuxian SET hp=500.5,exp=2000.25')
        uow.execute('UPDATE sects SET sect_scale=100.75')
    assert state.app.settle_task('fractional', 'user', 1, DAY, 'hp', 100, 300, 25).applied
    saved = snapshot(state)
    assert saved[0][0]['hp'] == 400.5 and saved[0][0]['exp'] == 2300.25
    assert saved[1][0]['sect_scale'] == 125.75


@pytest.mark.parametrize('field,table,reason', [
    ('sect_task', 'user_xiuxian', 'sect_task_invalid_numeric_value'),
    ('stone', 'user_xiuxian', 'stone_invalid_numeric_value'),
    ('exp', 'user_xiuxian', 'exp_invalid_numeric_value'),
    ('sect_contribution', 'user_xiuxian', 'sect_contribution_invalid_numeric_value'),
    ('sect_materials', 'sects', 'sect_materials_invalid_numeric_value'),
])
def test_bad_numeric_data_preserves_reason_and_zero_writes(state, field, table, reason):
    accept(state)
    with DatabaseUnitOfWork(state.database) as uow:
        uow.execute(f"UPDATE {table} SET {field}='broken'")
    before = snapshot(state)
    with pytest.raises(DomainError) as caught:
        state.app.settle_task('settle', 'user', 1, DAY, 'stone', 150, 300, 25)
    assert caught.value.details.get('reason', caught.value.code) == reason
    assert snapshot(state) == before


def test_exact_large_values_and_historical_receipt_format_are_preserved(state):
    accept(state)
    large = 2**70+71
    with DatabaseUnitOfWork(state.database) as uow:
        uow.execute('UPDATE user_xiuxian SET stone=xiuxian_int_store(?),exp=xiuxian_int_store(?)', (str(large), str(large)))
        uow.execute('UPDATE sects SET sect_scale=xiuxian_int_store(?)', (str(large),))
    assert state.app.settle_task('settle', 'user', 1, DAY, 'stone', 150, large, large).applied
    saved = snapshot(state)
    assert int(saved[0][0]['stone']) == large-150 and int(saved[0][0]['exp']) == large*2
    assert int(saved[1][0]['sect_scale']) == large*2
    assert state.app.task_settlement_receipt('settle', 'user')['sect_reward'] == large
    assert state.app.settle_task('settle', 'user', 1, DAY, 'stone', 999, 999, 999).status == 'duplicate'
    assert snapshot(state) == saved


@pytest.mark.parametrize('same_key', [True, False])
def test_concurrent_settlement_awards_once(state, same_key):
    accept(state)
    def settle(key):
        return SectApplication(state.database, clock=state.clock).settle_task(key, 'user', 1, DAY, 'stone', 150, 300, 25)
    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(settle, ('same', 'same' if same_key else 'other')))
    assert sorted(r.status for r in results) == sorted(('settled', 'duplicate' if same_key else 'task_missing'))
    saved = snapshot(state)
    assert saved[0][0]['stone'] == 850 and saved[0][0]['sect_task'] == 1 and len(saved[4]) == 1


@pytest.mark.parametrize('mode', ['claim', 'refresh', 'settlement'])
def test_legacy_facade_shares_verified_transactions_and_specific_reasons(state, mode):
    from nonebot_plugin_xiuxian_2.compatibility.legacy_sect_membership import SectMembershipService
    if mode != 'claim':
        accept(state)
    table = 'sect_task_settlement_operations' if mode == 'settlement' else 'sect_task_claim_operations'
    fault(state, table, 'INSERT', 'ignore', '')
    before = snapshot(state)
    service = SectMembershipService(state.database)
    def execute():
        if mode == 'claim':
            return service.claim_task('request', 'user', 1, DAY, 'old', TASK, 3)
        if mode == 'refresh':
            return service.refresh_task('request', 'user', 1, DAY, 'old', TASK, 'new', TASK, 3)
        return service.settle_task('request', 'user', 1, DAY, 'stone', 150, 300, 25)
    result = execute()
    assert not result.applied and result.reason == f'sect_task_{mode}_receipt_write_rejected' and result.subject == 'request'
    assert snapshot(state) == before
    with DatabaseUnitOfWork(state.database) as uow:
        uow.execute('DROP TRIGGER refuse_task')
    assert execute().applied
    saved = snapshot(state)
    assert execute().status == 'duplicate' and snapshot(state) == saved


@pytest.mark.parametrize('name,inner', [('sect_task_', '_claim_sect_task'),
                                     ('sect_task_refresh_', '_refresh_sect_task'),
                                     ('sect_task_complete_', '_complete_sect_task')])
@pytest.mark.parametrize('error', [DomainError('sect_task_data_invalid', 'private sql'), RuntimeError('private sql')])
def test_real_handlers_keep_known_unknown_and_failed_delivery_contract(name, inner, error):
    from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_sect as module
    event = SimpleNamespace(get_user_id=lambda: 'user')
    send = AsyncMock()
    with patch.object(module, inner, AsyncMock(side_effect=error)), patch.object(module, 'handle_send', send):
        asyncio.run(getattr(module, name)(None, event))
    reply = send.call_args.args[2]
    assert 'private sql' not in reply
    assert ('任务内容损坏' in reply) if isinstance(error, DomainError) else ('定位号' in reply)
    failure = RuntimeError('isolated delivery failed')
    with patch.object(module, inner, AsyncMock(side_effect=error)), patch.object(module, 'handle_send', AsyncMock(side_effect=failure)):
        with pytest.raises(RuntimeError) as caught:
            asyncio.run(getattr(module, name)(None, event))
    assert caught.value is failure


@pytest.mark.parametrize('flow', [FinishedException(), ProcessException()])
def test_task_normal_flow_passes_through(flow):
    from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_sect as module
    for name, inner in (('sect_task_', '_claim_sect_task'), ('sect_task_refresh_', '_refresh_sect_task'),
                        ('sect_task_complete_', '_complete_sect_task')):
        with patch.object(module, inner, AsyncMock(side_effect=flow)), patch.object(module, 'handle_send', AsyncMock()) as send:
            with pytest.raises(type(flow)) as caught:
                asyncio.run(getattr(module, name)(None, None))
        assert caught.value is flow and not send.called
