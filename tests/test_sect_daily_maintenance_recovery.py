"""Daily sect resets must verify every write before committing the whole day."""
import asyncio
import json
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import patch
from zoneinfo import ZoneInfo

import nonebot
import pytest
from apscheduler.events import EVENT_JOB_ERROR, EVENT_JOB_EXECUTED
from apscheduler.schedulers.asyncio import AsyncIOScheduler

from nonebot_plugin_xiuxian_2.features.sect.application import SectApplication
from nonebot_plugin_xiuxian_2.features.sect.daily_maintenance_repository import SectDailyMaintenanceSqlRepository
from nonebot_plugin_xiuxian_2.features.sect.migrations import apply_sect_operations
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_sect as sect, xiuxian_scheduler as scheduled
from tests.test_scheduled_job_failure_visibility import _execute_job

DAY = '2026-11-18'
COSTS = {1: 200, 2: 600}


@pytest.fixture
def state(tmp_path):
    database = tmp_path / 'daily.db'
    with DatabaseUnitOfWork(database) as uow:
        apply_sect_operations(uow)
        uow.execute('CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,sect_task INTEGER,sect_elixir_get INTEGER)')
        uow.execute("INSERT INTO user_xiuxian VALUES('first',3,1),('second',5,1)")
        uow.execute('CREATE TABLE sects(sect_id INTEGER PRIMARY KEY,sect_name TEXT,sect_owner TEXT,'
                    'elixir_room_level INTEGER,sect_materials INTEGER)')
        for row in ((1, 'charged', 'owner1', 2, 700), (2, 'downgraded', 'owner2', 2, 500),
                    (3, 'disabled', 'owner3', 1, 100), (4, 'no_room', 'owner4', 0, 900),
                    (5, 'inactive', None, 2, 900), (6, 'level_unsupported', 'owner6', 9, 100)):
            uow.execute('INSERT INTO sects VALUES(?,?,?,?,?)', row)
        uow.execute('CREATE TABLE user_cd(user_id TEXT PRIMARY KEY,last_check_info_time TEXT)')
    app = SectApplication(database)
    clock = SimpleNamespace(moment=datetime(2026, 11, 17, 16, 1, tzinfo=timezone.utc))
    clock.now = lambda: clock.moment
    config = {'宗门丹房参数': {'elixir_room_level': {str(k): {'level_up_cost': {'建设度': v}} for k, v in COSTS.items()}}}
    with patch.object(sect, 'sect_application', app), patch.object(sect, 'runtime_clock', clock), \
            patch.object(sect, 'config', config):
        yield SimpleNamespace(database=database, app=app, clock=clock)


def snapshot(state):
    with DatabaseUnitOfWork(state.database, read_only=True) as uow:
        return tuple(uow.query_all(f'SELECT * FROM {table} ORDER BY rowid') for table in
                     ('user_xiuxian', 'sects', 'sect_daily_reset_operations', 'user_cd'))


def install_fault(state, step, action):
    table, verb, condition, mutation = {
        'user': ('user_xiuxian', 'UPDATE', "WHEN NEW.user_id='second'", "UPDATE user_xiuxian SET sect_task=9 WHERE user_id='second';"),
        'material': ('sects', 'UPDATE', 'WHEN NEW.sect_id=1', 'UPDATE sects SET sect_materials=999 WHERE sect_id=1;'),
        'level': ('sects', 'UPDATE', 'WHEN NEW.sect_id=2', 'UPDATE sects SET elixir_room_level=2 WHERE sect_id=2;'),
        'receipt': ('sect_daily_reset_operations', 'INSERT', '', 'UPDATE sect_daily_reset_operations SET user_count=999;'),
    }[step]
    if action == 'late_rewrite':
        table, verb, condition = 'sect_daily_reset_operations', 'INSERT', ''
    statement = ('SELECT RAISE(IGNORE);' if action == 'ignore' else
                 "SELECT RAISE(ABORT,'isolated sect daily abort');" if action == 'abort' else mutation)
    timing = 'BEFORE' if action in {'ignore', 'abort'} else 'AFTER'
    with DatabaseUnitOfWork(state.database) as uow:
        uow.execute(f'CREATE TRIGGER refuse_daily {timing} {verb} ON {table} {condition} '
                    "BEGIN INSERT INTO user_cd VALUES('side-effect','test'); " + statement + ' END')


@pytest.mark.parametrize('step', ['user', 'material', 'level', 'receipt'])
@pytest.mark.parametrize('action', ['ignore', 'rewrite', 'late_rewrite'])
def test_rejected_or_rewritten_step_rolls_back_all_players_sects_and_trigger_effects(state, step, action):
    install_fault(state, step, action)
    before = snapshot(state)
    result = state.app.reset_daily_maintenance(DAY, COSTS)
    assert not result.applied and result.reason == f'sect_daily_reset_{step}_write_rejected'
    assert result.subject == {'user': 'second', 'material': '1', 'level': '2', 'receipt': DAY}[step]
    assert snapshot(state) == before
    with patch.object(sect, 'logger') as logger:
        event = _execute_job(scheduled.daily_reset_sect_task)
    assert event.code == EVENT_JOB_ERROR and result.reason in str(event.exception)
    assert f'subject={result.subject}' in str(event.exception)
    assert not logger.opt.return_value.info.called and snapshot(state) == before
    with DatabaseUnitOfWork(state.database) as uow:
        uow.execute('DROP TRIGGER refuse_daily')
    restarted = SectApplication(state.database)
    with patch.object(sect, 'sect_application', restarted):
        assert _execute_job(scheduled.daily_reset_sect_task).code == EVENT_JOB_EXECUTED
        with DatabaseUnitOfWork(state.database) as uow:
            uow.execute('UPDATE user_xiuxian SET sect_task=7,sect_elixir_get=1')
            uow.execute('UPDATE sects SET sect_materials=101 WHERE sect_id=1')
        saved = snapshot(state)
        assert _execute_job(scheduled.daily_reset_sect_task).code == EVENT_JOB_EXECUTED
        assert snapshot(state) == saved


@pytest.mark.parametrize('step', ['user', 'material', 'level', 'receipt'])
def test_original_abort_reaches_real_scheduler_and_rolls_back_entire_day(state, step):
    install_fault(state, step, 'abort')
    before = snapshot(state)
    event = _execute_job(scheduled.daily_reset_sect_task)
    assert event.code == EVENT_JOB_ERROR and 'isolated sect daily abort' in str(event.exception)
    assert snapshot(state) == before


def test_normal_branches_business_days_timezone_and_new_usage_replay(state):
    state.clock.moment = datetime(2026, 11, 17, 15, 59, 59, 999999, tzinfo=timezone.utc)
    assert _execute_job(scheduled.daily_reset_sect_task).code == EVENT_JOB_EXECUTED
    result = state.app.reset_daily_maintenance('2026-11-17', COSTS)
    assert result.status == 'duplicate' and result.user_count == 2
    assert [outcome.status for outcome in result.outcomes] == [
        'charged', 'downgraded', 'disabled', 'no_room', 'inactive', 'level_unsupported']
    rows = snapshot(state)
    assert [(row['elixir_room_level'], row['sect_materials']) for row in rows[1]] == [
        (2, 100), (1, 500), (0, 100), (0, 900), (2, 900), (9, 100)]
    state.clock.moment += timedelta(microseconds=1)
    assert _execute_job(scheduled.daily_reset_sect_task).code == EVENT_JOB_EXECUTED
    with DatabaseUnitOfWork(state.database) as uow:
        uow.execute('UPDATE user_xiuxian SET sect_task=4,sect_elixir_get=1')
    before = snapshot(state)
    assert [row['business_date'] for row in before[2]] == ['2026-11-17', DAY]
    state.clock.moment = state.clock.moment.astimezone(ZoneInfo('Asia/Shanghai'))
    assert _execute_job(scheduled.daily_reset_sect_task).code == EVENT_JOB_EXECUTED
    state.clock.moment = datetime(2026, 11, 18)
    assert _execute_job(scheduled.daily_reset_sect_task).code == EVENT_JOB_EXECUTED
    assert snapshot(state) == before
    state.clock.moment = datetime(2026, 11, 18, 16, tzinfo=timezone.utc)
    with patch.object(scheduled, 'scheduler', SimpleNamespace(timezone=timezone.utc)):
        assert _execute_job(sect.resetusertask).code == EVENT_JOB_EXECUTED
    assert snapshot(state) == before
    assert _execute_job(scheduled.daily_reset_sect_task).code == EVENT_JOB_EXECUTED
    assert all(row['sect_task'] == row['sect_elixir_get'] == 0 for row in snapshot(state)[0])
    with DatabaseUnitOfWork(state.database, read_only=True) as uow:
        assert uow.query_one('PRAGMA integrity_check') == {'integrity_check': 'ok'}


def test_actual_registration_pending_failure_retry_cron_boundary_and_execution(state):
    from nonebot_plugin_xiuxian_2.compatibility.scheduler import DeferredScheduler
    declaration = next((job for job in getattr(scheduled.scheduler, '_pending', ()) if job.job_id == 'daily_reset_sect_task'), None)
    if declaration is None:
        live = scheduled.scheduler.get_job('daily_reset_sect_task')
        assert live and live.func is scheduled.daily_reset_sect_task
        args = dict(trigger=live.trigger, id=live.id, coalesce=live.coalesce,
                    max_instances=live.max_instances, misfire_grace_time=live.misfire_grace_time)
    else:
        assert declaration.function is scheduled.daily_reset_sect_task
        args = dict(declaration.kwargs, trigger=declaration.args[0])

    async def run():
        scheduler = AsyncIOScheduler(timezone='Asia/Shanghai')
        bridge = DeferredScheduler(scheduler)
        bridge.add_job(scheduled.daily_reset_sect_task, **args)
        error = RuntimeError('isolated sect daily registration failure')
        with patch.object(scheduler, 'add_job', side_effect=error):
            for _ in range(2):
                with pytest.raises(RuntimeError) as caught:
                    bridge.activate()
                assert caught.value is error and not bridge.active
                assert bridge.pending_ids == ('daily_reset_sect_task',)
        bridge.activate()
        bridge.activate()
        assert len(scheduler.get_jobs()) == 1 and not bridge.pending_ids
        job = scheduler.get_job('daily_reset_sect_task')
        assert job.coalesce and job.max_instances == 1 and job.misfire_grace_time == 300
        cutoff = datetime(2026, 11, 18, 8, 0, 15, tzinfo=ZoneInfo('Asia/Shanghai'))
        assert job.trigger.get_next_fire_time(None, cutoff-timedelta(microseconds=1)) == cutoff
        assert job.trigger.get_next_fire_time(None, cutoff) == cutoff
        assert job.trigger.get_next_fire_time(None, cutoff+timedelta(microseconds=1)) == cutoff+timedelta(days=1)
        outcome = asyncio.get_running_loop().create_future()
        scheduler.add_listener(outcome.set_result, EVENT_JOB_ERROR | EVENT_JOB_EXECUTED)
        scheduler.modify_job(job.id, next_run_time=datetime.now(timezone.utc))
        scheduler.start()
        try:
            assert (await asyncio.wait_for(outcome, timeout=3)).code == EVENT_JOB_EXECUTED
            assert snapshot(state)[2][0]['business_date'] == DAY
        finally:
            scheduler.shutdown(wait=False)
            await asyncio.sleep(0)
    asyncio.run(run())


def test_large_material_deduction_and_old_spaced_payload_replay(state):
    total = 2**64+7
    with DatabaseUnitOfWork(state.database) as uow:
        uow.execute('UPDATE sects SET sect_materials=xiuxian_int_store(?) WHERE sect_id=1', (str(total),))
    result = state.app.reset_daily_maintenance(DAY, COSTS)
    assert result.status == 'applied'
    assert int(snapshot(state)[1][0]['sect_materials']) == total-600
    with DatabaseUnitOfWork(state.database) as uow:
        uow.execute('UPDATE sect_daily_reset_operations SET payload=?', (json.dumps(COSTS, sort_keys=True),))
    before = snapshot(state)
    assert state.app.reset_daily_maintenance(DAY, COSTS).status == 'duplicate'
    assert snapshot(state) == before
    result = state.app.reset_daily_maintenance(DAY, {1: 200, 2: 601})
    assert result.status == result.reason == 'operation_conflict' and result.subject == DAY
    assert snapshot(state) == before


def test_large_insufficient_materials_downgrades_without_changing_exact_balance(state):
    total = 2**64+7
    with DatabaseUnitOfWork(state.database) as uow:
        uow.execute('UPDATE sects SET sect_materials=xiuxian_int_store(?) WHERE sect_id=1', (str(total),))
    result = state.app.reset_daily_maintenance(DAY, {1: 200, 2: 2**65})
    assert result.status == 'applied' and result.outcomes[0].status == 'downgraded'
    saved = snapshot(state)[1][0]
    assert saved['elixir_room_level'] == 1 and int(saved['sect_materials']) == total


@pytest.mark.parametrize('field,value,reason', [('sect_materials', 'broken', 'sect_materials_invalid_numeric_value'),
                                               ('elixir_room_level', 'broken', 'elixir_room_level_invalid_numeric_value')])
def test_bad_state_preserves_specific_reason_and_rolls_back_reset(state, field, value, reason):
    with DatabaseUnitOfWork(state.database) as uow:
        uow.execute(f'UPDATE sects SET {field}=? WHERE sect_id=1', (value,))
    before = snapshot(state)
    result = state.app.reset_daily_maintenance(DAY, COSTS)
    assert result.reason == reason and result.subject == '1'
    assert snapshot(state) == before


@pytest.mark.parametrize('column,value', [('payload', 'broken'), ('outcomes', '{}'), ('outcomes', '[{}]')])
def test_corrupt_historical_receipt_is_not_unknown_or_changed_configuration(state, column, value):
    assert state.app.reset_daily_maintenance(DAY, COSTS).status == 'applied'
    with DatabaseUnitOfWork(state.database) as uow:
        uow.execute(f'UPDATE sect_daily_reset_operations SET {column}=?', (value,))
    before = snapshot(state)
    result = state.app.reset_daily_maintenance(DAY, COSTS)
    assert result.reason == 'sect_daily_reset_receipt_invalid' and result.subject == DAY
    assert snapshot(state) == before


@pytest.mark.parametrize('step', ['user', 'material', 'level', 'receipt'])
def test_legacy_service_shares_write_checks_and_old_replay(state, step):
    from nonebot_plugin_xiuxian_2.compatibility.legacy_sect_daily_maintenance import SectDailyResetMaintenanceService
    install_fault(state, step, 'ignore')
    before = snapshot(state)
    result = SectDailyResetMaintenanceService(state.database).settle(DAY, COSTS)
    assert not result.applied and result.reason == f'sect_daily_reset_{step}_write_rejected'
    assert snapshot(state) == before


def test_absent_database_is_not_created(tmp_path):
    path = tmp_path / 'missing.db'
    result = SectDailyMaintenanceSqlRepository(path).settle(DAY, COSTS)
    assert result.status == result.reason == 'schema_missing' and not path.exists()
