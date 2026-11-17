"""Scheduled material grants must persist before the scheduler reports success."""
import asyncio
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import patch
from zoneinfo import ZoneInfo

import nonebot
import pytest
from apscheduler.events import EVENT_JOB_ERROR, EVENT_JOB_EXECUTED
from apscheduler.schedulers.asyncio import AsyncIOScheduler

from nonebot_plugin_xiuxian_2.features.sect.application import SectApplication
from nonebot_plugin_xiuxian_2.features.sect.scheduled_material_repository import SectScheduledMaterialSqlRepository
from nonebot_plugin_xiuxian_2.features.sect.migrations import apply_sect_scheduled_materials
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_sect as sect
from tests.test_scheduled_job_failure_visibility import _execute_job


@pytest.fixture
def state(tmp_path):
    database = tmp_path / 'materials.db'
    with DatabaseUnitOfWork(database) as uow:
        uow.execute('CREATE TABLE sects(sect_id INTEGER PRIMARY KEY,sect_owner TEXT,sect_scale INTEGER,'
                    'sect_materials INTEGER,combat_power INTEGER,elixir_room_level INTEGER)')
        uow.execute("INSERT INTO sects VALUES(1,'owner',120,50,1,0)")
        uow.execute('CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,sect_id INTEGER,power INTEGER)')
        uow.execute("INSERT INTO user_xiuxian VALUES('owner',1,300)")
        apply_sect_scheduled_materials(uow)
    app = SectApplication(database)
    clock = SimpleNamespace(now=lambda: datetime(2026, 11, 17, 16, 1, tzinfo=timezone.utc))
    with patch.object(sect, 'sect_application', app), patch.object(sect, 'runtime_clock', clock), \
            patch.object(sect, 'config', {'发放宗门资材': {'倍率': 2}}):
        yield SimpleNamespace(database=database, app=app, clock=clock)


def snapshot(state):
    with DatabaseUnitOfWork(state.database, read_only=True) as uow:
        return [uow.query_all(f'SELECT * FROM {table} ORDER BY rowid') for table in
                ('sects', 'user_xiuxian', 'sect_scheduled_material_grants')]


@pytest.mark.parametrize('target,action', [('state', 'ignore'), ('receipt', 'ignore'),
                                         ('state', 'rewrite'), ('receipt', 'rewrite'), ('state', 'late_rewrite'),
                                         ('state', 'power_rewrite'), ('receipt', 'power_rewrite')])
def test_refused_write_rolls_back_all_state_and_receipts(state, target, action):
    table = 'sects' if target == 'state' and action != 'late_rewrite' else 'sect_scheduled_material_grants'
    event = 'UPDATE' if table == 'sects' else 'INSERT'
    timing = 'BEFORE' if action == 'ignore' else 'AFTER'
    field = 'combat_power' if action == 'power_rewrite' else 'sect_materials' if target == 'state' else 'materials'
    mutation = 'SELECT RAISE(IGNORE);' if action == 'ignore' else (
        f'UPDATE {"sects" if target == "state" else "sect_scheduled_material_grants"} SET {field}=999;')
    with DatabaseUnitOfWork(state.database) as uow:
        uow.execute(f'CREATE TRIGGER refuse_materials {timing} {event} ON {table} BEGIN '
                    f'UPDATE user_xiuxian SET power=0; {mutation} END')
    before = snapshot(state)
    result = state.app.grant_scheduled_materials('test-day', 1, 2)
    assert not result.applied
    assert (result['reason'], result['subject']) == (f'sect_material_{target}_write_rejected', '1')
    assert snapshot(state) == before
    with DatabaseUnitOfWork(state.database) as uow:
        uow.execute('DROP TRIGGER refuse_materials')
    restarted = SectApplication(state.database)
    assert restarted.grant_scheduled_materials('test-day', 1, 2).status == 'granted'
    saved = snapshot(state)
    assert restarted.grant_scheduled_materials('test-day', 1, 99).status == 'duplicate'
    assert snapshot(state) == saved


def test_missing_receipt_migration_is_a_real_scheduler_failure(state):
    with DatabaseUnitOfWork(state.database) as uow:
        uow.execute('DROP TABLE sect_scheduled_material_grants')
    with patch.object(sect, 'logger') as logger:
        event = _execute_job(sect.materialsupdate_)
    assert event.code == EVENT_JOB_ERROR and 'schema_missing' in str(event.exception)
    assert 'sect_id=1' in str(event.exception)
    assert not logger.opt.return_value.info.called


def test_scheduled_grant_uses_scheduler_business_day(state):
    assert _execute_job(sect.materialsupdate_).code == EVENT_JOB_EXECUTED
    with DatabaseUnitOfWork(state.database, read_only=True) as uow:
        row = uow.query_one('SELECT grant_key FROM sect_scheduled_material_grants')
    assert row == {'grant_key': 'sect-materials:2026-11-18'}


@pytest.mark.parametrize('target', ['state', 'receipt'])
def test_actual_task_failure_preserves_prior_sect_and_retries_remaining(state, target):
    table, event = ('sects', 'UPDATE') if target == 'state' else ('sect_scheduled_material_grants', 'INSERT')
    with DatabaseUnitOfWork(state.database) as uow:
        uow.execute("INSERT INTO sects VALUES(2,'second',100,20,1,0)")
        uow.execute(f'CREATE TRIGGER refuse_materials BEFORE {event} ON {table} WHEN NEW.sect_id=2 '
                    'BEGIN UPDATE user_xiuxian SET power=0; SELECT RAISE(IGNORE); END')
    outcome = _execute_job(sect.materialsupdate_)
    assert outcome.code == EVENT_JOB_ERROR
    assert f'sect_material_{target}_write_rejected' in str(outcome.exception) and 'sect_id=2' in str(outcome.exception)
    with DatabaseUnitOfWork(state.database) as uow:
        assert uow.query_all('SELECT sect_id,sect_materials FROM sects ORDER BY sect_id') == [
            {'sect_id': 1, 'sect_materials': 290}, {'sect_id': 2, 'sect_materials': 20}]
        assert uow.query_one('SELECT power FROM user_xiuxian')['power'] == 300
        assert uow.query_one('SELECT COUNT(*) AS n FROM sect_scheduled_material_grants')['n'] == 1
        uow.execute('DROP TRIGGER refuse_materials')
        uow.execute('UPDATE sects SET sect_materials=291 WHERE sect_id=1')
    with patch.object(sect, 'sect_application', SectApplication(state.database)):
        assert _execute_job(sect.materialsupdate_).code == EVENT_JOB_EXECUTED
        before = snapshot(state)
        with patch.object(sect, 'logger') as logger:
            assert _execute_job(sect.materialsupdate_).code == EVENT_JOB_EXECUTED
        assert '本次发放 0 个宗门' in str(logger.opt.return_value.info.call_args)
        assert snapshot(state) == before
    assert before[0][0]['sect_materials'] == 291 and before[0][1]['sect_materials'] == 220


@pytest.mark.parametrize('target', ['sects', 'sect_scheduled_material_grants'])
def test_unknown_sqlite_abort_retains_original_exception_and_full_rollback(state, target):
    verb = 'UPDATE' if target == 'sects' else 'INSERT'
    with DatabaseUnitOfWork(state.database) as uow:
        uow.execute(f'CREATE TRIGGER refuse_materials BEFORE {verb} ON {target} '
                    "BEGIN UPDATE user_xiuxian SET power=0; SELECT RAISE(ABORT,'isolated material abort'); END")
    before = snapshot(state)
    outcome = _execute_job(sect.materialsupdate_)
    assert outcome.code == EVENT_JOB_ERROR and 'isolated material abort' in str(outcome.exception)
    assert snapshot(state) == before


@pytest.mark.parametrize('column', ['sect_scale', 'sect_materials'])
def test_corrupt_asset_values_are_precise_and_zero_write(state, column):
    with DatabaseUnitOfWork(state.database) as uow:
        uow.execute(f'UPDATE sects SET {column}=?', ('broken',))
    before = snapshot(state)
    result = state.app.grant_scheduled_materials('test-day', 1, 2)
    assert not result.applied and result['reason'] == f'{column}_invalid_numeric_value'
    assert result['subject'] == '1' and snapshot(state) == before
    event = _execute_job(sect.materialsupdate_)
    assert event.code == EVENT_JOB_ERROR and result['reason'] in str(event.exception)


def test_large_new_grants_and_power_round_trip_exactly_without_changing_old_replay(state):
    scale, total = 2**63+1, 2**64+3
    with DatabaseUnitOfWork(state.database) as uow:
        uow.execute('UPDATE sects SET sect_scale=xiuxian_int_store(?),sect_materials=xiuxian_int_store(?)',
                    (str(scale), str(total)))
        uow.execute('UPDATE user_xiuxian SET power=?', (2**62,))
        uow.execute("INSERT INTO user_xiuxian VALUES('member',1,?)", (2**62,))
    result = state.app.grant_scheduled_materials('large', 1, 2)
    assert result.status == 'granted' and result.materials == scale*2 and result.combat_power == 2**63
    with DatabaseUnitOfWork(state.database, read_only=True) as uow:
        row = uow.query_one('SELECT sect_materials,combat_power FROM sects')
        assert int(row['sect_materials']) == total + scale*2 and int(row['combat_power']) == 2**63
    replay = state.app.grant_scheduled_materials('large', 1, 99)
    assert replay.status == 'duplicate' and (replay.materials, replay.combat_power) == (scale*2, 2**63)


def test_existing_high_realm_real_power_keeps_legacy_integer_conversion(state):
    from nonebot_plugin_xiuxian_2.core.numeric import as_int_like
    value = 1.2e100
    with DatabaseUnitOfWork(state.database) as uow:
        uow.execute('UPDATE user_xiuxian SET power=?', (value,))
    result = state.app.grant_scheduled_materials('high-realm', 1, 2)
    assert result.status == 'granted' and result.combat_power == as_int_like(value)
    assert state.app.grant_scheduled_materials('high-realm', 1, 99).combat_power == result.combat_power


def test_corrupt_historical_material_receipt_preserves_numeric_reason_without_writes(state):
    with DatabaseUnitOfWork(state.database) as uow:
        uow.execute("INSERT INTO sect_scheduled_material_grants(grant_key,sect_id,materials,combat_power) "
                    "VALUES('sect-materials:2026-11-18',1,'broken',300)")
    before = snapshot(state)
    result = state.app.grant_scheduled_materials('sect-materials:2026-11-18', 1, 2)
    assert result['reason'] == 'sect_material_receipt_invalid_numeric_value' and result['subject'] == '1'
    event = _execute_job(sect.materialsupdate_)
    assert event.code == EVENT_JOB_ERROR and result['reason'] in str(event.exception)
    assert snapshot(state) == before


@pytest.mark.parametrize('action', ['missing', 'inactive'])
def test_normal_target_disappearance_is_not_a_failed_task(state, action):
    targets = state.app.list_scheduled_material_targets()
    with DatabaseUnitOfWork(state.database) as uow:
        uow.execute('DELETE FROM sects' if action == 'missing' else 'UPDATE sects SET sect_owner=NULL')
    before = snapshot(state)
    with patch.object(state.app, 'list_scheduled_material_targets', return_value=targets):
        assert _execute_job(sect.materialsupdate_).code == EVENT_JOB_EXECUTED
    assert snapshot(state) == before


def test_missing_database_does_not_create_empty_business_database(tmp_path):
    path = tmp_path / 'absent.db'
    result = SectScheduledMaterialSqlRepository(path).grant('day', 1, 2)
    assert result['reason'] == 'schema_missing' and not path.exists()


@pytest.mark.parametrize('target', ['sects', 'sect_scheduled_material_grants'])
def test_legacy_service_uses_the_same_write_checks(state, target):
    from nonebot_plugin_xiuxian_2.compatibility.legacy_sect_membership import SectMembershipService
    verb = 'UPDATE' if target == 'sects' else 'INSERT'
    with DatabaseUnitOfWork(state.database) as uow:
        uow.execute(f'CREATE TRIGGER refuse_materials BEFORE {verb} ON {target} BEGIN SELECT RAISE(IGNORE); END')
    before = snapshot(state)
    result = SectMembershipService(state.database).grant_scheduled_materials('day', 1, 2)
    assert not result.applied and result.reason == f'sect_material_{"state" if target == "sects" else "receipt"}_write_rejected'
    assert snapshot(state) == before


def test_clock_midnight_equivalence_naive_time_and_configured_timezone(state):
    moment = datetime(2026, 11, 17, 15, 59, 59, 999999, tzinfo=timezone.utc)
    def run(value):
        with patch.object(state.clock, 'now', return_value=value):
            assert _execute_job(sect.materialsupdate_).code == EVENT_JOB_EXECUTED
    run(moment)
    midnight = moment + timedelta(microseconds=1)
    run(midnight)
    before = snapshot(state)
    assert [row['grant_key'] for row in before[2]] == ['sect-materials:2026-11-17', 'sect-materials:2026-11-18']
    run(midnight.astimezone(ZoneInfo('Asia/Shanghai')))
    run(datetime(2026, 11, 18))
    assert snapshot(state) == before
    with patch.object(sect, 'materialsupdate', SimpleNamespace(timezone=timezone.utc)):
        run(midnight + timedelta(days=1))
    assert snapshot(state) == before
    run(midnight + timedelta(days=1))
    after = snapshot(state)
    assert after[2][-1]['grant_key'] == 'sect-materials:2026-11-19'
    assert after[0][0]['sect_materials'] == 50 + 3 * 240


def test_actual_declaration_pending_retry_cron_boundaries_and_registered_execution(state):
    from nonebot_plugin_xiuxian_2.compatibility.scheduler import DeferredScheduler
    source = sect.materialsupdate
    declaration = next((job for job in getattr(source, '_pending', ()) if job.job_id == 'sect_materials_grant'), None)
    if declaration is None:
        live = source.get_job('sect_materials_grant')
        assert live is not None and live.func is sect.materialsupdate_
        arguments = dict(trigger=live.trigger, id=live.id, coalesce=live.coalesce,
                         max_instances=live.max_instances, misfire_grace_time=live.misfire_grace_time)
    else:
        assert declaration.function is sect.materialsupdate_
        arguments = dict(declaration.kwargs, trigger=declaration.args[0])

    async def run():
        scheduler = AsyncIOScheduler(timezone='Asia/Shanghai')
        bridge = DeferredScheduler(scheduler)
        bridge.add_job(sect.materialsupdate_, **arguments)
        error = RuntimeError('isolated materials registration failure')
        with patch.object(scheduler, 'add_job', side_effect=error):
            for _ in range(2):
                with pytest.raises(RuntimeError) as caught:
                    bridge.activate()
                assert caught.value is error and not bridge.active
                assert bridge.pending_ids == ('sect_materials_grant',)
        bridge.activate()
        assert bridge.active and not bridge.pending_ids
        bridge.activate()
        assert len(scheduler.get_jobs()) == 1
        job = scheduler.get_job('sect_materials_grant')
        assert job.coalesce and job.max_instances == 1 and job.misfire_grace_time == 300
        base = datetime(2026, 11, 18, tzinfo=ZoneInfo('Asia/Shanghai'))
        cutoff = job.trigger.get_next_fire_time(None, base)
        if 'hour' in arguments:
            assert str(job.trigger.fields[5]) == str(arguments['hour'])
        assert job.trigger.get_next_fire_time(None, cutoff-timedelta(microseconds=1)) == cutoff
        assert job.trigger.get_next_fire_time(None, cutoff) == cutoff
        assert job.trigger.get_next_fire_time(None, cutoff+timedelta(microseconds=1)) > cutoff
        outcome = asyncio.get_running_loop().create_future()
        scheduler.add_listener(outcome.set_result, EVENT_JOB_ERROR | EVENT_JOB_EXECUTED)
        scheduler.modify_job(job.id, next_run_time=datetime.now(timezone.utc))
        scheduler.start()
        try:
            assert (await asyncio.wait_for(outcome, timeout=3)).code == EVENT_JOB_EXECUTED
            assert snapshot(state)[2][0]['grant_key'] == 'sect-materials:2026-11-18'
        finally:
            scheduler.shutdown(wait=False)
            await asyncio.sleep(0)
    asyncio.run(run())
