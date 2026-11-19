"""Recovery ticks must report verified batches, including partial failure."""
import sqlite3
import asyncio
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from types import SimpleNamespace
from unittest.mock import patch

import nonebot
import pytest
from apscheduler.events import EVENT_JOB_ERROR, EVENT_JOB_EXECUTED
from apscheduler.schedulers.asyncio import AsyncIOScheduler

from nonebot_plugin_xiuxian_2.features.base.stamina_application import PlayerStaminaApplication
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork
from tests.test_scheduled_job_failure_visibility import _execute_job

nonebot.init()
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils import lay_out, utils


@pytest.fixture
def recovery(tmp_path):
    database = tmp_path / 'recovery.db'
    with DatabaseUnitOfWork(database) as uow:
        uow.execute('CREATE TABLE user_xiuxian(user_id TEXT,user_stamina INTEGER)')
        for uid, amount in (('first', 0), ('second', 1), ('third', 2)):
            uow.execute('INSERT INTO user_xiuxian VALUES(?,?)', (uid, amount))
        uow.execute('CREATE TABLE isolated_effect(value TEXT)')
    app = PlayerStaminaApplication(database)
    config = SimpleNamespace(max_stamina=10, stamina_recovery_points=3)
    with patch.object(utils, '_player_stamina_application', app), patch.object(lay_out, 'XiuConfig', return_value=config):
        yield SimpleNamespace(database=database, app=app, config=config)


def snapshot(recovery):
    with DatabaseUnitOfWork(recovery.database, read_only=True) as uow:
        return [uow.query_all('SELECT rowid,user_id,user_stamina FROM user_xiuxian ORDER BY rowid'),
                uow.query_all('SELECT * FROM isolated_effect')]


@pytest.mark.parametrize('action', ['ignore', 'rewrite', 'late', 'abort'])
def test_recovery_rejects_incomplete_batch_and_rolls_back_effects(recovery, action):
    timing = 'BEFORE' if action in {'ignore', 'abort'} else 'AFTER'
    subject = 'third' if action == 'late' else 'second'
    body = ('SELECT RAISE(IGNORE);' if action == 'ignore' else
            "SELECT RAISE(ABORT,'isolated recovery abort');" if action == 'abort' else
            "UPDATE user_xiuxian SET user_stamina=0 WHERE user_id='first';" if action == 'late' else
            "UPDATE user_xiuxian SET user_stamina=0 WHERE user_id='second';")
    with DatabaseUnitOfWork(recovery.database) as uow:
        uow.execute(f"CREATE TRIGGER refuse_recovery {timing} UPDATE ON user_xiuxian WHEN NEW.user_id='{subject}' "
                    "BEGIN INSERT INTO isolated_effect VALUES('effect'); " + body + ' END')
    before = snapshot(recovery)
    failed = _execute_job(lay_out.limit_all_stamina_)
    assert failed.code == EVENT_JOB_ERROR, (failed.code, snapshot(recovery))
    if action == 'abort':
        assert isinstance(failed.exception, sqlite3.IntegrityError)
        assert 'isolated recovery abort' in str(failed.exception)
    else:
        assert 'stamina_recovery_write_rejected' in str(failed.exception)
    assert snapshot(recovery) == before
    with DatabaseUnitOfWork(recovery.database) as uow:
        uow.execute('DROP TRIGGER refuse_recovery')
    with patch.object(utils, '_player_stamina_application', PlayerStaminaApplication(recovery.database)):
        assert _execute_job(lay_out.limit_all_stamina_).code == EVENT_JOB_EXECUTED
    assert [row['user_stamina'] for row in snapshot(recovery)[0]] == [3, 4, 5]


@pytest.mark.parametrize('missing', ['file', 'column'])
def test_missing_recovery_schema_is_scheduler_error_without_creating_data(recovery, missing):
    if missing == 'file':
        recovery.app.repository.database = recovery.database.with_name('absent.db')
    else:
        with DatabaseUnitOfWork(recovery.database) as uow:
            uow.execute('ALTER TABLE user_xiuxian RENAME COLUMN user_stamina TO old_stamina')
    event = _execute_job(lay_out.limit_all_stamina_)
    assert event.code == EVENT_JOB_ERROR and 'schema_missing' in str(event.exception)
    assert not recovery.database.with_name('absent.db').exists()


@pytest.mark.parametrize('field', ['max_stamina', 'stamina_recovery_points'])
@pytest.mark.parametrize('value', [-1, 'broken', 1.5])
def test_invalid_recovery_config_is_explicit_zero_write(recovery, field, value):
    setattr(recovery.config, field, value)
    before = snapshot(recovery)
    event = _execute_job(lay_out.limit_all_stamina_)
    assert event.code == EVENT_JOB_ERROR and 'stamina_recovery_config_invalid' in str(event.exception)
    assert field in str(event.exception) and snapshot(recovery) == before


def test_partial_batches_preserve_prior_commit_and_stop_before_later_users(recovery):
    with DatabaseUnitOfWork(recovery.database) as uow:
        uow.execute("CREATE TRIGGER refuse_recovery BEFORE UPDATE ON user_xiuxian WHEN NEW.user_id='second' "
                    "BEGIN INSERT INTO isolated_effect VALUES('effect'); SELECT RAISE(IGNORE); END")
    result = recovery.app.recover(10, 3, batch_size=1)
    assert result.get('reason') == 'stamina_recovery_write_rejected', result
    assert result['updated'] == 1 and result['subject'] == 'second'
    saved = snapshot(recovery)
    assert [row['user_stamina'] for row in saved[0]] == [3, 1, 2] and saved[1] == []
    with DatabaseUnitOfWork(recovery.database) as uow:
        uow.execute('DROP TRIGGER refuse_recovery')
    # A later interval is a new tick, not an idempotent replay of the failed one.
    assert PlayerStaminaApplication(recovery.database).recover(10, 3, batch_size=1) == {'status': 'applied', 'updated': 3}
    assert [row['user_stamina'] for row in snapshot(recovery)[0]] == [6, 4, 5]


@pytest.mark.parametrize('value', ['broken', 1.5])
def test_corrupt_stamina_is_not_silently_excluded_by_sqlite_comparison(recovery, value):
    with DatabaseUnitOfWork(recovery.database) as uow:
        uow.execute("UPDATE user_xiuxian SET user_stamina=? WHERE user_id='second'", (value,))
    before = snapshot(recovery)
    event = _execute_job(lay_out.limit_all_stamina_)
    assert event.code == EVENT_JOB_ERROR
    assert event.exception.details['reason'] == 'user_stamina_invalid_numeric_value'
    assert event.exception.details['subject'] == 'second' and snapshot(recovery) == before


def test_invalid_environment_batch_size_retains_field_and_does_not_write(recovery):
    before = snapshot(recovery)
    with patch.dict('os.environ', {'XIUXIAN_STAMINA_RECOVERY_BATCH_SIZE': 'broken'}):
        event = _execute_job(lay_out.limit_all_stamina_)
    assert event.code == EVENT_JOB_ERROR
    assert event.exception.details['reason'] == 'stamina_recovery_config_invalid'
    assert event.exception.details['subject'] == 'batch_size' and snapshot(recovery) == before


def test_null_duplicate_cap_zero_points_and_each_new_tick_keep_existing_rules(recovery):
    with DatabaseUnitOfWork(recovery.database) as uow:
        uow.execute('DELETE FROM user_xiuxian')
        for value in (None, 9, 10, 12, -2):
            uow.execute("INSERT INTO user_xiuxian VALUES('same',?)", (value,))
    before = snapshot(recovery)
    assert recovery.app.recover(10, 0, batch_size=2) == {'status': 'applied', 'updated': 0}
    assert snapshot(recovery) == before
    assert recovery.app.recover(10, 3, batch_size=2) == {'status': 'applied', 'updated': 3}
    assert [row['user_stamina'] for row in snapshot(recovery)[0]] == [3, 10, 10, 12, 1]
    assert recovery.app.recover(10, 3, batch_size=0) == {'status': 'applied', 'updated': 2}
    assert [row['user_stamina'] for row in snapshot(recovery)[0]] == [6, 10, 10, 12, 4]
    with DatabaseUnitOfWork(recovery.database, read_only=True) as uow:
        assert uow.query_one('PRAGMA integrity_check') == {'integrity_check': 'ok'}


@pytest.mark.parametrize('action', ['ignore', 'abort'])
def test_real_scheduler_preserves_committed_batches_on_late_failure(recovery, action):
    body = 'SELECT RAISE(IGNORE);' if action == 'ignore' else "SELECT RAISE(ABORT,'late recovery failure');"
    with DatabaseUnitOfWork(recovery.database) as uow:
        uow.execute("CREATE TRIGGER refuse_recovery BEFORE UPDATE ON user_xiuxian WHEN NEW.user_id='second' "
                    "BEGIN INSERT INTO isolated_effect VALUES('effect'); " + body + ' END')
    with patch.dict('os.environ', {'XIUXIAN_STAMINA_RECOVERY_BATCH_SIZE': '1'}), patch.object(lay_out, 'logger') as logger:
        event = _execute_job(lay_out.limit_all_stamina_)
    assert event.code == EVENT_JOB_ERROR and snapshot(recovery)[1] == []
    assert [row['user_stamina'] for row in snapshot(recovery)[0]] == [3, 1, 2]
    assert logger.opt.call_args.kwargs['exception'] is event.exception
    assert not logger.info.called and not logger.success.called
    if action == 'ignore':
        assert event.exception.details['updated'] == 1 and event.exception.details['subject'] == 'second'
    else:
        assert isinstance(event.exception, sqlite3.IntegrityError) and 'late recovery failure' in str(event.exception)


def test_actual_declaration_pending_retries_interval_boundary_and_live_execution(recovery):
    from nonebot_plugin_xiuxian_2.compatibility.scheduler import DeferredScheduler
    source = lay_out.limit_all_stamina
    declaration = next((job for job in getattr(source, '_pending', ()) if job.job_id == 'recover_user_stamina'), None)
    if declaration is None:
        live = source.get_job('recover_user_stamina')
        assert live is not None and live.func is lay_out.limit_all_stamina_
        arguments = dict(trigger=live.trigger, id=live.id, coalesce=live.coalesce,
                         max_instances=live.max_instances, misfire_grace_time=live.misfire_grace_time)
    else:
        assert declaration.function is lay_out.limit_all_stamina_
        arguments = dict(declaration.kwargs, trigger=declaration.args[0])
    async def run():
        scheduler = AsyncIOScheduler(timezone='Asia/Shanghai')
        bridge = DeferredScheduler(scheduler)
        bridge.add_job(lay_out.limit_all_stamina_, **arguments)
        failure = RuntimeError('isolated stamina registration failure')
        with patch.object(scheduler, 'add_job', side_effect=failure):
            for _ in range(2):
                with pytest.raises(RuntimeError) as caught:
                    bridge.activate()
                assert caught.value is failure and not bridge.active
                assert bridge.pending_ids == ('recover_user_stamina',)
        bridge.activate()
        bridge.activate()
        assert bridge.active and not bridge.pending_ids and len(scheduler.get_jobs()) == 1
        job = scheduler.get_job('recover_user_stamina')
        assert job.coalesce and job.max_instances == 1 and job.misfire_grace_time == 30
        assert job.trigger.interval == timedelta(minutes=1)
        cutoff = job.trigger.start_date + timedelta(minutes=1)
        assert job.trigger.get_next_fire_time(None, cutoff-timedelta(microseconds=1)) == cutoff
        assert job.trigger.get_next_fire_time(None, cutoff) == cutoff
        assert job.trigger.get_next_fire_time(None, cutoff+timedelta(microseconds=1)) == cutoff+timedelta(minutes=1)
        assert job.trigger.get_next_fire_time(None, cutoff.astimezone(timezone.utc)) == cutoff
        shanghai_midnight = datetime(2026, 11, 18, tzinfo=ZoneInfo('Asia/Shanghai'))
        assert job.trigger.get_next_fire_time(None, shanghai_midnight) == job.trigger.get_next_fire_time(None, shanghai_midnight.astimezone(timezone.utc))
        result = asyncio.get_running_loop().create_future()
        scheduler.add_listener(result.set_result, EVENT_JOB_ERROR | EVENT_JOB_EXECUTED)
        scheduler.modify_job(job.id, next_run_time=datetime.now(timezone.utc))
        scheduler.start()
        try:
            assert (await asyncio.wait_for(result, timeout=3)).code == EVENT_JOB_EXECUTED
            assert [row['user_stamina'] for row in snapshot(recovery)[0]] == [3, 4, 5]
        finally:
            scheduler.shutdown(wait=False)
            await asyncio.sleep(0)
    asyncio.run(run())


def test_unknown_configuration_type_and_zero_cost_retain_existing_semantics(recovery):
    recovery.config.stamina_recovery_points = object()
    before = snapshot(recovery)
    event = _execute_job(lay_out.limit_all_stamina_)
    assert event.code == EVENT_JOB_ERROR
    assert event.exception.details['reason'] == 'stamina_recovery_config_invalid'
    assert event.exception.details['subject'] == 'stamina_recovery_points'
    assert snapshot(recovery) == before
    recovery.config.stamina_recovery_points = 0
    assert _execute_job(lay_out.limit_all_stamina_).code == EVENT_JOB_EXECUTED
    assert snapshot(recovery) == before


def test_default_batch_recheck_handles_more_than_one_sql_bind_chunk(recovery):
    with DatabaseUnitOfWork(recovery.database) as uow:
        uow.execute('DELETE FROM user_xiuxian')
        for index in range(503):
            uow.execute('INSERT INTO user_xiuxian VALUES(?,0)', (str(index),))
        uow.execute("CREATE TRIGGER refuse_recovery AFTER UPDATE ON user_xiuxian WHEN NEW.user_id='502' "
                    "BEGIN UPDATE user_xiuxian SET user_stamina=0 WHERE user_id='501'; END")
    before = snapshot(recovery)
    result = recovery.app.recover(10, 3)
    assert result['reason'] == 'stamina_recovery_write_rejected' and result['subject'] == '501'
    assert result['updated'] == 0 and snapshot(recovery) == before
    with DatabaseUnitOfWork(recovery.database) as uow:
        uow.execute('DROP TRIGGER refuse_recovery')
    assert recovery.app.recover(10, 3) == {'status': 'applied', 'updated': 503}


@pytest.mark.parametrize('body', ["DELETE FROM user_xiuxian WHERE user_id='first';",
                                  "UPDATE user_xiuxian SET user_id='changed' WHERE user_id='first';"])
def test_late_target_deletion_or_identity_rewrite_rolls_back_whole_batch(recovery, body):
    with DatabaseUnitOfWork(recovery.database) as uow:
        uow.execute("CREATE TRIGGER refuse_recovery AFTER UPDATE ON user_xiuxian WHEN NEW.user_id='third' BEGIN " + body + ' END')
    before = snapshot(recovery)
    result = recovery.app.recover(10, 3)
    assert result['reason'] == 'stamina_recovery_write_rejected' and result['subject'] == 'first'
    assert snapshot(recovery) == before
