"""Daily reset must preserve both its frozen targets and committed chunks."""
import asyncio
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch
from zoneinfo import ZoneInfo

import nonebot
import pytest
from apscheduler.events import EVENT_JOB_ERROR, EVENT_JOB_EXECUTED
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from nonebot.exception import FinishedException, StopPropagation

nonebot.init()

from nonebot_plugin_xiuxian_2.features.boss.application import BossApplication
from nonebot_plugin_xiuxian_2.features.boss.migrations import apply_boss_player_schema
from nonebot_plugin_xiuxian_2.features.boss.world_boss_repository import WorldBossDailyLimitResetSqlRepository
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork
from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_boss as boss
from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_scheduler as scheduled
from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_admin as admin
from tests.test_world_boss_refresh_recovery import fire


DAY = '2026-11-18'
OPERATIONS = 'world_boss_daily_limit_reset_operations'
TARGETS = 'world_boss_daily_limit_reset_targets'


@pytest.fixture
def state(tmp_path):
    database = tmp_path / 'player.db'
    with DatabaseUnitOfWork(database) as uow:
        apply_boss_player_schema(uow)
        uow.executemany('INSERT INTO boss VALUES(?,?,?,?)', [('u1', 12, 30, 2), ('u2', 8, 20, 1)])
        uow.execute('CREATE TABLE reset_probe(value INTEGER)')
        uow.execute('INSERT INTO reset_probe VALUES(7)')
    clock = SimpleNamespace(moment=datetime(2026, 11, 17, 16, 1, 40, tzinfo=timezone.utc))
    clock.now = lambda: clock.moment
    repo = WorldBossDailyLimitResetSqlRepository(database, clock=clock)
    app = BossApplication(tmp_path / 'game.db', database, clock=clock, world_boss_repository=repo)
    with patch.object(boss, 'runtime_clock', clock), patch.object(boss, 'boss_application', app):
        yield SimpleNamespace(database=database, repo=repo, app=app, clock=clock)


def rows(state):
    with DatabaseUnitOfWork(state.database, read_only=True) as uow:
        return tuple(uow.query_all(f'SELECT * FROM {table} ORDER BY rowid')
                     for table in ('boss', OPERATIONS, TARGETS, 'reset_probe'))


def fault(state, step, action):
    table, event = {
        'operation': (OPERATIONS, 'INSERT'), 'freeze': (TARGETS, 'INSERT'),
        'limit': ('boss', 'UPDATE'), 'target': (TARGETS, 'UPDATE'),
        'progress': (OPERATIONS, 'UPDATE'),
    }[step]
    timing = 'BEFORE' if action == 'ignore' else 'AFTER'
    mutation = 'SELECT RAISE(IGNORE);' if action == 'ignore' else {
        'operation': f'UPDATE {OPERATIONS} SET total=99;',
        'freeze': f"UPDATE {TARGETS} SET status='applied';",
        'limit': "UPDATE boss SET boss_stone=99 WHERE user_id=NEW.user_id;",
        'target': f"UPDATE {TARGETS} SET previous_stone=99 WHERE user_id=NEW.user_id;",
        'progress': f'UPDATE {OPERATIONS} SET completed=99;',
    }[step]
    with DatabaseUnitOfWork(state.database) as uow:
        uow.execute(f'CREATE TRIGGER refuse_reset {timing} {event} ON {table} BEGIN '
                    f'UPDATE reset_probe SET value=0; {mutation} END')


@pytest.mark.parametrize('step', ['operation', 'freeze', 'limit', 'target', 'progress'])
@pytest.mark.parametrize('action', ['ignore', 'rewrite'])
def test_rejected_write_preserves_committed_chunks_and_recovers(state, step, action):
    if step not in {'operation', 'freeze'}:
        assert state.repo.reset(DAY, chunk_size=1).completed == 1
        with DatabaseUnitOfWork(state.database) as uow:
            uow.execute("UPDATE boss SET boss_stone=5 WHERE user_id='u1'")
    before = rows(state)
    fault(state, step, action)
    result = state.repo.reset(DAY, chunk_size=1)
    assert not result.succeeded
    assert result.reason == f'world_boss_reset_{step}_write_rejected'
    assert result.subject == ('u2' if step in {'limit', 'target'} else DAY)
    assert rows(state) == before
    with DatabaseUnitOfWork(state.database) as uow:
        uow.execute('DROP TRIGGER refuse_reset')
    restarted = WorldBossDailyLimitResetSqlRepository(state.database, clock=state.clock)
    assert restarted.reset(DAY).task_status == 'completed'
    saved = rows(state)
    assert restarted.reset(DAY).status == 'duplicate' and rows(state) == saved
    assert saved[0][0]['boss_stone'] == (0 if step in {'operation', 'freeze'} else 5)
    with DatabaseUnitOfWork(state.database, read_only=True) as uow:
        assert uow.query_one('PRAGMA integrity_check') == {'integrity_check': 'ok'}


def test_registered_callback_uses_business_date_and_replays_same_day(state):
    outcome = asyncio.run(fire(scheduled.daily_reset_boss_limits))
    assert outcome.code == EVENT_JOB_EXECUTED
    assert rows(state)[1][0]['business_date'] == DAY
    with DatabaseUnitOfWork(state.database) as uow:
        uow.execute("UPDATE boss SET boss_stone=5 WHERE user_id='u1'")
    saved = rows(state)
    assert asyncio.run(fire(scheduled.daily_reset_boss_limits)).code == EVENT_JOB_EXECUTED
    assert rows(state) == saved


def test_default_repository_date_uses_injected_clock(state):
    assert state.repo.reset().business_date == DAY


def test_actual_job_refusal_is_error_with_original_reason_then_recovers(state):
    fault(state, 'target', 'ignore')
    outcome = asyncio.run(fire(scheduled.daily_reset_boss_limits))
    assert outcome.code == EVENT_JOB_ERROR
    assert 'world_boss_reset_target_write_rejected' in str(outcome.exception)
    assert 'u1' in str(outcome.exception)
    assert all(row['status'] == 'pending' for row in rows(state)[2])
    with DatabaseUnitOfWork(state.database) as uow:
        uow.execute('DROP TRIGGER refuse_reset')
    assert asyncio.run(fire(scheduled.daily_reset_boss_limits)).code == EVENT_JOB_EXECUTED
    assert rows(state)[1][0]['status'] == 'completed'


@pytest.mark.parametrize('target', ['limit', 'target'])
def test_final_progress_trigger_cannot_rewrite_earlier_steps(state, target):
    assert state.repo.reset(DAY, chunk_size=1).completed == 1
    before = rows(state)
    mutation = ("UPDATE boss SET boss_integral=99 WHERE user_id='u2';" if target == 'limit' else
                f"UPDATE {TARGETS} SET status='pending' WHERE user_id='u2';")
    with DatabaseUnitOfWork(state.database) as uow:
        uow.execute(f'CREATE TRIGGER refuse_reset AFTER UPDATE ON {OPERATIONS} BEGIN {mutation} END')
    result = state.repo.reset(DAY)
    assert result.reason == f'world_boss_reset_{target}_write_rejected' and result.subject == 'u2'
    assert result.completed == 1 and rows(state) == before


def test_deleted_target_is_skipped_only_after_durable_receipt_and_new_player_is_not_added(state):
    assert state.repo.reset(DAY, chunk_size=1).completed == 1
    with DatabaseUnitOfWork(state.database) as uow:
        uow.execute("DELETE FROM boss WHERE user_id='u2'")
        uow.execute("INSERT INTO boss VALUES('u3',6,8,2)")
    before = rows(state)
    fault(state, 'target', 'ignore')
    result = state.repo.reset(DAY)
    assert result.reason == 'world_boss_reset_target_write_rejected' and rows(state) == before
    with DatabaseUnitOfWork(state.database) as uow:
        uow.execute('DROP TRIGGER refuse_reset')
    result = state.repo.reset(DAY)
    assert (result.total, result.completed, result.changed, result.skipped) == (2, 2, 1, 1)
    assert state.app.daily_limit_snapshot('u3').stone == 8


@pytest.mark.parametrize('mutation', [f"DELETE FROM {TARGETS} WHERE user_id='u2'",
                                     f"UPDATE {TARGETS} SET status='invalid' WHERE user_id='u2'",
                                     f'UPDATE {OPERATIONS} SET completed=99'])
def test_corrupt_running_progress_fails_without_guessing_or_overwriting(state, mutation):
    state.repo.reset(DAY, chunk_size=1)
    with DatabaseUnitOfWork(state.database) as uow:
        uow.execute(mutation)
    before = rows(state)
    result = state.repo.reset(DAY)
    assert result.reason == 'world_boss_reset_progress_invalid'
    assert result.subject == DAY and rows(state) == before


def test_abort_rolls_back_current_chunk_and_scheduler_keeps_original_exception(state):
    state.repo.reset(DAY, chunk_size=1)
    before = rows(state)
    with DatabaseUnitOfWork(state.database) as uow:
        uow.execute(f"CREATE TRIGGER refuse_reset BEFORE UPDATE ON {TARGETS} "
                    "BEGIN UPDATE reset_probe SET value=0; SELECT RAISE(ABORT,'isolated reset abort'); END")
    outcome = asyncio.run(fire(scheduled.daily_reset_boss_limits))
    assert outcome.code == EVENT_JOB_ERROR and 'isolated reset abort' in str(outcome.exception)
    assert rows(state) == before
    with DatabaseUnitOfWork(state.database) as uow:
        uow.execute('DROP TRIGGER refuse_reset')
    assert asyncio.run(fire(scheduled.daily_reset_boss_limits)).code == EVENT_JOB_EXECUTED


def test_empty_and_retained_completed_receipts_are_still_idempotent(state):
    with DatabaseUnitOfWork(state.database) as uow:
        uow.execute('DELETE FROM boss')
    first = state.repo.reset(DAY)
    assert first.succeeded and (first.total, first.completed, first.task_status) == (0, 0, 'completed')
    assert state.repo.reset(DAY).status == 'duplicate'
    with DatabaseUnitOfWork(state.database) as uow:
        uow.execute("INSERT INTO boss VALUES('new',2,5,1)")
        uow.execute(f'UPDATE {OPERATIONS} SET total=10,completed=10,changed=7,skipped=2')
    before = rows(state)
    result = state.repo.reset(DAY)
    assert result.status == 'duplicate' and result.total == 10 and rows(state) == before


@pytest.mark.parametrize('value,reason', [('bad', 'boss_stone_invalid_numeric_value'),
                                        (1.5, 'boss_stone_invalid_numeric_value'),
                                        (float(2**54), 'boss_stone_precision_lost_real')])
def test_invalid_limit_preserves_reason_and_current_chunk(state, value, reason):
    state.repo.reset(DAY, chunk_size=1)
    with DatabaseUnitOfWork(state.database) as uow:
        if isinstance(value, float):
            uow.execute('ALTER TABLE boss RENAME TO old_boss')
            uow.execute('CREATE TABLE boss(user_id TEXT PRIMARY KEY,boss_integral,boss_stone,boss_battle_count)')
            uow.execute('INSERT INTO boss SELECT * FROM old_boss')
        uow.execute("UPDATE boss SET boss_stone=? WHERE user_id='u2'", (value,))
    before = rows(state)
    result = state.repo.reset(DAY)
    assert (result.reason, result.subject) == (reason, 'u2') and rows(state) == before


def test_clock_boundaries_and_explicit_date_and_configured_timezone(state):
    state.clock.moment = datetime(2026, 11, 17, 15, 59, 59, 999999, tzinfo=timezone.utc)
    first = asyncio.run(boss.set_boss_limits_reset())
    assert first.business_date == '2026-11-17'
    state.clock.moment += timedelta(microseconds=1)
    assert asyncio.run(boss.set_boss_limits_reset()).business_date == DAY
    before = rows(state)
    state.clock.moment = state.clock.moment.astimezone(ZoneInfo('Asia/Shanghai'))
    assert asyncio.run(boss.set_boss_limits_reset()).status == 'duplicate'
    assert rows(state) == before
    with patch.object(boss, 'scheduler', SimpleNamespace(timezone=timezone.utc)):
        assert asyncio.run(boss.set_boss_limits_reset()).business_date == '2026-11-17'
    assert asyncio.run(boss.set_boss_limits_reset('2026-11-19')).business_date == '2026-11-19'


def test_live_registration_pending_retry_and_cron_boundary_then_actual_execution(state):
    from nonebot_plugin_xiuxian_2.compatibility.scheduler import DeferredScheduler
    source = scheduled.scheduler
    declaration = next((job for job in getattr(source, '_pending', ()) if job.job_id == 'daily_reset_boss_limits'), None)
    if declaration is None:
        live = source.get_job('daily_reset_boss_limits')
        assert live is not None and live.func is scheduled.daily_reset_boss_limits
        arguments = dict(trigger=live.trigger, id=live.id, coalesce=live.coalesce,
                         max_instances=live.max_instances, misfire_grace_time=live.misfire_grace_time)
    else:
        assert declaration.function is scheduled.daily_reset_boss_limits
        arguments = dict(declaration.kwargs, trigger=declaration.args[0])

    async def run():
        scheduler = AsyncIOScheduler(timezone='Asia/Shanghai')
        bridge = DeferredScheduler(scheduler)
        bridge.add_job(scheduled.daily_reset_boss_limits, **arguments)
        error = RuntimeError('isolated daily registration failure')
        with patch.object(scheduler, 'add_job', side_effect=error):
            for _ in range(2):
                with pytest.raises(RuntimeError) as caught:
                    bridge.activate()
                assert caught.value is error and not bridge.active
                assert bridge.pending_ids == ('daily_reset_boss_limits',)
        bridge.activate()
        job = scheduler.get_job('daily_reset_boss_limits')
        assert job.coalesce and job.max_instances == 1 and job.misfire_grace_time == 300
        cutoff = state.clock.moment
        assert job.trigger.get_next_fire_time(None, cutoff-timedelta(microseconds=1)) == cutoff
        assert job.trigger.get_next_fire_time(None, cutoff) == cutoff
        assert job.trigger.get_next_fire_time(None, cutoff+timedelta(microseconds=1)) == cutoff+timedelta(days=1)
        outcome = asyncio.get_running_loop().create_future()
        scheduler.add_listener(outcome.set_result, EVENT_JOB_ERROR | EVENT_JOB_EXECUTED)
        scheduler.modify_job(job.id, next_run_time=datetime.now(timezone.utc))
        scheduler.start()
        try:
            assert (await asyncio.wait_for(outcome, timeout=3)).code == EVENT_JOB_EXECUTED
            assert rows(state)[1][0]['business_date'] == DAY
        finally:
            scheduler.shutdown(wait=False)
            await asyncio.sleep(0)
    asyncio.run(run())


@pytest.mark.parametrize('step', ['operation', 'freeze', 'limit', 'target', 'progress'])
def test_legacy_facade_uses_same_rejection_and_recovery_contract(state, step):
    from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_boss.transaction_service import WorldBossDailyLimitResetService
    service = WorldBossDailyLimitResetService(state.database, clock=state.clock)
    if step not in {'operation', 'freeze'}:
        assert service.reset(DAY, chunk_size=1).completed == 1
    fault(state, step, 'ignore')
    before = rows(state)
    result = service.reset(DAY)
    assert result.reason == f'world_boss_reset_{step}_write_rejected' and rows(state) == before
    with DatabaseUnitOfWork(state.database) as uow:
        uow.execute('DROP TRIGGER refuse_reset')
    assert service.reset(DAY).task_status == 'completed'
    assert service.reset(DAY).status == 'duplicate'


@pytest.mark.parametrize('reason', ['schema_missing', 'world_boss_reset_operation_write_rejected',
                                  'world_boss_reset_freeze_write_rejected', 'world_boss_reset_limit_write_rejected',
                                  'world_boss_reset_target_write_rejected', 'world_boss_reset_progress_write_rejected',
                                  'world_boss_reset_progress_invalid', 'boss_stone_invalid_numeric_value',
                                  'previous_stone_precision_lost_real'])
@pytest.mark.parametrize('onebot,markdown,send_fails', [(True, True, False), (False, False, False),
                                                     (False, True, False), (False, True, True)])
def test_known_failure_survives_admin_reply_and_adapter_fallback(state, reason, onebot, markdown, send_fails):
    from nonebot_plugin_xiuxian_2.features.boss.world_boss_repository import WorldBossDailyLimitResetResult
    from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils import utils
    result = WorldBossDailyLimitResetResult('state_changed', DAY, task_status='running', total=2, completed=1,
                                          reason=reason, subject='subject-test')
    args = SimpleNamespace(extract_plain_text=lambda: '')
    event = SimpleNamespace(get_user_id=lambda: 'admin')
    captured = AsyncMock()
    with patch.object(boss, 'set_boss_limits_reset', new=AsyncMock(return_value=result)), \
            patch.object(admin, 'handle_send', captured):
        with pytest.raises(FinishedException):
            asyncio.run(admin.boss_reset_(object(), event, args))
    reply = captured.call_args.args[2]
    assert reason in reply and '主体：subject-test' in reply and '重置完成' not in reply
    bot = SimpleNamespace(adapter=SimpleNamespace(get_name=lambda: 'OneBot V11' if onebot else 'QQ'))
    config = SimpleNamespace(markdown_status=markdown, markdown_id='', markdown_id2='', markdown_button_status=False,
                             at_sender=False, message_optimization=False, img=False)
    plain, md = AsyncMock(), AsyncMock(side_effect=RuntimeError('isolated delivery failure') if send_fails else None)
    with patch.object(utils, 'XiuConfig', return_value=config), patch.object(utils, 'get_chat_scene', return_value='group'), \
            patch.object(utils, 'is_group_event', return_value=True), \
            patch.object(utils, 'get_real_id_async', new=AsyncMock(return_value='')), \
            patch.object(utils, '_send_event_message', plain), patch.object(utils, '_send_markdown_or_keyboard', md):
        asyncio.run(utils.handle_send(bot, event, reply))
    if onebot or not markdown or send_fails:
        plain.assert_awaited_once()
        assert plain.call_args.kwargs['message'] == reply
    else:
        assert reason in md.call_args.args[2] and 'subject-test' in md.call_args.args[2]


def test_unknown_admin_fault_normal_flow_and_failed_reply_propagate(state):
    event = SimpleNamespace(get_user_id=lambda: 'admin')
    args = SimpleNamespace(extract_plain_text=lambda: '')
    fault, delivery = RuntimeError('isolated reset fault'), RuntimeError('isolated reply failure')
    reset, sent = AsyncMock(side_effect=fault), AsyncMock()
    with patch.object(boss, 'set_boss_limits_reset', reset), patch.object(admin, 'handle_send', sent), \
            patch.object(admin, 'report_exception', return_value='定位号：reset-test') as report:
        with pytest.raises(FinishedException):
            asyncio.run(admin.boss_reset_(object(), event, args))
        assert report.call_args.args[1] is fault and '定位号：reset-test' in sent.call_args.args[2]
        sent.side_effect = delivery
        with pytest.raises(RuntimeError) as caught:
            asyncio.run(admin.boss_reset_(object(), event, args))
        assert caught.value is delivery
        for flow in (FinishedException(), StopPropagation()):
            reset.side_effect = flow
            report.reset_mock()
            sent.reset_mock()
            with pytest.raises(type(flow)) as caught:
                asyncio.run(admin.boss_reset_(object(), event, args))
            assert caught.value is flow
            report.assert_not_called()
            sent.assert_not_called()


def test_later_player_rejection_rolls_back_whole_chunk_but_keeps_previous_chunk(state):
    state.repo.reset(DAY, chunk_size=1)
    with DatabaseUnitOfWork(state.database) as uow:
        uow.execute("UPDATE boss SET boss_stone=5 WHERE user_id='u1'")
        uow.execute("INSERT INTO boss VALUES('u3',5,9,1)")
        # The next date freezes all three; finish u1 before refusing the u2/u3 chunk.
    next_day = '2026-11-19'
    assert state.repo.reset(next_day, chunk_size=1).completed == 1
    with DatabaseUnitOfWork(state.database) as uow:
        uow.execute("UPDATE boss SET boss_stone=7 WHERE user_id='u1'")
        uow.execute(f"CREATE TRIGGER refuse_reset BEFORE UPDATE ON {TARGETS} WHEN NEW.user_id='u3' "
                    "BEGIN UPDATE reset_probe SET value=0; SELECT RAISE(IGNORE); END")
    before = rows(state)
    result = state.repo.reset(next_day)
    assert (result.reason, result.subject, result.completed) == ('world_boss_reset_target_write_rejected', 'u3', 1)
    assert rows(state) == before
    with DatabaseUnitOfWork(state.database) as uow:
        uow.execute('DROP TRIGGER refuse_reset')
    result = state.repo.reset(next_day)
    assert (result.total, result.completed, result.changed, result.task_status) == (3, 3, 3, 'completed')
    assert state.app.daily_limit_snapshot('u1').stone == 7


def test_missing_schema_is_visible_to_task_and_admin_then_restored(state):
    with DatabaseUnitOfWork(state.database) as uow:
        uow.execute(f'DROP TABLE {TARGETS}')
    with DatabaseUnitOfWork(state.database, read_only=True) as uow:
        before = uow.query_all('SELECT * FROM boss ORDER BY user_id')
    outcome = asyncio.run(fire(scheduled.daily_reset_boss_limits))
    assert outcome.code == EVENT_JOB_ERROR and 'schema_missing' in str(outcome.exception) and DAY in str(outcome.exception)
    sent = AsyncMock()
    with patch.object(admin, 'handle_send', sent):
        with pytest.raises(FinishedException):
            asyncio.run(admin.boss_reset_(object(), object(), SimpleNamespace(extract_plain_text=lambda: '')))
    assert 'schema_missing' in sent.call_args.args[2] and DAY in sent.call_args.args[2]
    with DatabaseUnitOfWork(state.database) as uow:
        assert uow.query_all('SELECT * FROM boss ORDER BY user_id') == before
        assert not uow.query_all(f'SELECT * FROM {OPERATIONS}')
        apply_boss_player_schema(uow)
    assert asyncio.run(fire(scheduled.daily_reset_boss_limits)).code == EVENT_JOB_EXECUTED


def test_running_rejection_stops_loop_after_one_attempt_without_completion_log(state):
    from nonebot_plugin_xiuxian_2.features.boss.world_boss_repository import WorldBossDailyLimitResetResult
    rejected = WorldBossDailyLimitResetResult('state_changed', DAY, task_status='running', total=2, completed=1,
                                            reason='world_boss_reset_target_write_rejected', subject='u2')
    reset, logger = Mock(return_value=rejected), Mock()
    with patch.object(state.app, 'reset_daily_limit', reset), patch.object(boss, 'logger', logger):
        assert asyncio.run(boss.set_boss_limits_reset()) is rejected
    reset.assert_called_once()
    logger.error.assert_called_once()
    logger.opt.assert_not_called()


def test_bad_previous_value_preserves_numeric_reason_and_subject(state):
    state.repo.reset(DAY, chunk_size=1)
    with DatabaseUnitOfWork(state.database) as uow:
        uow.execute(f"UPDATE {TARGETS} SET previous_stone='bad' WHERE user_id='u1'")
    before = rows(state)
    result = state.repo.reset(DAY)
    assert (result.reason, result.subject) == ('previous_stone_invalid_numeric_value', 'u1')
    assert rows(state) == before
