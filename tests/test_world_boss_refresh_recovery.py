"""Full refresh must commit both state and receipt before publishing its cache."""
import asyncio
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

import nonebot
import pytest
from apscheduler.events import EVENT_JOB_ERROR, EVENT_JOB_EXECUTED
from apscheduler.schedulers.asyncio import AsyncIOScheduler

nonebot.init()

from nonebot_plugin_xiuxian_2.bootstrap import legacy
from nonebot_plugin_xiuxian_2.compatibility.scheduler import DeferredScheduler
from nonebot_plugin_xiuxian_2.features.boss.application import BossApplication
from nonebot_plugin_xiuxian_2.features.boss.migrations import apply_boss_player_schema
from nonebot_plugin_xiuxian_2.features.boss.world_boss_repository import WorldBossFullRefreshSqlRepository
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork
from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_boss as boss


CONFIG = {"Boss名字": ["refresh-boss"], "Boss灵石": {"练气境": [100]}, "Boss倍率": {}}
BOSS = {"name": "refresh-boss", "jj": "练气境", "气血": 1000, "总血量": 1000,
        "真元": 100, "攻击": 10, "max_stone": 100, "stone": 100}


@pytest.fixture
def state(tmp_path):
    database = tmp_path / 'player.db'
    with DatabaseUnitOfWork(database) as uow:
        apply_boss_player_schema(uow)
        uow.execute("INSERT INTO world_boss_state VALUES('global','[]','old',0)")
        uow.execute("INSERT INTO boss VALUES('side-effect',7,9,2)")
    repo = WorldBossFullRefreshSqlRepository(database, lambda: CONFIG)
    app = BossApplication(tmp_path / 'game.db', database, full_refresh_config_loader=lambda: CONFIG)
    moment = datetime(2026, 11, 17, 10, tzinfo=timezone.utc)
    clock = SimpleNamespace(moment=moment)
    clock.now = lambda: clock.moment
    config = dict(CONFIG, Boss生成时间参数={'hours': 1, 'minutes': 0})
    with patch.object(boss, 'boss_application', app), \
            patch.object(boss, '_world_boss_full_refresh_service_instance', None), \
            patch.object(boss, 'runtime_clock', clock), patch.object(boss, 'config', config), \
            patch.object(boss, 'get_boss_config', return_value=CONFIG), \
            patch.object(boss, 'create_all_bosses', return_value=[deepcopy(BOSS)]), \
            patch.object(boss, 'group_boss', {'global': []}), \
            patch.object(boss.old_boss_info, 'data', {'global': []}), patch.object(boss, 'groups', []):
        yield SimpleNamespace(database=database, repo=repo, app=app, clock=clock, config=config)


def refresh(repo, **changes):
    values = dict(operation_id='refresh-test', trigger='scheduled', expected_revision=0,
                  expected_bosses=[], expected_config=repo.config_snapshot(CONFIG, ['练气境']), bosses=[BOSS])
    values.update(changes)
    return repo.refresh(**values)


def rows(state):
    with DatabaseUnitOfWork(state.database, read_only=True) as uow:
        return tuple(uow.query_all(f'SELECT * FROM {table}') for table in
                     ('world_boss_state', 'world_boss_full_refresh_operations', 'boss'))


async def fire(callback):
    scheduler = AsyncIOScheduler(timezone='Asia/Shanghai')
    event = asyncio.get_running_loop().create_future()
    scheduler.add_listener(lambda result: event.set_result(result), EVENT_JOB_ERROR | EVENT_JOB_EXECUTED)
    scheduler.add_job(callback, 'date')
    scheduler.start()
    try:
        return await asyncio.wait_for(event, timeout=3)
    finally:
        scheduler.shutdown(wait=False)
        await asyncio.sleep(0)


@pytest.mark.parametrize('target,action', [('state', 'ignore'), ('state', 'rewrite'),
                                          ('receipt', 'ignore'), ('receipt', 'rewrite'),
                                          ('state', 'late_rewrite')])
def test_refused_write_rolls_back_every_step_and_retries(state, target, action):
    table = 'world_boss_state' if target == 'state' and action != 'late_rewrite' else 'world_boss_full_refresh_operations'
    event = 'UPDATE' if table == 'world_boss_state' else 'INSERT'
    timing = 'BEFORE' if action == 'ignore' else 'AFTER'
    mutation = "SELECT RAISE(IGNORE);" if action == 'ignore' else (
        "UPDATE world_boss_state SET revision=77 WHERE state_key='global';" if target == 'state'
        else "UPDATE world_boss_full_refresh_operations SET payload='rewritten';")
    with DatabaseUnitOfWork(state.database) as uow:
        uow.execute(f'CREATE TRIGGER refuse_refresh {timing} {event} ON {table} BEGIN '
                    f'UPDATE boss SET boss_integral=0; {mutation} END')
    before = rows(state)
    result = refresh(state.repo)
    assert not result.succeeded
    assert result.reason == f'world_boss_refresh_{target}_write_rejected'
    assert result.subject == ('global' if target == 'state' else 'refresh-test')
    assert rows(state) == before
    with DatabaseUnitOfWork(state.database) as uow:
        uow.execute('DROP TRIGGER refuse_refresh')
    restarted = WorldBossFullRefreshSqlRepository(state.database, lambda: CONFIG)
    assert refresh(restarted).status == 'refreshed'
    after = rows(state)
    assert refresh(restarted).status == 'duplicate'
    assert rows(state) == after
    with DatabaseUnitOfWork(state.database, read_only=True) as uow:
        assert uow.query_one('PRAGMA integrity_check') == {'integrity_check': 'ok'}


def test_startup_registration_fault_is_visible_and_retryable(state):
    async def run():
        scheduler = AsyncIOScheduler(timezone='Asia/Shanghai')
        bridge = DeferredScheduler(scheduler)
        bridge.activate()
        fault = RuntimeError('isolated refresh registration fault')
        add_job = scheduler.add_job
        with patch.object(boss, 'scheduler', bridge), patch.object(legacy, '_startup', [boss.set_boss_generation]), \
                patch.object(legacy, '_started', False), patch.object(scheduler, 'add_job', side_effect=fault) as add:
            for _ in range(2):
                with pytest.raises(RuntimeError) as caught:
                    await legacy.run_legacy_startup()
                assert caught.value is fault and not legacy._started
                assert scheduler.get_job('generate_all_bosses') is None
            add.side_effect = add_job
            await legacy.run_legacy_startup()
            assert legacy._started
            job = scheduler.get_job('generate_all_bosses')
            assert job.func is boss.generate_all_bosses_task
            assert job.trigger.interval == timedelta(hours=1)
            assert job.coalesce and job.max_instances == 1 and job.misfire_grace_time == 60
            outcome = asyncio.get_running_loop().create_future()
            scheduler.add_listener(lambda event: outcome.set_result(event), EVENT_JOB_ERROR | EVENT_JOB_EXECUTED)
            scheduler.modify_job(job.id, next_run_time=datetime.now(timezone.utc))
            scheduler.start()
            try:
                assert (await asyncio.wait_for(outcome, timeout=3)).code == EVENT_JOB_EXECUTED
                assert state.repo.snapshot() == ([BOSS], 1)
            finally:
                scheduler.shutdown(wait=False)
                await asyncio.sleep(0)
    asyncio.run(run())


def test_deferred_registration_preserves_failed_world_boss_job_until_recovery(state):
    scheduler = AsyncIOScheduler(timezone='Asia/Shanghai')
    bridge = DeferredScheduler(scheduler)
    with patch.object(boss, 'scheduler', bridge):
        asyncio.run(boss.set_boss_generation())
    fault = RuntimeError('isolated deferred refresh fault')
    with patch.object(scheduler, 'add_job', side_effect=fault):
        for _ in range(2):
            with pytest.raises(RuntimeError) as caught:
                bridge.activate()
            assert caught.value is fault
            assert bridge.pending_ids == ('generate_all_bosses',) and not bridge.active
    bridge.activate()
    assert bridge.active and not bridge.pending_ids
    assert scheduler.get_job('generate_all_bosses').func is boss.generate_all_bosses_task


@pytest.mark.parametrize('target', ['state', 'receipt', 'schema'])
def test_actual_scheduler_refusal_then_recovery_publishes_only_committed_state(state, target):
    with DatabaseUnitOfWork(state.database) as uow:
        if target == 'schema':
            uow.execute('DROP TABLE world_boss_full_refresh_operations')
        else:
            table = 'world_boss_state' if target == 'state' else 'world_boss_full_refresh_operations'
            event = 'UPDATE' if target == 'state' else 'INSERT'
            uow.execute(f'CREATE TRIGGER refuse_refresh BEFORE {event} ON {table} BEGIN SELECT RAISE(IGNORE); END')
    outcome = asyncio.run(fire(boss.generate_all_bosses_task))
    assert outcome.code == EVENT_JOB_ERROR
    reason = 'schema_missing' if target == 'schema' else f'world_boss_refresh_{target}_write_rejected'
    assert reason in str(outcome.exception)
    assert boss.group_boss['global'] == boss.old_boss_info.data['global'] == []
    with DatabaseUnitOfWork(state.database) as uow:
        if target == 'schema':
            apply_boss_player_schema(uow)
        else:
            uow.execute('DROP TRIGGER refuse_refresh')
    assert asyncio.run(fire(boss.generate_all_bosses_task)).code == EVENT_JOB_EXECUTED
    assert boss.group_boss['global'] == boss.old_boss_info.data['global'] == [BOSS]
    assert state.repo.snapshot() == ([BOSS], 1)
    saved = rows(state)
    assert asyncio.run(fire(boss.generate_all_bosses_task)).code == EVENT_JOB_EXECUTED
    assert rows(state) == saved


def test_business_clock_slot_boundary_timezone_and_replay(state):
    from zoneinfo import ZoneInfo
    state.clock.moment -= timedelta(microseconds=1)
    assert asyncio.run(fire(boss.generate_all_bosses_task)).code == EVENT_JOB_EXECUTED
    state.clock.moment += timedelta(microseconds=1)
    assert asyncio.run(fire(boss.generate_all_bosses_task)).code == EVENT_JOB_EXECUTED
    assert state.repo.snapshot() == ([BOSS], 2)
    saved = rows(state)
    state.clock.moment = state.clock.moment.astimezone(ZoneInfo('Asia/Shanghai'))
    assert asyncio.run(fire(boss.generate_all_bosses_task)).code == EVENT_JOB_EXECUTED
    assert rows(state) == saved
    with DatabaseUnitOfWork(state.database, read_only=True) as uow:
        ids = [row['operation_id'] for row in uow.query_all('SELECT operation_id FROM world_boss_full_refresh_operations ORDER BY operation_id')]
    slot = int(state.clock.moment.timestamp() // 3600)
    assert ids == [f'world-boss-full-refresh:scheduled:3600:{value}' for value in (slot-1, slot)]


def test_corrupt_persisted_session_is_not_silently_replaced(state):
    with DatabaseUnitOfWork(state.database) as uow:
        uow.execute("UPDATE world_boss_state SET bosses='broken-json'")
    before = rows(state)
    outcome = asyncio.run(fire(boss.generate_all_bosses_task))
    assert outcome.code == EVENT_JOB_ERROR and 'world_boss_state_invalid' in str(outcome.exception)
    assert rows(state) == before and boss.group_boss['global'] == []


@pytest.mark.parametrize('value', ['broken-json', '[]', '{"bosses":[1],"revision":1,"trigger":"scheduled"}'])
def test_corrupt_receipt_is_not_replayed_as_success_or_overwritten(state, value):
    assert asyncio.run(fire(boss.generate_all_bosses_task)).code == EVENT_JOB_EXECUTED
    with DatabaseUnitOfWork(state.database) as uow:
        uow.execute('UPDATE world_boss_full_refresh_operations SET result_json=?', (value,))
    before = rows(state)
    outcome = asyncio.run(fire(boss.generate_all_bosses_task))
    assert outcome.code == EVENT_JOB_ERROR and 'world_boss_refresh_receipt_invalid' in str(outcome.exception)
    assert rows(state) == before


@pytest.mark.parametrize('target', ['state', 'receipt'])
def test_sqlite_abort_keeps_original_error_and_no_partial_writes(state, target):
    table = 'world_boss_state' if target == 'state' else 'world_boss_full_refresh_operations'
    event = 'UPDATE' if target == 'state' else 'INSERT'
    with DatabaseUnitOfWork(state.database) as uow:
        uow.execute(f"CREATE TRIGGER refuse_refresh BEFORE {event} ON {table} "
                    "BEGIN UPDATE boss SET boss_integral=0; SELECT RAISE(ABORT,'isolated refresh abort'); END")
    before = rows(state)
    outcome = asyncio.run(fire(boss.generate_all_bosses_task))
    import sqlite3
    assert outcome.code == EVENT_JOB_ERROR and isinstance(outcome.exception, sqlite3.IntegrityError)
    assert str(outcome.exception) == 'isolated refresh abort'
    assert rows(state) == before and boss.group_boss['global'] == []


def test_initial_state_insert_refusal_and_old_receipt_replay(state):
    with DatabaseUnitOfWork(state.database) as uow:
        uow.execute('DELETE FROM world_boss_state')
        uow.execute("CREATE TRIGGER refuse_refresh BEFORE INSERT ON world_boss_state BEGIN SELECT RAISE(IGNORE); END")
    before = rows(state)
    result = refresh(state.repo)
    assert result.reason == 'world_boss_refresh_state_write_rejected' and rows(state) == before
    with DatabaseUnitOfWork(state.database) as uow:
        uow.execute('DROP TRIGGER refuse_refresh')
    assert refresh(state.repo).succeeded
    legacy_payload = state.repo._json(dict(trigger='scheduled', expected_revision=0, expected_bosses=[],
                                          expected_config=state.repo.config_snapshot(CONFIG, ['练气境']), bosses=[BOSS]))
    with DatabaseUnitOfWork(state.database) as uow:
        uow.execute('UPDATE world_boss_full_refresh_operations SET payload=?', (legacy_payload,))
    before = rows(state)
    assert refresh(state.repo).status == 'duplicate'
    assert rows(state) == before
    assert refresh(state.repo, trigger='manual').reason == 'operation_conflict'


@pytest.mark.parametrize('changes,reason,subject', [
    ({'expected_revision': 99}, 'session_changed', 'global'),
    ({'expected_config': {}}, 'config_changed', 'boss_config'),
    ({'bosses': [{**BOSS, 'name': 'invalid'}]}, 'world_boss_generated_invalid', 'generated_bosses'),
])
def test_known_rejections_keep_reason_and_subject_without_writes(state, changes, reason, subject):
    before = rows(state)
    result = refresh(state.repo, **changes)
    assert not result.succeeded and (result.reason, result.subject) == (reason, subject)
    assert rows(state) == before


@pytest.mark.parametrize('settings', [{'hours': -1, 'minutes': 0}, {'hours': 1.5, 'minutes': 0},
                                      {'hours': True, 'minutes': 0}, {'minutes': 1}, {'hours': 1, 'minutes': 'bad'}])
def test_invalid_schedule_fails_startup_and_callback_with_same_reason(state, settings):
    state.config['Boss生成时间参数'] = settings
    with patch.object(boss, 'scheduler') as scheduler:
        with pytest.raises(Exception, match='world_boss_schedule_invalid'):
            asyncio.run(boss.set_boss_generation())
        scheduler.add_job.assert_not_called()
    outcome = asyncio.run(fire(boss.generate_all_bosses_task))
    assert outcome.code == EVENT_JOB_ERROR and 'world_boss_schedule_invalid' in str(outcome.exception)


def test_disabled_schedule_is_a_normal_skip_and_string_minutes_register(state):
    with patch.object(boss, 'scheduler') as scheduler:
        state.config['Boss生成时间参数'] = {'hours': '0', 'minutes': '0'}
        asyncio.run(boss.set_boss_generation())
        scheduler.add_job.assert_not_called()
        before = rows(state)
        assert asyncio.run(fire(boss.generate_all_bosses_task)).code == EVENT_JOB_EXECUTED
        assert rows(state) == before
        state.config['Boss生成时间参数'] = {'hours': '1', 'minutes': '2'}
        asyncio.run(boss.set_boss_generation())
        assert scheduler.add_job.call_args.kwargs['minutes'] == 62


def test_notify_failure_is_visible_after_commit_and_replay_keeps_damaged_session(state):
    sent = AsyncMock(side_effect=RuntimeError('isolated notice fault'))
    with patch.object(boss, 'groups', ['group-test']), patch.object(boss, 'get_bot', return_value=object()), \
            patch.object(boss.delivery_service, 'send_to_group', sent):
        event = asyncio.run(fire(boss.generate_all_bosses_task))
        assert event.code == EVENT_JOB_ERROR and 'isolated notice fault' in str(event.exception)
        assert state.repo.snapshot() == ([BOSS], 1)
        import json
        damaged = [{**BOSS, '气血': 700}]
        with DatabaseUnitOfWork(state.database) as uow:
            uow.execute('UPDATE world_boss_state SET bosses=?,revision=2', (json.dumps(damaged),))
        before = rows(state)
        assert asyncio.run(fire(boss.generate_all_bosses_task)).code == EVENT_JOB_EXECUTED
        assert rows(state) == before and boss.group_boss['global'] == boss.old_boss_info.data['global'] == damaged
        sent.assert_awaited_once()


def test_unknown_command_fault_and_flow_and_failed_reply_propagate(state):
    from nonebot.exception import FinishedException, StopPropagation
    fault, delivery = RuntimeError('isolated command fault'), RuntimeError('isolated reply fault')
    operation = Mock(side_effect=fault)
    event = SimpleNamespace(message_id='manual-test', get_user_id=lambda: 'admin')
    args = SimpleNamespace(extract_plain_text=lambda: '')
    sent = AsyncMock()
    with patch.object(boss, '_refresh_all_world_bosses', operation), \
            patch.object(boss, 'report_exception', return_value='定位号：boss-test') as report, \
            patch.object(boss.delivery_service, 'reply', sent):
        asyncio.run(boss.generate_all_bosses(object(), event, args))
        assert report.call_args.args[1] is fault and '定位号：boss-test' in sent.call_args.args[2]
        sent.side_effect = delivery
        with pytest.raises(RuntimeError) as caught:
            asyncio.run(boss.generate_all_bosses(object(), event, args))
        assert caught.value is delivery
        for flow in (FinishedException(), StopPropagation()):
            operation.side_effect = flow
            report.reset_mock()
            sent.reset_mock()
            with pytest.raises(type(flow)) as caught:
                asyncio.run(boss.generate_all_bosses(object(), event, args))
            assert caught.value is flow
            report.assert_not_called()
            sent.assert_not_called()


@pytest.mark.parametrize('onebot', [True, False])
@pytest.mark.parametrize('reason', ['schema_missing', 'operation_conflict', 'config_changed', 'session_changed',
                                  'world_boss_generated_invalid', 'world_boss_state_invalid',
                                  'world_boss_refresh_receipt_invalid', 'world_boss_refresh_state_write_rejected',
                                  'world_boss_refresh_receipt_write_rejected'])
def test_manual_failure_reason_and_subject_survive_real_plain_delivery(state, onebot, reason):
    from nonebot_plugin_xiuxian_2.features.boss.world_boss_repository import WorldBossFullRefreshResult
    from nonebot_plugin_xiuxian_2.xiuxian.messaging.delivery import MessageDeliveryService
    bot = SimpleNamespace(adapter=SimpleNamespace(get_name=lambda: 'OneBot V11' if onebot else 'QQ'))
    event = SimpleNamespace(message_id='manual-test', get_user_id=lambda: 'admin')
    args = SimpleNamespace(extract_plain_text=lambda: '')
    result = WorldBossFullRefreshResult('state_changed', reason=reason, subject='subject-test')
    sent = AsyncMock(return_value={'id': 'sent-test'})
    path = 'nonebot_plugin_xiuxian_2.xiuxian.messaging.delivery.'
    with patch.object(boss, '_refresh_all_world_bosses', return_value=result), \
            patch.object(boss, 'delivery_service', MessageDeliveryService()), \
            patch(path+'get_chat_scene', return_value='group'), patch(path+'get_group_id', return_value='group-test'), \
            patch(path+'get_message_reference_id', return_value=None), patch(path+'send_group_message', sent):
        asyncio.run(boss.generate_all_bosses(bot, event, args))
    reply = sent.call_args.kwargs['message']
    assert reason in reply and '主体：subject-test' in reply
    assert sent.call_args.kwargs['source_message_id'] == 'manual-test'
