"""Manual spawn must hand off the same durable session as scheduled refresh."""
import asyncio
from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch
import json

import pytest
from nonebot.exception import FinishedException, StopPropagation
from apscheduler.events import EVENT_JOB_EXECUTED

from tests.test_world_boss_refresh_recovery import BOSS, CONFIG, state, fire
from nonebot_plugin_xiuxian_2.features.boss.world_boss_repository import (
    WorldBossManualSpawnResult, WorldBossManualSpawnSqlRepository,
)
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork
from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_boss as boss


def spawn(repo, **changes):
    values = dict(operation_id='spawn-test', expected_revision=0, expected_bosses=[],
                  expected_config=repo.config_snapshot(CONFIG, '练气境'), boss=BOSS)
    values.update(changes)
    return repo.spawn(**values)


def snapshot(state):
    with DatabaseUnitOfWork(state.database, read_only=True) as uow:
        return tuple(uow.query_all(f'SELECT * FROM {table}') for table in
                     ('world_boss_state', 'world_boss_manual_spawn_operations', 'boss'))


@pytest.mark.parametrize('target,action', [('state', 'ignore'), ('state', 'rewrite'),
                                          ('receipt', 'ignore'), ('receipt', 'rewrite'), ('state', 'late_rewrite')])
def test_rejected_write_rolls_back_state_receipt_and_trigger_side_effects(state, target, action):
    repo = WorldBossManualSpawnSqlRepository(state.database, lambda: CONFIG)
    table = 'world_boss_state' if target == 'state' and action != 'late_rewrite' else 'world_boss_manual_spawn_operations'
    event = 'UPDATE' if table == 'world_boss_state' else 'INSERT'
    timing = 'BEFORE' if action == 'ignore' else 'AFTER'
    mutation = 'SELECT RAISE(IGNORE);' if action == 'ignore' else (
        "UPDATE world_boss_state SET revision=99 WHERE state_key='global';" if target == 'state' else
        "UPDATE world_boss_manual_spawn_operations SET payload='rewritten';")
    with DatabaseUnitOfWork(state.database) as uow:
        uow.execute(f'CREATE TRIGGER refuse_spawn {timing} {event} ON {table} BEGIN '
                    f'UPDATE boss SET boss_stone=0; {mutation} END')
    before = snapshot(state)
    result = spawn(repo)
    assert not result.succeeded
    assert (result.reason, result.subject) == (f'world_boss_spawn_{target}_write_rejected',
                                             'global' if target == 'state' else 'spawn-test')
    assert snapshot(state) == before
    with DatabaseUnitOfWork(state.database) as uow:
        uow.execute('DROP TRIGGER refuse_spawn')
    assert spawn(repo).status == 'spawned'
    saved = snapshot(state)
    assert spawn(repo).status == 'duplicate' and snapshot(state) == saved


def test_corrupt_state_is_not_replaced_as_empty_session(state):
    repo = WorldBossManualSpawnSqlRepository(state.database, lambda: CONFIG)
    with DatabaseUnitOfWork(state.database) as uow:
        uow.execute("UPDATE world_boss_state SET bosses='broken-json'")
    before = snapshot(state)
    with pytest.raises(Exception, match='world_boss_state_invalid'):
        repo.snapshot()
    assert snapshot(state) == before


def test_same_message_changed_explicit_parameters_does_not_replay_different_boss(state):
    with patch.object(boss, 'createboss_jj', return_value=deepcopy(BOSS)):
        assert boss._spawn_world_boss('same-message', '练气境', BOSS['name']).status == 'spawned'
        before = snapshot(state)
        result = boss._spawn_world_boss('same-message', '筑基境', 'other-boss')
    assert not result.succeeded and result.reason == 'operation_conflict'
    assert snapshot(state) == before


def test_known_rejection_keeps_specific_reason_in_actual_random_handler(state):
    with DatabaseUnitOfWork(state.database) as uow:
        uow.execute('DROP TABLE world_boss_manual_spawn_operations')
    sent = AsyncMock()
    event = SimpleNamespace(message_id='manual-test')
    with patch.object(boss, 'createboss', return_value='练气境'), \
            patch.object(boss, 'createboss_jj', return_value=deepcopy(BOSS)), \
            patch.object(boss, 'assign_bot', new=AsyncMock(return_value=(object(), None))), \
            patch.object(boss, 'handle_send', sent):
        with pytest.raises(FinishedException):
            asyncio.run(boss.create_(object(), event, SimpleNamespace(extract_plain_text=lambda: '')))
    assert 'schema_missing' in sent.call_args.args[2]
    assert '场次或生成配置已变化' not in sent.call_args.args[2]


@pytest.mark.parametrize('target', ['state', 'receipt'])
def test_initial_state_insert_refusal_and_unknown_abort_roll_back(state, target):
    repo = WorldBossManualSpawnSqlRepository(state.database, lambda: CONFIG)
    with DatabaseUnitOfWork(state.database) as uow:
        uow.execute('DELETE FROM world_boss_state')
        table = 'world_boss_state' if target == 'state' else 'world_boss_manual_spawn_operations'
        uow.execute(f'CREATE TRIGGER refuse_spawn BEFORE INSERT ON {table} '
                    "BEGIN UPDATE boss SET boss_stone=0; SELECT RAISE(ABORT,'isolated manual abort'); END")
    before = snapshot(state)
    with pytest.raises(Exception, match='isolated manual abort'):
        spawn(repo)
    assert snapshot(state) == before
    with DatabaseUnitOfWork(state.database) as uow:
        uow.execute('DROP TRIGGER refuse_spawn')
        uow.execute('CREATE TRIGGER refuse_spawn BEFORE INSERT ON world_boss_state '
                    'BEGIN SELECT RAISE(IGNORE); END')
    assert spawn(repo).reason == 'world_boss_spawn_state_write_rejected'
    assert snapshot(state) == before


@pytest.mark.parametrize('changes,reason,subject', [
    ({'expected_revision': 1}, 'session_changed', 'global'),
    ({'expected_config': {}}, 'config_changed', 'boss_config'),
    ({'boss': {**BOSS, 'stone': -1}}, 'world_boss_generated_invalid', '练气境'),
])
def test_known_rejection_is_precise_and_zero_write(state, changes, reason, subject):
    repo = WorldBossManualSpawnSqlRepository(state.database, lambda: CONFIG)
    before = snapshot(state)
    result = spawn(repo, **changes)
    assert not result.succeeded and (result.reason, result.subject) == (reason, subject)
    assert snapshot(state) == before


@pytest.mark.parametrize('value', ['broken-json', '{}', '[1]'])
def test_bad_session_does_not_get_silently_filtered_or_overwritten(state, value):
    repo = WorldBossManualSpawnSqlRepository(state.database, lambda: CONFIG)
    with DatabaseUnitOfWork(state.database) as uow:
        uow.execute('UPDATE world_boss_state SET bosses=?', (value,))
    before = snapshot(state)
    result = spawn(repo)
    assert result.reason == 'world_boss_state_invalid' and snapshot(state) == before


@pytest.mark.parametrize('column,value', [('result_json', 'bad'), ('result_json', '[]'),
                                         ('result_json', '{"bosses":[{}],"boss":{},"revision":1}'),
                                         ('payload', 'bad'), ('payload', '{}')])
def test_bad_receipt_is_explicit_and_never_overwritten(state, column, value):
    repo = WorldBossManualSpawnSqlRepository(state.database, lambda: CONFIG)
    assert spawn(repo).status == 'spawned'
    with DatabaseUnitOfWork(state.database) as uow:
        uow.execute(f'UPDATE world_boss_manual_spawn_operations SET {column}=?', (value,))
    before = snapshot(state)
    with pytest.raises(Exception, match='world_boss_spawn_receipt_invalid'):
        repo.get_result('spawn-test')
    assert snapshot(state) == before


def test_replay_preserves_current_damage_after_real_scheduled_refresh(state):
    with patch.object(boss, 'createboss_jj', return_value=deepcopy(BOSS)) as generate:
        assert boss._spawn_world_boss('manual-before-task', '练气境').status == 'spawned'
        assert asyncio.run(fire(boss.generate_all_bosses_task)).code == EVENT_JOB_EXECUTED
        damaged = [{**BOSS, '气血': 700}]
        with DatabaseUnitOfWork(state.database) as uow:
            uow.execute('UPDATE world_boss_state SET bosses=?,revision=3', (json.dumps(damaged),))
        before = snapshot(state)
        result = boss._spawn_world_boss('manual-before-task', '练气境')
        assert result.status == 'duplicate' and result.boss == BOSS
        assert snapshot(state) == before and boss.group_boss['global'] == boss.old_boss_info.data['global'] == damaged
        generate.assert_called_once()
        conflict = boss._spawn_world_boss('manual-before-task', '练气境', BOSS['name'])
        assert conflict.reason == 'operation_conflict' and snapshot(state) == before
    with DatabaseUnitOfWork(state.database, read_only=True) as uow:
        assert uow.query_one('PRAGMA integrity_check') == {'integrity_check': 'ok'}


def test_old_payload_without_request_replays_before_current_config_validation(state):
    repo = WorldBossManualSpawnSqlRepository(state.database, lambda: CONFIG)
    assert spawn(repo).status == 'spawned'
    with patch.object(boss, 'get_boss_config', return_value={}), patch.object(boss, 'createboss_jj') as generate:
        result = boss._spawn_world_boss('spawn-test', '练气境', BOSS['name'])
        assert result.status == 'duplicate' and result.boss == BOSS
        assert boss._spawn_world_boss('spawn-test', '筑基境').reason == 'operation_conflict'
        generate.assert_not_called()


@pytest.mark.parametrize('target', ['state', 'receipt'])
def test_legacy_manual_facade_uses_same_atomic_write_checks(state, target):
    from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_boss.transaction_service import WorldBossManualSpawnService
    repo = WorldBossManualSpawnService(state.database, lambda: CONFIG)
    table, event = ('world_boss_state', 'UPDATE') if target == 'state' else ('world_boss_manual_spawn_operations', 'INSERT')
    with DatabaseUnitOfWork(state.database) as uow:
        uow.execute(f'CREATE TRIGGER refuse_spawn BEFORE {event} ON {table} BEGIN SELECT RAISE(IGNORE); END')
    before = snapshot(state)
    assert spawn(repo).reason == f'world_boss_spawn_{target}_write_rejected'
    assert snapshot(state) == before
    with DatabaseUnitOfWork(state.database) as uow:
        uow.execute('DROP TRIGGER refuse_spawn')
    assert spawn(repo).status == 'spawned' and repo.get_result('spawn-test').status == 'duplicate'


@pytest.mark.parametrize('appointed', [False, True])
def test_unknown_fault_and_normal_flow_and_failed_delivery_propagate(state, appointed):
    handler = boss.create_appointed_ if appointed else boss.create_
    args = SimpleNamespace(extract_plain_text=lambda: '练气境' if appointed else '')
    event = SimpleNamespace(message_id='manual-test', get_user_id=lambda: 'admin')
    error, delivery = RuntimeError('isolated spawn fault'), RuntimeError('isolated spawn reply fault')
    generate, sent = Mock(side_effect=error), AsyncMock()
    with patch.object(boss, '_spawn_world_boss', generate), patch.object(boss, 'handle_send', sent), \
            patch.object(boss, 'assign_bot', new=AsyncMock(return_value=(object(), None))), \
            patch.object(boss, 'report_exception', return_value='定位号：manual-test') as report:
        with pytest.raises(FinishedException):
            asyncio.run(handler(object(), event, args))
        assert report.call_args.args[1] is error and '定位号：manual-test' in sent.call_args.args[2]
        sent.side_effect = delivery
        with pytest.raises(RuntimeError) as caught:
            asyncio.run(handler(object(), event, args))
        assert caught.value is delivery
        for flow in (FinishedException(), StopPropagation()):
            generate.side_effect = flow
            report.reset_mock()
            sent.reset_mock()
            with pytest.raises(type(flow)) as caught:
                asyncio.run(handler(object(), event, args))
            assert caught.value is flow
            report.assert_not_called()
            sent.assert_not_called()


@pytest.mark.parametrize('reason', ['schema_missing', 'operation_conflict', 'config_changed', 'session_changed',
                                  'world_boss_generated_invalid', 'world_boss_realm_invalid', 'world_boss_name_invalid',
                                  'world_boss_state_invalid', 'world_boss_spawn_receipt_invalid',
                                  'world_boss_spawn_state_write_rejected', 'world_boss_spawn_receipt_write_rejected'])
@pytest.mark.parametrize('onebot,markdown,send_fails', [(True, True, False), (False, False, False),
                                                     (False, True, False), (False, True, True)])
def test_specific_rejection_survives_adapter_reply_and_markdown_fallback(state, reason, onebot, markdown, send_fails):
    from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils import utils
    result = WorldBossManualSpawnResult('state_changed', reason=reason, subject='subject-test')
    event = SimpleNamespace(message_id='manual-test', get_user_id=lambda: 'admin')
    args = SimpleNamespace(extract_plain_text=lambda: '练气境')
    captured = AsyncMock()
    with patch.object(boss, '_spawn_world_boss', return_value=result), patch.object(boss, 'handle_send', captured), \
            patch.object(boss, 'assign_bot', new=AsyncMock(return_value=(object(), None))):
        with pytest.raises(FinishedException):
            asyncio.run(boss.create_appointed_(object(), event, args))
    reply = captured.call_args.args[2]
    assert reason in reply and '主体：subject-test' in reply and '已生成' not in reply
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
