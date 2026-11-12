from concurrent.futures import ThreadPoolExecutor
import sqlite3
import tempfile
import unittest
from pathlib import Path

import pytest
import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.core.errors import DomainError, OperationConflictError
from nonebot_plugin_xiuxian_2.core.numeric import as_int_like, bind_sqlite_param
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_activity import service
from tests.test_activity_event_replay import _configure_activity


def snapshot(database):
    with sqlite3.connect(database) as conn:
        return list(conn.iterdump())


@pytest.fixture
def event_database(monkeypatch, tmp_path):
    database = tmp_path / 'game.db'
    _configure_activity(monkeypatch, database, [{
        'key': 'money-event', 'name': 'test', 'type': 'event_points',
        'event_rules': [{'event': 'work', 'points': 1}],
    }])
    return database


def balance(database):
    with DatabaseUnitOfWork(database, read_only=True) as uow:
        return tuple(as_int_like(v) for v in uow.query_one(
            "SELECT points,total_points FROM activity_point_balance WHERE user_id='u'").values())


def event(amount=1, event_id='reward'):
    return service.record_activity_event('u', 'work', amount, event_id=event_id,
                                         occurred_at='2026-11-12T10:00:00+08:00')


@pytest.mark.parametrize('value', [2**53 + 1, 2**63 - 1, 2**63 + 1, 10**45 + 123])
def test_large_event_reward_replay_and_payload_conflict(event_database, value):
    assert event(value)
    assert balance(event_database) == (value, value)
    before = snapshot(event_database)
    assert event(value) == []
    with pytest.raises(OperationConflictError):
        event(value + 1)
    assert before == snapshot(event_database)
    with DatabaseUnitOfWork(event_database, read_only=True) as uow:
        assert as_int_like(uow.query_one('SELECT points FROM activity_point_event_log')['points']) == value
        assert uow.query_one('SELECT COUNT(*) AS n FROM activity_event_operations')['n'] == 1


def test_same_request_concurrent_reward_is_once_and_different_requests_accumulate(event_database):
    value = 10**45 + 123
    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(lambda _: event(value), range(2)))
    assert sorted(map(len, results)) == [0, 1]
    assert balance(event_database) == (value, value)
    with ThreadPoolExecutor(max_workers=2) as pool:
        list(pool.map(lambda i: event(1, f'new-{i}'), range(2)))
    assert balance(event_database) == (value + 2, value + 2)


@pytest.mark.parametrize('bad,reason', [(float(2**63), 'activity_points_precision_lost_real'),
                                       ('broken', 'activity_points_invalid_numeric_value')])
def test_bad_balance_or_daily_log_refuses_with_zero_writes(event_database, bad, reason):
    with DatabaseUnitOfWork(event_database) as uow:
        uow.execute("INSERT INTO activity_point_balance(activity_key,user_id,points,total_points) VALUES('money-event','u',?,?)", (bad, 0))
    before = snapshot(event_database)
    with pytest.raises(DomainError) as caught:
        event()
    assert caught.value.details['reason'] == reason
    assert snapshot(event_database) == before


def test_receipt_failure_rolls_back_reward_then_recovers(event_database):
    with DatabaseUnitOfWork(event_database) as uow:
        uow.execute("CREATE TRIGGER fail_reward BEFORE INSERT ON activity_point_event_log BEGIN SELECT RAISE(ABORT,'reward fault'); END")
    before = snapshot(event_database)
    with pytest.raises(sqlite3.IntegrityError, match='reward fault'):
        event(10**45 + 123)
    assert snapshot(event_database) == before
    with DatabaseUnitOfWork(event_database) as uow:
        uow.execute('DROP TRIGGER fail_reward')
    assert event(10**45 + 123)
    assert balance(event_database) == (10**45 + 123,) * 2


def test_admin_adjustments_serialize_exact_read_modify_write(event_database):
    value = 10**45 + 123
    service.adjust_activity_points('money-event', 'u', value)
    with ThreadPoolExecutor(max_workers=2) as pool:
        list(pool.map(lambda _: service.adjust_activity_points('money-event', 'u', 1), range(2)))
    assert balance(event_database) == (value + 2, value + 2)


def test_admin_batch_large_progress_resume_and_bad_history_zero_write(tmp_path):
    from nonebot_plugin_xiuxian_2.features.admin_asset.tests.test_stone_batch_repository import AdminStoneBatchRepositoryTests
    from nonebot_plugin_xiuxian_2.features.admin_asset.stone_batch_repository import AdminStoneBatchSqlRepository
    value = 10**45 + 123
    database = AdminStoneBatchRepositoryTests()._database(str(tmp_path), users=[('a', bind_sqlite_param(value, exact=True)), ('b', bind_sqlite_param(value + 1, exact=True))])
    repo = AdminStoneBatchSqlRepository(database)
    first = repo.adjust('batch', 'operator', value, chunk_size=1)
    assert first.applied_delta == value
    assert repo.find_running('operator', value) == 'batch'
    final = repo.adjust('batch', 'operator', value, chunk_size=1)
    assert (final.applied_delta, final.completed) == (value * 2, 2)
    before = snapshot(database)
    assert repo.adjust('batch', 'operator', value).status == 'duplicate'
    assert repo.adjust('batch', 'operator', value + 1).status == 'operation_conflict'
    assert snapshot(database) == before
    with DatabaseUnitOfWork(database, read_only=True) as uow:
        for i, row in enumerate(uow.query_all('SELECT previous_stone,final_stone,applied_delta FROM admin_stone_batch_progress ORDER BY user_id')):
            assert tuple(as_int_like(v) for v in row.values()) == (value + i, value * 2 + i, value)
    with DatabaseUnitOfWork(database) as uow:
        uow.execute("UPDATE user_xiuxian SET stone=? WHERE user_id='b'", (float(2**63),))
    before = snapshot(database)
    with pytest.raises(DomainError) as caught:
        repo.adjust('bad', 'operator', 1, chunk_size=2)
    assert caught.value.details['reason'] == 'stone_precision_lost_real'
    assert snapshot(database) == before


def test_player_manager_integer_money_write_read_and_bad_input_zero_write(tmp_path, monkeypatch):
    from tests.test_player_field_list_cache import module
    class IsolatedManager(module.PlayerDataManager):
        _instance = {}
        _has_init = {}
    monkeypatch.setattr(module, 'DATABASE', tmp_path)
    manager = IsolatedManager()
    try:
        for table, field in [('training', 'points'), ('tower', 'score'), ('arena', 'honor_points'), ('boss_limit', 'integral'), ('bankinfo', 'savestone')]:
            for value in (2**53 + 1, 2**63 + 1, 10**45 + 123):
                manager.update_or_write_data('u', table, field, value, data_type='INTEGER')
                assert manager.get_all_field_data(table, field) == [('u', value)]
            before = snapshot(manager.database_path)
            with pytest.raises(DomainError):
                manager.update_or_write_data('u', table, field, float(2**63), data_type='INTEGER')
            assert snapshot(manager.database_path) == before
    finally:
        manager.close()


def load_tests(loader, suite, pattern):
    # The release workflow uses unittest; reuse the same isolated pytest cases.
    event_cases = [
        (test_large_event_reward_replay_and_payload_conflict, (value,))
        for value in (2**53 + 1, 2**63 - 1, 2**63 + 1, 10**45 + 123)
    ] + [
        (test_same_request_concurrent_reward_is_once_and_different_requests_accumulate, ()),
        (test_bad_balance_or_daily_log_refuses_with_zero_writes, (float(2**63), 'activity_points_precision_lost_real')),
        (test_bad_balance_or_daily_log_refuses_with_zero_writes, ('broken', 'activity_points_invalid_numeric_value')),
        (test_receipt_failure_rolls_back_reward_then_recovers, ()),
        (test_admin_adjustments_serialize_exact_read_modify_write, ()),
    ]
    for function, arguments in event_cases:
        def run(function=function, arguments=arguments):
            with tempfile.TemporaryDirectory() as directory, pytest.MonkeyPatch.context() as monkeypatch:
                database = Path(directory) / 'game.db'
                _configure_activity(monkeypatch, database, [{
                    'key': 'money-event', 'name': 'test', 'type': 'event_points',
                    'event_rules': [{'event': 'work', 'points': 1}],
                }])
                function(database, *arguments)
        suite.addTest(unittest.FunctionTestCase(run, description=function.__name__ + repr(arguments)))
    for function in (test_admin_batch_large_progress_resume_and_bad_history_zero_write,
                     test_player_manager_integer_money_write_read_and_bad_input_zero_write):
        def run(function=function):
            with tempfile.TemporaryDirectory() as directory, pytest.MonkeyPatch.context() as monkeypatch:
                if function is test_player_manager_integer_money_write_read_and_bad_input_zero_write:
                    function(Path(directory), monkeypatch)
                else:
                    function(Path(directory))
        suite.addTest(unittest.FunctionTestCase(run, description=function.__name__))
    return suite
