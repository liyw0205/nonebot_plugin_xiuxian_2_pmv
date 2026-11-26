from __future__ import annotations

import copy
import importlib.util
import sys
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from types import ModuleType, SimpleNamespace
from unittest.mock import Mock, patch

import pytest
from tests.test_db_backend import db_backend


# Load the real source without starting the legacy plugin package or its jobs.
_package = ModuleType("nonebot_plugin_xiuxian_2._cache_tests.field_lists")
_package.__path__ = []
_package.db_backend = db_backend
_cache_module = ModuleType(f"{_package.__name__}.shared_read_cache")
_invalidate_snapshot_database = Mock()
_cache_module.process_read_cache = SimpleNamespace(
    invalidate_snapshot_database=_invalidate_snapshot_database
)
_path = Path(__file__).resolve().parents[1] / "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_utils/player_data_manager.py"
_spec = importlib.util.spec_from_file_location(f"{_package.__name__}.manager", _path)
module = importlib.util.module_from_spec(_spec)
with patch.dict(
    sys.modules,
    {
        _package.__name__: _package,
        f"{_package.__name__}.shared_read_cache": _cache_module,
    },
):
    _spec.loader.exec_module(module)


@pytest.fixture
def cache_manager(monkeypatch, tmp_path):
    manager = object.__new__(module.PlayerDataManager)
    manager._conn_lock = threading.RLock()
    manager.lock = manager._conn_lock
    manager._field_list_cache = {}
    manager._field_list_cache_charged_bytes = 0
    manager._ensured_tables = set()
    manager._ensured_fields = set()
    manager.database_path = tmp_path / "unused-player.db"
    manager._ensure_table_exists = Mock()
    manager._ensure_field_exists = Mock()
    state = SimpleNamespace(now=100.0, rows=[("u", '{"score":[1]}')])
    cursor = Mock()
    cursor.fetchall.side_effect = lambda: list(state.rows)
    manager.conn = Mock()
    manager.conn.cursor.return_value = cursor
    state.cursor = cursor
    monkeypatch.setattr(module, "time", SimpleNamespace(monotonic=lambda: state.now))
    return manager, state


def query(manager, kind, *, table="scores", ttl=10):
    if kind == "fields":
        return manager.get_all_field_data(table, "points", cache_ttl=ttl)
    return manager.list_users_by_fields(table, {"built": 1}, cache_ttl=ttl)


def test_get_all_fields_projects_requested_columns_without_decoding_wide_rows(tmp_path):
    database = tmp_path / "projected-player.db"
    with db_backend.transaction(database) as connection:
        connection.execute(
            "CREATE TABLE mentor(user_id TEXT PRIMARY KEY,mentor_id TEXT,"
            "apprentice_ids TEXT,mentor_history TEXT)"
        )
        connection.execute(
            "INSERT INTO mentor VALUES(%s,%s,%s,%s)",
            ("mentor", None, '["apprentice"]', '[{"large":"unused"}]'),
        )

    manager = object.__new__(module.PlayerDataManager)
    manager._conn_lock = threading.RLock()
    manager._ensured_tables = set()
    manager._ensured_fields = set()
    manager.database_path = database
    manager.conn = db_backend.connect(database, check_same_thread=False)
    try:
        rows = manager.get_all_fields("mentor", ("mentor_id", "apprentice_ids"))
    finally:
        manager.conn.close()

    assert rows == [
        {"user_id": "mentor", "mentor_id": None, "apprentice_ids": ["apprentice"]}
    ]


def assert_accounting(manager):
    assert manager._field_list_cache_charged_bytes == sum(
        entry[2] for entry in manager._field_list_cache.values()
    )
    assert 0 <= manager._field_list_cache_charged_bytes <= module.FIELD_LIST_CACHE_MAX_BYTES
    assert len(manager._field_list_cache) <= module.FIELD_LIST_CACHE_MAX_ENTRIES
    assert all(entry[2] <= module.FIELD_LIST_CACHE_MAX_ENTRY_BYTES for entry in manager._field_list_cache.values())


@pytest.mark.parametrize("kind", ("fields", "ids"))
def test_expired_other_keys_are_removed_on_read(cache_manager, kind):
    manager, state = cache_manager
    query(manager, kind, table="old")
    state.now += 10
    query(manager, kind, table="new")
    assert {key[1] for key in manager._field_list_cache} == {"new"}
    assert_accounting(manager)


@pytest.mark.parametrize("kind", ("fields", "ids"))
def test_exact_deadline_and_non_sliding_ttl(cache_manager, kind):
    manager, state = cache_manager
    first = query(manager, kind)
    state.now += 9
    assert query(manager, kind) == first
    assert state.cursor.execute.call_count == 1
    state.rows = [("v", '{"score":[2]}')]
    state.now += 1
    assert query(manager, kind) != first
    assert state.cursor.execute.call_count == 2
    assert_accounting(manager)


@pytest.mark.parametrize("kind", ("fields", "ids"))
def test_cache_results_are_independent_copies(cache_manager, kind):
    manager, state = cache_manager
    first = query(manager, kind)
    if kind == "fields":
        first[0][1]["score"].append(99)
    else:
        first.append("other")
    second = query(manager, kind)
    assert second == ([("u", {"score": [1]})] if kind == "fields" else ["u"])
    second.clear()
    assert query(manager, kind)
    assert state.cursor.execute.call_count == 1


@pytest.mark.parametrize("kind", ("fields", "ids"))
@pytest.mark.parametrize("ttl", (0, -1, float("inf"), float("nan")))
def test_uncached_queries_still_sweep_expired_entries(cache_manager, kind, ttl):
    manager, state = cache_manager
    query(manager, kind, table="old")
    state.now += 10
    assert query(manager, kind, table="uncached", ttl=ttl)
    assert manager._field_list_cache == {}
    assert_accounting(manager)


def test_empty_filter_still_performs_expiry_maintenance(cache_manager):
    manager, state = cache_manager
    query(manager, "ids")
    state.now += 10
    assert manager.list_users_by_fields("scores", {}) == []
    assert manager._field_list_cache == {}
    assert_accounting(manager)


@pytest.mark.parametrize("kind", ("fields", "ids"))
def test_empty_result_is_cached(cache_manager, kind):
    manager, state = cache_manager
    state.rows = []
    assert query(manager, kind) == []
    assert query(manager, kind) == []
    assert state.cursor.execute.call_count == 1
    assert_accounting(manager)


def test_entry_budget_is_shared_fifo_not_lru(cache_manager, monkeypatch):
    manager, _ = cache_manager
    monkeypatch.setattr(module, "FIELD_LIST_CACHE_MAX_ENTRIES", 2)
    query(manager, "fields", table="a")
    query(manager, "ids", table="b")
    query(manager, "fields", table="a")
    query(manager, "fields", table="c")
    assert {key[1] for key in manager._field_list_cache} == {"b", "c"}
    assert_accounting(manager)


def test_byte_budget_evicts_multiple_entries_and_replacement_releases_charge(cache_manager, monkeypatch):
    manager, _ = cache_manager
    keys = [("get_all_field_data", name, "points") for name in ("a", "b", "c")]
    small = [("u", "x" * 100)]
    small_charge = module._field_list_cache_charge(keys[0], small)
    big = [("u", "x" * (2 * small_charge))]
    big_charge = module._field_list_cache_charge(keys[2], big)
    monkeypatch.setattr(module, "FIELD_LIST_CACHE_MAX_BYTES", big_charge + small_charge // 2)
    assert manager._store_cached_field_list(keys[0], small, 10, deep=True)
    assert manager._store_cached_field_list(keys[1], small, 10, deep=True)
    assert manager._store_cached_field_list(keys[2], big, 10, deep=True)
    assert list(manager._field_list_cache) == [keys[2]]
    assert_accounting(manager)
    assert manager._store_cached_field_list(keys[2], small, 10, deep=True)
    assert manager._field_list_cache_charged_bytes == module._field_list_cache_charge(
        keys[2], manager._field_list_cache[keys[2]][1]
    )
    assert_accounting(manager)


@pytest.mark.parametrize("large_part", ("key", "value", "deep_json", "wide_json"))
def test_large_results_bypass_cache_before_copy(cache_manager, monkeypatch, large_part):
    manager, state = cache_manager
    spy = Mock(wraps=copy.deepcopy)
    monkeypatch.setattr(module, "copy", SimpleNamespace(deepcopy=spy))
    if large_part == "key":
        result = manager.get_all_field_data("x" * (module.FIELD_LIST_CACHE_MAX_ENTRY_BYTES + 1), "points")
    else:
        if large_part == "value":
            value = {"value": "x" * (module.FIELD_LIST_CACHE_MAX_ENTRY_BYTES + 1)}
        elif large_part == "deep_json":
            value = 1
            for _ in range(module.FIELD_LIST_CACHE_MAX_DEPTH + 1):
                value = [value]
        else:
            value = [0] * (module.FIELD_LIST_CACHE_MAX_NODES + 1)
        state.rows = [("u", module.json.dumps(value))]
        result = query(manager, "fields")
        assert result == [("u", value)]
    assert result
    assert manager._field_list_cache == {}
    spy.assert_not_called()
    assert_accounting(manager)


def test_large_exclude_user_key_is_not_cached(cache_manager):
    manager, _ = cache_manager
    assert manager.list_users_by_fields(
        "scores", {"built": 1}, exclude_user_id="x" * (module.FIELD_LIST_CACHE_MAX_ENTRY_BYTES + 1)
    ) == ["u"]
    assert manager._field_list_cache == {}
    assert_accounting(manager)


def test_unsupported_and_cyclic_values_cannot_trigger_cache_copy(cache_manager, monkeypatch):
    manager, _ = cache_manager

    class Unsupported:
        def __sizeof__(self):
            raise AssertionError("unsupported object must not be sized")

    cycle = []
    cycle.append(cycle)
    spy = Mock(side_effect=AssertionError("uncacheable objects must not be copied"))
    monkeypatch.setattr(module, "copy", SimpleNamespace(deepcopy=spy))
    for value in ([Unsupported()], cycle):
        assert not manager._store_cached_field_list(("test", "a"), value, 10, deep=True)
    spy.assert_not_called()
    assert manager._field_list_cache == {}
    assert_accounting(manager)


def test_failed_copy_measurement_does_not_evict_valid_entries(cache_manager, monkeypatch):
    manager, _ = cache_manager
    monkeypatch.setattr(module, "FIELD_LIST_CACHE_MAX_ENTRIES", 1)
    query(manager, "fields", table="valid")
    before = dict(manager._field_list_cache)
    spy = Mock(return_value=["x" * (module.FIELD_LIST_CACHE_MAX_ENTRY_BYTES + 1)])
    monkeypatch.setattr(module, "copy", SimpleNamespace(deepcopy=spy))
    assert not manager._store_cached_field_list(("test", "new"), ["small"], 10, deep=True)
    assert manager._field_list_cache == before
    assert_accounting(manager)


def test_charge_node_and_depth_boundaries_are_exact(monkeypatch):
    key = ("test", "a")
    monkeypatch.setattr(module, "FIELD_LIST_CACHE_MAX_NODES", 6)
    assert module._field_list_cache_charge(key, [[], []]) is not None
    assert module._field_list_cache_charge(key, [[], [0]]) is None
    monkeypatch.setattr(module, "FIELD_LIST_CACHE_MAX_NODES", 8192)
    monkeypatch.setattr(module, "FIELD_LIST_CACHE_MAX_DEPTH", 2)
    assert module._field_list_cache_charge(key, [[0]]) is not None
    assert module._field_list_cache_charge(key, [[[0]]]) is None


def test_finite_ttl_deadline_overflow_is_not_cached(cache_manager):
    manager, state = cache_manager
    state.now = sys.float_info.max
    assert not manager._store_cached_field_list(("test", "a"), ["u"], sys.float_info.max)
    assert manager._field_list_cache == {}
    assert_accounting(manager)


def test_same_key_rejection_preserves_value_and_successful_replacement_moves_fifo(cache_manager, monkeypatch):
    manager, _ = cache_manager
    monkeypatch.setattr(module, "FIELD_LIST_CACHE_MAX_ENTRIES", 2)
    key_a, key_b, key_c = [("test", name) for name in ("a", "b", "c")]
    assert manager._store_cached_field_list(key_a, ["old"], 10)
    assert manager._store_cached_field_list(key_b, ["b"], 10)
    before = dict(manager._field_list_cache)
    assert not manager._store_cached_field_list(key_a, ["x" * (module.FIELD_LIST_CACHE_MAX_ENTRY_BYTES + 1)], 10)
    assert manager._field_list_cache == before
    assert_accounting(manager)
    assert manager._store_cached_field_list(key_a, ["new"], 10)
    assert manager._store_cached_field_list(key_c, ["c"], 10)
    assert list(manager._field_list_cache) == [key_a, key_c]
    assert manager._get_cached_field_list(key_a, 10) == ["new"]
    assert_accounting(manager)


def test_table_field_and_global_invalidation_preserve_accounting(cache_manager):
    manager, _ = cache_manager
    manager.get_all_field_data("a", "one")
    manager.get_all_field_data("a", "two")
    query(manager, "ids", table="a")
    query(manager, "fields", table="b")
    manager.invalidate_field_list_cache("a", "one")
    assert set(manager._field_list_cache) == {
        ("get_all_field_data", "a", "two"), ("get_all_field_data", "b", "points")
    }
    assert_accounting(manager)
    manager.invalidate_field_list_cache("a")
    assert {key[1] for key in manager._field_list_cache} == {"b"}
    assert_accounting(manager)
    manager.invalidate_field_list_cache()
    assert manager._field_list_cache == {}
    assert manager._field_list_cache_charged_bytes == 0


@pytest.mark.parametrize("action", ("close", "reconnect", "failed_reconnect"))
def test_connection_lifecycle_releases_only_field_list_cache(cache_manager, monkeypatch, action):
    _invalidate_snapshot_database.reset_mock()
    manager, _ = cache_manager
    old_connection = manager.conn
    manager.other_cache = {"preserve": "business-state"}
    query(manager, "fields")
    connect = Mock(return_value=Mock())
    monkeypatch.setattr(module.db_backend, "connect", connect)
    if action == "failed_reconnect":
        connect.side_effect = RuntimeError("test unavailable")
        with pytest.raises(RuntimeError, match="test unavailable"):
            manager.reconnect()
    else:
        getattr(manager, action)()
    old_connection.close.assert_called_once()
    assert manager.other_cache == {"preserve": "business-state"}
    assert manager._field_list_cache == {}
    assert manager._field_list_cache_charged_bytes == 0
    if action == "reconnect":
        _invalidate_snapshot_database.assert_called_once_with(manager.database_path)
    else:
        _invalidate_snapshot_database.assert_not_called()
    assert not manager.database_path.exists()
    assert_accounting(manager)


@pytest.mark.parametrize("kind", ("fields", "ids"))
def test_query_failure_drops_expired_cache_without_publishing(cache_manager, kind):
    manager, state = cache_manager
    query(manager, kind)
    state.now += 10
    state.cursor.execute.side_effect = RuntimeError("test query failed")
    if kind == "fields":
        with pytest.raises(RuntimeError, match="test query failed"):
            query(manager, kind)
    else:
        assert query(manager, kind) == []
    assert manager._field_list_cache == {}
    assert_accounting(manager)


def test_concurrent_stores_keep_shared_accounting_consistent(cache_manager, monkeypatch):
    manager, _ = cache_manager
    monkeypatch.setattr(module, "FIELD_LIST_CACHE_MAX_ENTRIES", 3)
    with ThreadPoolExecutor(max_workers=4) as executor:
        results = list(executor.map(
            lambda index: manager._store_cached_field_list(("test", str(index)), ["u"], 10),
            range(48),
        ))
    assert all(results)
    assert len(manager._field_list_cache) == 3
    assert_accounting(manager)


def test_real_sqlite_writes_and_reconnect_do_not_replay_stale_cache(tmp_path, monkeypatch):
    class IsolatedManager(module.PlayerDataManager):
        _instance = {}
        _has_init = {}

    monkeypatch.setattr(module, "DATABASE", tmp_path)
    manager = IsolatedManager()
    try:
        manager.update_or_write_data("u", "scores", "points", {"score": [1]})
        assert manager.get_all_field_data("scores", "points") == [("u", {"score": [1]})]
        assert_accounting(manager)
        manager.update_or_write_data("u", "scores", "points", {"score": [2]})
        assert manager._field_list_cache == {}
        assert manager.get_all_field_data("scores", "points") == [("u", {"score": [2]})]
        manager.reconnect()
        assert manager._field_list_cache == {}
        assert manager._field_list_cache_charged_bytes == 0
        assert manager.get_all_field_data("scores", "points") == [("u", {"score": [2]})]
    finally:
        manager.close()


def test_get_fields_by_ids_batches_requested_relationship_rows(tmp_path, monkeypatch):
    class IsolatedManager(module.PlayerDataManager):
        _instance = {}
        _has_init = {}

    monkeypatch.setattr(module, "DATABASE", tmp_path)
    manager = IsolatedManager()
    try:
        manager.update_or_write_data("u1", "partner", "affection", 125)
        manager.update_or_write_data("u1", "partner", "partner_id", "u2")
        manager.update_or_write_data("u2", "partner", "mentor_id", "u1")
        rows = manager.get_fields_by_ids("partner", ["u1", "u2", "u1"], ("partner_id",))
        assert rows == {
            "u1": {"user_id": "u1", "partner_id": "u2"},
            "u2": {"user_id": "u2", "partner_id": None},
        }
        assert manager.get_all_field_data("partner", "affection") == [
            ("u1", 125),
            ("u2", None),
        ]
    finally:
        manager.close()


def test_display_batch_is_live_chunked_and_never_initializes_schema(tmp_path):
    database = tmp_path / "live-player.db"
    with db_backend.transaction(database) as connection:
        connection.execute("CREATE TABLE mentor(user_id TEXT PRIMARY KEY,mentor_id TEXT)")
        connection.executemany("INSERT INTO mentor VALUES(%s,'m')", [(str(i),) for i in range(1, 402)])
    manager = object.__new__(module.PlayerDataManager)
    manager._conn_lock = threading.RLock()
    manager.conn = db_backend.connect_readonly(database)
    manager._ensure_table_exists = Mock(side_effect=AssertionError("unexpected DDL"))
    manager._ensure_field_exists = Mock(side_effect=AssertionError("unexpected DDL"))
    statements = []
    manager.conn._raw.set_trace_callback(statements.append)
    try:
        assert manager.get_fields_by_ids("mentor", [], ("mentor_id",), ensure_fields=False) == {}
        assert statements == []
        ids = [str(i) for i in range(1, 402)] + ["1", "missing"]
        rows = manager.get_fields_by_ids("mentor", ids, ("mentor_id",), ensure_fields=False)
        assert len(rows) == 401
        assert rows["1"]["mentor_id"] == "m"
        assert len([sql for sql in statements if sql.startswith('SELECT "user_id"')]) == 2
        assert not any(sql.startswith(("CREATE", "ALTER", "INSERT", "UPDATE")) for sql in statements)
        with db_backend.transaction(database) as connection:
            connection.execute("UPDATE mentor SET mentor_id='new' WHERE user_id='1'")
        assert manager.get_fields_by_ids("mentor", ["1"], ("mentor_id",), ensure_fields=False)["1"]["mentor_id"] == "new"
        for table, fields in (("missing", ("mentor_id",)), ("mentor", ("missing_column",))):
            assert manager.get_fields_by_ids(table, ["1"], fields, ensure_fields=False) == {}
        manager.conn._raw.set_authorizer(
            lambda action, *_: db_backend.sqlite3.SQLITE_DENY
            if action == db_backend.sqlite3.SQLITE_READ else db_backend.sqlite3.SQLITE_OK
        )
        with pytest.raises(db_backend.sqlite3.DatabaseError):
            manager.get_fields_by_ids("mentor", ["1"], ("mentor_id",), ensure_fields=False)
        manager.conn._raw.set_authorizer(None)
        assert manager.get_fields_by_ids("mentor", ["1"], ("mentor_id",), ensure_fields=False)["1"]["mentor_id"] == "new"
    finally:
        manager.conn.close()
