import asyncio
from collections import Counter
import itertools
import sqlite3
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from ....infrastructure.database import DatabaseUnitOfWork
from ....plugin import apply_platform_schema
from ...combat_settlement.application import CombatSettlementApplication
from ...combat_settlement.migrations import apply_dao_battle_operations, apply_dao_battle_record
from ..application import MapApplication
from .. import random_target_query, random_target_repository
from ..random_target_query import select_random_nearby_target
from ..random_target_repository import MapRandomTargetSqlQueryRepository
from .test_map_named_target_query import POSITION, load_handler, nearby


def repository_for(nearby):
    return MapRandomTargetSqlQueryRepository(nearby[1], nearby[2])


def choose(repository, source=None, **kwargs):
    source = source or SimpleNamespace(randint=Mock(side_effect=lambda low, high: high))
    return asyncio.run(select_random_nearby_target(
        repository, **(POSITION | kwargs), exclude_user_id="me", random_source=source,
    ))


def pairs(repository):
    upper = repository.upper_rowids()
    if upper is None:
        return []
    found, after = [], None
    while rows := repository.page(after, upper, tuple(POSITION.values()), "me"):
        assert len(rows) <= random_target_repository.CANDIDATE_PAGE_SIZE
        assert all(set(row) == {"map_cursor", "profile_cursor"} for row in rows)
        found.extend((row["map_cursor"], row["profile_cursor"]) for row in rows)
        after = found[-1]
    return found


def track_connections(monkeypatch):
    connections = []
    original = DatabaseUnitOfWork.__enter__

    def enter(uow):
        result = original(uow)
        assert uow.read_only
        connections.append(uow.connection)
        return result

    monkeypatch.setattr(DatabaseUnitOfWork, "__enter__", enter)
    return connections


def assert_closed(connections):
    assert connections
    for connection in connections:
        with pytest.raises(sqlite3.ProgrammingError, match="closed"):
            connection.execute("SELECT 1")


def test_application_random_target_reads_a_single_observed_stream(nearby):
    _, player, game = nearby
    source = SimpleNamespace(randint=lambda low, high: high)
    target = asyncio.run(MapApplication(game, player).random_nearby_target(
        **POSITION, exclude_user_id="me", random_source=source,
    ))
    assert target == {"user_id": "first", "user_name": "target", "level": "first-level", "power": 20}


@pytest.mark.parametrize("target", (None, {"user_id": "first", "user_name": "target", "power": 20}))
def test_random_handler_awaits_application_and_never_loads_full_list(target):
    env = load_handler("dao_qc", None)
    query = AsyncMock(return_value=target)
    env["map_application"].random_nearby_target = query
    asyncio.run(env["_"]("bot", object(), SimpleNamespace(extract_plain_text=lambda: "")))
    query.assert_awaited_once_with(**POSITION, exclude_user_id="me", random_source=env["runtime_random"])
    env["_get_all_in_same_node"].assert_not_called()
    env["map_application"].nearby_target.assert_not_called()
    if target is None:
        env["dao_battle_application"].settle_dao_battle.assert_not_called()
        assert env["handle_send"].await_args.args[2] == "附近无可切磋道友。"
    else:
        assert env["dao_battle_application"].settle_dao_battle.call_args.kwargs["target_id"] == "first"


def test_application_awaits_feature_selector_with_explicit_random_source(nearby, monkeypatch):
    source = object()
    selected = {"user_id": "first"}
    query = AsyncMock(return_value=selected)
    monkeypatch.setattr("nonebot_plugin_xiuxian_2.features.map.application.select_random_nearby_target", query)
    assert asyncio.run(MapApplication(nearby[2], nearby[1]).random_nearby_target(
        **POSITION, exclude_user_id="me", random_source=source,
    )) is selected
    args = query.await_args
    assert isinstance(args.args[0], MapRandomTargetSqlQueryRepository)
    assert args.kwargs == POSITION | {"exclude_user_id": "me", "random_source": source}


@pytest.mark.parametrize("field", ("realm", "heaven", "node_id"))
def test_random_selection_checks_each_position_dimension(nearby, field):
    with DatabaseUnitOfWork(nearby[1]) as uow:
        uow.execute(f"UPDATE map_status SET {field}='other' WHERE user_id='first'")
    assert choose(repository_for(nearby))["user_id"] == "second"


@pytest.mark.parametrize("field", ("realm", "heaven", "node_id"))
@pytest.mark.parametrize("value", (None, "", 0))
def test_invalid_position_never_opens_a_database(field, value):
    repository = Mock()
    assert choose(repository, **{field: value}) is None
    repository.upper_rowids.assert_not_called()


@pytest.mark.parametrize("table", ("map_status", "user_xiuxian"))
def test_empty_table_has_no_boundaries_or_target(nearby, table):
    with DatabaseUnitOfWork(nearby[1 if table == "map_status" else 2]) as uow:
        uow.execute(f"DELETE FROM {table}")
    repository = repository_for(nearby)
    assert repository.upper_rowids() is None
    assert choose(repository) is None


def test_self_and_orphan_rows_do_not_count_as_candidates(nearby):
    with DatabaseUnitOfWork(nearby[2]) as uow:
        uow.execute("DELETE FROM user_xiuxian WHERE user_id<>'me'")
    source = SimpleNamespace(randint=Mock())
    assert choose(repository_for(nearby), source) is None
    source.randint.assert_not_called()


def test_duplicate_map_profile_pairs_keep_their_weight_across_pages(nearby, monkeypatch):
    with DatabaseUnitOfWork(nearby[1]) as uow:
        uow.execute("INSERT INTO map_status VALUES ('first','realm','heaven','node')")
    with DatabaseUnitOfWork(nearby[2]) as uow:
        uow.execute("INSERT INTO user_xiuxian VALUES ('first','duplicate','level',40)")
    monkeypatch.setattr(random_target_repository, "CANDIDATE_PAGE_SIZE", 1)
    repository = repository_for(nearby)
    assert pairs(repository) == [(2, 2), (2, 4), (3, 3), (5, 2), (5, 4)]
    source = SimpleNamespace(randint=Mock(return_value=1))
    assert choose(repository, source)["user_name"] == "duplicate"
    assert source.randint.call_args_list == [((1, n),) for n in range(1, 6)]


class MemoryStream:
    def __init__(self, count):
        self.count = count
        self.observed = 0

    def upper_rowids(self):
        return 0, self.count

    def page(self, after, upper, position, exclude):
        start = 0 if after is None else after[1]
        return [{"map_cursor": 0, "profile_cursor": n}
                for n in range(start + 1, min(start + 256, self.count) + 1)]

    def candidate(self, cursor, upper, position, exclude):
        self.observed += 1
        return {"user_id": str(cursor[1])}


def test_three_candidate_reservoir_is_exactly_fair():
    counts = Counter()
    for second, third in itertools.product(range(1, 3), range(1, 4)):
        source = SimpleNamespace(randint=Mock(side_effect=[1, second, third]))
        counts[choose(MemoryStream(3), source)["user_id"]] += 1
        assert source.randint.call_args_list == [((1, 1),), ((1, 2),), ((1, 3),)]
    assert counts == {"1": 2, "2": 2, "3": 2}


def test_negative_zero_and_all_negative_rowids_are_not_skipped(nearby, monkeypatch):
    with DatabaseUnitOfWork(nearby[1]) as uow:
        uow.execute("DELETE FROM map_status")
        uow.executemany("INSERT INTO map_status(rowid,user_id,realm,heaven,node_id) VALUES (?,?,'realm','heaven','node')",
                        [(-4, "first"), (0, "second")])
    with DatabaseUnitOfWork(nearby[2]) as uow:
        uow.execute("DELETE FROM user_xiuxian")
        uow.executemany("INSERT INTO user_xiuxian(rowid,user_id,user_name,level,power) VALUES (?,?,?,'level',1)",
                        [(-8, "first", "first"), (0, "second", "second")])
    monkeypatch.setattr(random_target_repository, "CANDIDATE_PAGE_SIZE", 1)
    repository = repository_for(nearby)
    assert pairs(repository) == [(-4, -8), (0, 0)]
    assert choose(repository)["user_id"] == "first"
    with DatabaseUnitOfWork(nearby[1]) as uow:
        uow.execute("DELETE FROM map_status WHERE rowid=0")
    with DatabaseUnitOfWork(nearby[2]) as uow:
        uow.execute("DELETE FROM user_xiuxian WHERE rowid=0")
    assert repository.upper_rowids() == (-4, -8)
    assert choose(repository)["user_id"] == "first"


def test_late_page_candidate_is_observed_without_loading_large_fields_in_pages(nearby, monkeypatch):
    with DatabaseUnitOfWork(nearby[2]) as uow:
        uow.execute("DELETE FROM user_xiuxian WHERE user_id='second'")
        uow.executemany("INSERT INTO user_xiuxian VALUES ('first',?,'level',1)",
                        [(str(n),) for n in range(300)])
    original = DatabaseUnitOfWork.query_all
    page_lengths = []

    def bounded(uow, sql, params=()):
        assert "user_name" not in sql and "power" not in sql
        assert sql.endswith("LIMIT ?") and params[-1] == 256
        rows = original(uow, sql, params)
        assert all(set(row) == {"map_cursor", "profile_cursor"} for row in rows)
        page_lengths.append(len(rows))
        return rows

    monkeypatch.setattr(DatabaseUnitOfWork, "query_all", bounded)
    source = SimpleNamespace(randint=Mock(return_value=1))
    assert choose(repository_for(nearby), source)["user_name"] == "299"
    assert page_lengths == [256, 45, 0]
    assert source.randint.call_args.args == (1, 301)


def test_double_high_water_excludes_later_map_and_profile_appends(nearby, monkeypatch):
    repository = repository_for(nearby)
    original = repository.page
    monkeypatch.setattr(random_target_repository, "CANDIDATE_PAGE_SIZE", 1)
    observed_bounds = []

    def page(after, upper, position, exclude):
        observed_bounds.append(upper)
        if after is not None and len(observed_bounds) == 2:
            with DatabaseUnitOfWork(nearby[1]) as uow:
                uow.execute("INSERT INTO map_status VALUES ('first','realm','heaven','node')")
            with DatabaseUnitOfWork(nearby[2]) as uow:
                uow.execute("INSERT INTO user_xiuxian VALUES ('second','new','level',40)")
                uow.execute("INSERT INTO user_xiuxian VALUES ('missing','new','level',50)")
        return original(after, upper, position, exclude)

    monkeypatch.setattr(repository, "page", page)
    source = SimpleNamespace(randint=Mock(return_value=1))
    assert choose(repository, source)["user_id"] == "second"
    assert observed_bounds == [(4, 3)] * 3
    assert source.randint.call_args_list == [((1, 1),), ((1, 2),)]


@pytest.mark.parametrize("mutation", (
    "DELETE FROM map_status WHERE rowid=2",
    "UPDATE map_status SET realm='other' WHERE rowid=2",
    "UPDATE map_status SET heaven='other' WHERE rowid=2",
    "UPDATE map_status SET node_id='other' WHERE rowid=2",
    "UPDATE map_status SET user_id='me' WHERE rowid=2",
    "UPDATE map_status SET user_id='second' WHERE rowid=2",
    "DELETE FROM user_xiuxian WHERE rowid=2",
    "UPDATE user_xiuxian SET user_id='second' WHERE rowid=2",
))
def test_exact_pair_rechecks_eligibility_after_metadata_read(nearby, monkeypatch, mutation):
    repository = repository_for(nearby)
    original = repository.candidate

    def candidate(cursor, upper, position, exclude):
        if cursor == (2, 2):
            database = nearby[2 if "user_xiuxian" in mutation else 1]
            with DatabaseUnitOfWork(database) as uow:
                uow.execute(mutation)
        return original(cursor, upper, position, exclude)

    monkeypatch.setattr(repository, "candidate", candidate)
    source = SimpleNamespace(randint=Mock(return_value=1))
    assert choose(repository, source)["user_id"] == "second"
    assert source.randint.call_args_list == [((1, 1),)]


def test_hole_insert_and_rowid_reuse_follow_cursor_not_global_snapshot(nearby, monkeypatch):
    repository = repository_for(nearby)
    original = repository.page
    monkeypatch.setattr(random_target_repository, "CANDIDATE_PAGE_SIZE", 1)
    changed = False

    def page(after, upper, position, exclude):
        nonlocal changed
        if after is not None and not changed:
            changed = True
            with DatabaseUnitOfWork(nearby[1]) as uow:
                uow.execute("DELETE FROM map_status WHERE rowid IN (1,3)")
                uow.executemany("INSERT INTO map_status(rowid,user_id,realm,heaven,node_id) VALUES (?,'first','realm','heaven','node')",
                                [(1,), (3,)])
            with DatabaseUnitOfWork(nearby[2]) as uow:
                uow.execute("DELETE FROM user_xiuxian WHERE rowid=1")
                uow.execute("INSERT INTO user_xiuxian(rowid,user_id,user_name,level,power) VALUES (1,'first','reused','level',9)")
        return original(after, upper, position, exclude)

    monkeypatch.setattr(repository, "page", page)
    source = SimpleNamespace(randint=Mock(return_value=1))
    assert choose(repository, source)["user_name"] == "target"
    # (1,*) and (2,1) are behind the cursor; reused (3,1)/(3,2) are observed.
    assert source.randint.call_args_list == [((1, n),) for n in range(1, 4)]


def test_public_fields_use_current_values_null_defaults_and_python_big_int(nearby, monkeypatch):
    repository = repository_for(nearby)
    original = repository.candidate
    large = 10 ** 50

    def candidate(cursor, upper, position, exclude):
        if cursor == (2, 2):
            with DatabaseUnitOfWork(nearby[2]) as uow:
                uow.execute("UPDATE user_xiuxian SET user_name=NULL,level=NULL,power=? WHERE rowid=2", (str(large),))
        return original(cursor, upper, position, exclude)

    # A TEXT power column preserves integers beyond SQLite's numeric range.
    with DatabaseUnitOfWork(nearby[2]) as uow:
        rows = uow.query_all("SELECT user_id,user_name,level,power FROM user_xiuxian")
        uow.execute("DROP TABLE user_xiuxian")
        uow.execute("CREATE TABLE user_xiuxian (user_id TEXT,user_name TEXT,level TEXT,power TEXT)")
        uow.executemany("INSERT INTO user_xiuxian VALUES (?,?,?,?)", [tuple(row.values()) for row in rows])
    monkeypatch.setattr(repository, "candidate", candidate)
    assert choose(repository) == {"user_id": "first", "user_name": "", "level": "", "power": large}


def test_cast_join_keeps_map_side_id_and_binary_self_exclusion(nearby):
    with DatabaseUnitOfWork(nearby[1]) as uow:
        uow.execute("DROP TABLE map_status")
        uow.execute("CREATE TABLE map_status (user_id COLLATE NOCASE,realm TEXT,heaven TEXT,node_id TEXT)")
        uow.executemany("INSERT INTO map_status VALUES (?,'realm','heaven','node')", [(42,), ("ME",)])
    with DatabaseUnitOfWork(nearby[2]) as uow:
        uow.execute("INSERT INTO user_xiuxian VALUES ('42','numeric','level',5)")
        uow.execute("INSERT INTO user_xiuxian VALUES ('ME','uppercase','level',6)")
    source = SimpleNamespace(randint=Mock(return_value=1))
    assert choose(repository_for(nearby), source)["user_id"] == "ME"
    assert source.randint.call_args_list == [((1, 1),), ((1, 2),)]


@pytest.mark.parametrize("missing", ("player", "game", "both"))
def test_missing_database_is_not_created(tmp_path, missing):
    player, game = tmp_path / "player.db", tmp_path / "game.db"
    if missing != "player" and missing != "both":
        with sqlite3.connect(player) as connection:
            connection.execute("CREATE TABLE map_status(user_id,realm,heaven,node_id)")
    if missing != "game" and missing != "both":
        with sqlite3.connect(game) as connection:
            connection.execute("CREATE TABLE user_xiuxian(user_id,user_name,level,power)")
    before = {path.name: path.read_bytes() for path in tmp_path.iterdir()}
    assert choose(MapRandomTargetSqlQueryRepository(player, game)) is None
    assert {path.name: path.read_bytes() for path in tmp_path.iterdir()} == before


@pytest.mark.parametrize("table,column", (
    ("map_status", None), ("user_xiuxian", None),
    ("map_status", "user_id"), ("map_status", "realm"), ("map_status", "heaven"), ("map_status", "node_id"),
    ("user_xiuxian", "user_id"), ("user_xiuxian", "user_name"), ("user_xiuxian", "level"), ("user_xiuxian", "power"),
))
def test_missing_schema_fails_closed_without_repair(nearby, monkeypatch, table, column):
    path = nearby[1 if table == "map_status" else 2]
    with DatabaseUnitOfWork(path) as uow:
        if column is None:
            uow.execute(f"DROP TABLE {table}")
        else:
            uow.execute(f"ALTER TABLE {table} RENAME COLUMN {column} TO absent")
    connections = track_connections(monkeypatch)
    assert choose(repository_for(nearby)) is None
    assert_closed(connections)


def test_bad_attached_database_closes_main_connection(nearby, monkeypatch):
    # A test-owned map database is a valid SQLite file but an invalid profile schema.
    repository = MapRandomTargetSqlQueryRepository(nearby[1], nearby[1])
    connections = track_connections(monkeypatch)
    assert choose(repository) is None
    assert_closed(connections)


def test_corrupt_attached_file_fails_closed_without_modifying_it(nearby, tmp_path, monkeypatch):
    corrupt = tmp_path / "corrupt.db"
    corrupt.write_bytes(b"not a SQLite database")
    connections = track_connections(monkeypatch)
    assert choose(MapRandomTargetSqlQueryRepository(nearby[1], corrupt)) is None
    assert corrupt.read_bytes() == b"not a SQLite database"
    assert_closed(connections)


@pytest.mark.parametrize("stage", ("boundary", "attach", "page", "candidate"))
def test_read_failure_closes_connections_and_discards_partial_sample(nearby, monkeypatch, stage):
    connections = track_connections(monkeypatch)
    original = DatabaseUnitOfWork.execute
    counts = Counter()
    monkeypatch.setattr(random_target_repository, "CANDIDATE_PAGE_SIZE", 1)

    def execute(uow, sql, params=()):
        kind = ("attach" if sql.startswith("ATTACH") else "boundary" if "MAX(rowid)" in sql
                else "page" if sql.startswith("SELECT map.rowid")
                else "candidate" if sql.startswith("SELECT map.user_id") else "other")
        counts[kind] += 1
        trigger = 1 if stage == "boundary" else 4 if stage == "attach" else 2
        if kind == stage and counts[kind] == trigger:
            raise sqlite3.OperationalError("injected read failure")
        return original(uow, sql, params)

    monkeypatch.setattr(DatabaseUnitOfWork, "execute", execute)
    source = SimpleNamespace(randint=Mock(return_value=1))
    assert choose(repository_for(nearby), source) is None
    assert_closed(connections)
    if stage != "boundary":
        source.randint.assert_called_once_with(1, 1)


def test_late_bad_power_discards_earlier_sample_and_closes_connections(nearby, monkeypatch):
    with DatabaseUnitOfWork(nearby[2]) as uow:
        uow.execute("UPDATE user_xiuxian SET power='invalid' WHERE user_id='second'")
    connections = track_connections(monkeypatch)
    source = SimpleNamespace(randint=Mock(return_value=1))
    assert choose(repository_for(nearby), source) is None
    source.randint.assert_called_once_with(1, 1)
    assert_closed(connections)


@pytest.mark.parametrize("bad", (
    {"map_cursor": 5, "profile_cursor": 3}, {"map_cursor": 3, "profile_cursor": 4},
    {"map_cursor": 2, "profile_cursor": 2}, {"map_cursor": 1, "profile_cursor": 3},
    {"map_cursor": "invalid", "profile_cursor": 3}, {},
))
def test_invalid_cursor_discards_partial_sample(nearby, monkeypatch, bad):
    repository = repository_for(nearby)
    monkeypatch.setattr(repository, "page", Mock(return_value=[{"map_cursor": 2, "profile_cursor": 2}, bad]))
    assert choose(repository) is None


def test_oversized_page_fails_closed():
    repository = Mock(upper_rowids=Mock(return_value=(1, 1)), page=Mock(return_value=[{}] * 257))
    assert choose(repository) is None
    repository.candidate.assert_not_called()


def test_yields_every_32_raw_rows_and_at_page_end_including_ineligible(monkeypatch):
    repository = MemoryStream(300)
    original = repository.candidate
    yields = []

    def candidate(*args):
        original(*args)
        return None

    async def sleep(delay):
        assert delay == 0
        yields.append(repository.observed)

    monkeypatch.setattr(repository, "candidate", candidate)
    monkeypatch.setattr(random_target_query.asyncio, "sleep", sleep)
    source = SimpleNamespace(randint=Mock())
    assert choose(repository, source) is None
    assert yields == [32, 64, 96, 128, 160, 192, 224, 256, 256, 288, 300]
    source.randint.assert_not_called()


@pytest.mark.parametrize("cancel", (False, True))
def test_all_connections_close_before_yield_and_cancellation_propagates(nearby, monkeypatch, cancel):
    connections = track_connections(monkeypatch)
    yielded = []

    async def sleep(delay):
        assert_closed(connections)
        yielded.append(delay)
        if cancel:
            raise asyncio.CancelledError()

    monkeypatch.setattr(random_target_query.asyncio, "sleep", sleep)
    if cancel:
        with pytest.raises(asyncio.CancelledError):
            choose(repository_for(nearby))
    else:
        assert choose(repository_for(nearby))["user_id"] == "first"
    assert yielded == [0]
    assert_closed(connections)


def test_mid_page_cancellation_stops_at_32_and_closes_every_connection(nearby, monkeypatch):
    with DatabaseUnitOfWork(nearby[2]) as uow:
        uow.executemany("INSERT INTO user_xiuxian VALUES ('first',?,'level',1)",
                        [(str(n),) for n in range(40)])
    connections = track_connections(monkeypatch)
    source = SimpleNamespace(randint=Mock(return_value=1))

    async def sleep(delay):
        assert_closed(connections)
        assert source.randint.call_count == 32
        raise asyncio.CancelledError()

    monkeypatch.setattr(random_target_query.asyncio, "sleep", sleep)
    with pytest.raises(asyncio.CancelledError):
        choose(repository_for(nearby), source)
    assert_closed(connections)


def test_cancelled_read_is_not_wrapped_as_no_candidate(nearby, monkeypatch):
    connections = track_connections(monkeypatch)
    original = DatabaseUnitOfWork.execute

    def execute(uow, sql, params=()):
        if sql.startswith("SELECT map.user_id"):
            raise asyncio.CancelledError()
        return original(uow, sql, params)

    monkeypatch.setattr(DatabaseUnitOfWork, "execute", execute)
    with pytest.raises(asyncio.CancelledError):
        choose(repository_for(nearby))
    assert_closed(connections)


def test_reads_are_read_only_uri_attached_and_have_no_request_ddl(nearby, monkeypatch):
    player, game = nearby[1].with_name("player with space.db"), nearby[2].with_name("game with space.db")
    nearby[1].rename(player)
    nearby[2].rename(game)
    connections = track_connections(monkeypatch)
    original = DatabaseUnitOfWork.execute
    statements = []

    def execute(uow, sql, params=()):
        statements.append((sql, params))
        return original(uow, sql, params)

    monkeypatch.setattr(DatabaseUnitOfWork, "execute", execute)
    assert choose(MapRandomTargetSqlQueryRepository(player, game))["user_id"] == "first"
    assert_closed(connections)
    uris = [params[0] for sql, params in statements if sql.startswith("ATTACH")]
    assert uris and all(uri.endswith("?mode=ro") and "%20" in uri for uri in uris)
    assert all(sql.endswith("LIMIT 1") for sql, _ in statements if sql.startswith("SELECT map.user_id"))
    assert not any(sql.startswith(("CREATE", "ALTER", "INSERT", "UPDATE", "DELETE")) for sql, _ in statements)


@pytest.mark.parametrize("drift", (False, True))
def test_random_handler_real_selection_settlement_replay_and_position_recheck(nearby, drift):
    _, player, game = nearby
    with DatabaseUnitOfWork(player) as uow:
        apply_platform_schema(uow)
        apply_dao_battle_operations(uow)
        apply_dao_battle_record(uow)
    env = load_handler("dao_qc", None)
    application = MapApplication(game, player)
    env["map_application"] = application
    env["runtime_random"].randint = Mock(side_effect=lambda low, high: high)
    env["dao_battle_application"] = CombatSettlementApplication(game, player)
    if drift:
        query = application.random_nearby_target

        async def moved(**kwargs):
            result = await query(**kwargs)
            with DatabaseUnitOfWork(player) as uow:
                uow.execute("UPDATE map_status SET node_id='elsewhere' WHERE user_id='first'")
            return result

        application.random_nearby_target = moved
    arguments = ("bot", object(), SimpleNamespace(extract_plain_text=lambda: ""))
    asyncio.run(env["_"](*arguments))
    if not drift:
        asyncio.run(env["_"](*arguments))
    with DatabaseUnitOfWork(player, read_only=True) as uow:
        records = uow.query_all("SELECT user_id,total FROM dao_record ORDER BY user_id")
        operations = uow.query_one("SELECT COUNT(*) AS count FROM map_dao_battle_operations")
    if drift:
        assert not records and operations["count"] == 0
        assert env["handle_send"].await_args.args[2] == "对方已离开当前节点，论道未结算。"
    else:
        assert records == [{"user_id": "first", "total": 1}, {"user_id": "me", "total": 1}]
        assert operations["count"] == 1
