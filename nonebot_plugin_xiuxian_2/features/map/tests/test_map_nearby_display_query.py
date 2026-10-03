import asyncio
from collections import Counter
import itertools
import sqlite3
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from ....infrastructure.database import DatabaseUnitOfWork
from ..application import MapApplication
from .. import nearby_display_query
from ..nearby_display_query import select_nearby_display
from ..nearby_display_repository import MapNearbyDisplaySqlQueryRepository
from .test_map_named_target_query import POSITION, load_handler, nearby
from .test_map_random_target_query import assert_closed, track_connections


def repository_for(nearby):
    return MapNearbyDisplaySqlQueryRepository(nearby[1], nearby[2])


def display(repository, source=None, **kwargs):
    source = source or SimpleNamespace(
        randint=Mock(side_effect=lambda low, high: high),
        sample=Mock(side_effect=lambda items, count: list(reversed(items))),
    )
    return asyncio.run(select_nearby_display(
        repository, **(POSITION | kwargs), exclude_user_id="me", random_source=source,
    ))


def metadata(repository):
    upper = repository.upper_rowids()
    if upper is None:
        return []
    found, after = [], None
    while rows := repository.page(after, upper, tuple(POSITION.values()), "me"):
        assert len(rows) == 1
        assert set(rows[0]) == {"user_cursor", "map_cursor", "profile_cursor"}
        found.extend(rows)
        after = rows[-1]["user_cursor"]
    return found


def add_users(nearby, count):
    ids = [f"u{number:03d}" for number in range(count)]
    with DatabaseUnitOfWork(nearby[1]) as uow:
        uow.executemany("INSERT INTO map_status VALUES (?,'realm','heaven','node')", [(uid,) for uid in ids])
    with DatabaseUnitOfWork(nearby[2]) as uow:
        uow.executemany("INSERT INTO user_xiuxian VALUES (?,?,'level',1)", [(uid, uid) for uid in ids])
    return ids


def test_application_nearby_display_returns_unique_users_without_full_list(nearby):
    _, player, game = nearby
    source = SimpleNamespace(randint=Mock(), sample=Mock())
    result = asyncio.run(MapApplication(game, player).nearby_display(
        **POSITION, exclude_user_id="me", random_source=source,
    ))
    assert [user["user_id"] for user in result] == ["first", "second"]
    source.randint.assert_not_called()
    source.sample.assert_not_called()


@pytest.mark.parametrize("users", ([], [{"user_id": "first", "user_name": "target", "level": "level"}]))
def test_display_handler_awaits_bounded_application_and_preserves_messages(users):
    env = load_handler("nearby_users_cmd", None)
    query = AsyncMock(return_value=users)
    env["map_application"].nearby_display = query
    asyncio.run(env["_"]("bot", object()))
    query.assert_awaited_once_with(**POSITION, exclude_user_id="me", random_source=env["runtime_random"])
    env["_get_all_in_same_node"].assert_not_called()
    assert env["handle_send"].await_args.args[2] == (
        "【附近道友】\n- target（level）" if users else "附近暂无其他道友。"
    )


def test_application_awaits_feature_selector_with_injected_rng(nearby, monkeypatch):
    source, result = object(), [{"user_id": "first"}]
    query = AsyncMock(return_value=result)
    monkeypatch.setattr("nonebot_plugin_xiuxian_2.features.map.application.select_nearby_display", query)
    assert asyncio.run(MapApplication(nearby[2], nearby[1]).nearby_display(
        **POSITION, exclude_user_id="me", random_source=source,
    )) is result
    assert isinstance(query.await_args.args[0], MapNearbyDisplaySqlQueryRepository)
    assert query.await_args.kwargs == POSITION | {"exclude_user_id": "me", "random_source": source}


def test_duplicate_map_and_profile_rows_use_first_matching_pair_without_weight(nearby):
    with DatabaseUnitOfWork(nearby[1]) as uow:
        uow.executemany("INSERT INTO map_status VALUES ('first','realm','heaven','node')", [()] * 8)
    with DatabaseUnitOfWork(nearby[2]) as uow:
        uow.executemany("INSERT INTO user_xiuxian VALUES ('first','duplicate','later',999)", [()] * 20)
    repository = repository_for(nearby)
    assert [(row["user_cursor"], row["map_cursor"], row["profile_cursor"]) for row in metadata(repository)] == [
        ("first", 2, 2), ("second", 3, 3),
    ]
    assert display(repository) == [
        {"user_id": "first", "user_name": "target", "level": "first-level", "power": 20},
        {"user_id": "second", "user_name": "target", "level": "second-level", "power": 30},
    ]


def test_earlier_map_row_at_another_node_does_not_hide_current_canonical_row(nearby):
    with DatabaseUnitOfWork(nearby[1]) as uow:
        uow.execute("UPDATE map_status SET node_id='other' WHERE user_id='first'")
        uow.execute("INSERT INTO map_status VALUES ('first','realm','heaven','node')")
    assert [user["user_id"] for user in display(repository_for(nearby))] == ["second", "first"]


@pytest.mark.parametrize("field", ("realm", "heaven", "node_id"))
def test_each_position_dimension_filters_both_canonical_map_and_candidate(nearby, field):
    with DatabaseUnitOfWork(nearby[1]) as uow:
        uow.execute(f"UPDATE map_status SET {field}='other' WHERE user_id='first'")
        uow.execute("INSERT INTO map_status VALUES ('first','realm','heaven','node')")
    assert [user["user_id"] for user in display(repository_for(nearby))] == ["second", "first"]


@pytest.mark.parametrize("field", ("realm", "heaven", "node_id"))
@pytest.mark.parametrize("value", (None, "", 0))
def test_incomplete_position_does_not_open_a_database(field, value):
    repository = Mock()
    assert display(repository, **{field: value}) == []
    repository.upper_rowids.assert_not_called()


@pytest.mark.parametrize("table", ("map_status", "user_xiuxian"))
def test_empty_table_and_missing_profile_produce_no_display(nearby, table):
    with DatabaseUnitOfWork(nearby[1 if table == "map_status" else 2]) as uow:
        uow.execute(f"DELETE FROM {table}")
    assert display(repository_for(nearby)) == []


def test_self_and_orphan_map_rows_are_not_counted(nearby):
    with DatabaseUnitOfWork(nearby[2]) as uow:
        uow.execute("DELETE FROM user_xiuxian WHERE user_id<>'me'")
    source = SimpleNamespace(randint=Mock(), sample=Mock())
    assert display(repository_for(nearby), source) == []
    source.randint.assert_not_called()
    source.sample.assert_not_called()


def test_at_most_ten_restores_map_order_instead_of_uid_scan_order(nearby):
    with DatabaseUnitOfWork(nearby[1]) as uow:
        uow.execute("UPDATE map_status SET user_id='z' WHERE user_id='first'")
        uow.execute("UPDATE map_status SET user_id='a' WHERE user_id='second'")
    with DatabaseUnitOfWork(nearby[2]) as uow:
        uow.execute("UPDATE user_xiuxian SET user_id='z' WHERE user_id='first'")
        uow.execute("UPDATE user_xiuxian SET user_id='a' WHERE user_id='second'")
    repository = repository_for(nearby)
    assert [row["user_cursor"] for row in metadata(repository)] == ["a", "z"]
    assert [user["user_id"] for user in display(repository)] == ["z", "a"]


def test_more_than_ten_samples_unique_ids_once_and_randomizes_display_order(nearby):
    ids = add_users(nearby, 10)
    with DatabaseUnitOfWork(nearby[1]) as uow:
        uow.executemany("INSERT INTO map_status VALUES ('first','realm','heaven','node')", [()] * 8)
    with DatabaseUnitOfWork(nearby[2]) as uow:
        uow.executemany("INSERT INTO user_xiuxian VALUES ('first','duplicate','later',999)", [()] * 20)
    source = SimpleNamespace(randint=Mock(side_effect=lambda low, high: high),
                             sample=Mock(side_effect=lambda items, count: list(reversed(items))))
    users = display(repository_for(nearby), source)
    assert [user["user_id"] for user in users] == list(reversed(["first", "second", *ids[:8]]))
    assert len({user["user_id"] for user in users}) == len(users) == 10
    assert source.randint.call_args_list == [((1, 11),), ((1, 12),)]
    source.sample.assert_called_once()
    assert len(source.sample.call_args.args[0]) == source.sample.call_args.args[1] == 10


class MemoryDisplay:
    def __init__(self, count):
        self.count = count
        self.observed = 0

    def upper_rowids(self):
        return self.count, self.count

    def page(self, after, upper, position, exclude):
        number = 1 if after is None else int(after) + 1
        if number > self.count:
            return []
        return [{"user_cursor": f"{number:04d}", "map_cursor": number, "profile_cursor": number}]

    def candidate(self, cursor, upper, position, exclude, *, expected_user_id):
        self.observed += 1
        return {"user_id": expected_user_id}


def test_ten_of_twelve_reservoir_subsets_are_exactly_uniform():
    subsets = Counter()
    for eleventh, twelfth in itertools.product(range(1, 12), range(1, 13)):
        source = SimpleNamespace(randint=Mock(side_effect=[eleventh, twelfth]),
                                 sample=Mock(side_effect=lambda items, count: list(items)))
        result = display(MemoryDisplay(12), source)
        subsets[tuple(sorted(user["user_id"] for user in result))] += 1
    assert len(subsets) == 66
    assert set(subsets.values()) == {2}


@pytest.mark.parametrize("count", (0, 1, 10, 11, 500))
def test_retained_sample_and_shuffle_input_never_exceed_ten(count):
    sizes = []

    def sample(items, size):
        sizes.append((len(items), size))
        return list(items)

    source = SimpleNamespace(randint=Mock(return_value=1), sample=Mock(side_effect=sample))
    result = display(MemoryDisplay(count), source)
    assert len(result) == min(count, 10)
    assert len({user["user_id"] for user in result}) == len(result)
    assert sizes == ([(10, 10)] if count > 10 else [])
    assert source.randint.call_count == max(count - 10, 0)


def test_negative_zero_rowids_and_all_negative_high_water_preserve_order(nearby):
    with DatabaseUnitOfWork(nearby[1]) as uow:
        uow.execute("DELETE FROM map_status")
        uow.executemany("INSERT INTO map_status(rowid,user_id,realm,heaven,node_id) VALUES (?,?,'realm','heaven','node')",
                        [(-4, "z"), (0, "a")])
    with DatabaseUnitOfWork(nearby[2]) as uow:
        uow.execute("DELETE FROM user_xiuxian")
        uow.executemany("INSERT INTO user_xiuxian(rowid,user_id,user_name,level,power) VALUES (?,?,?,'level',1)",
                        [(-8, "z", "z"), (0, "a", "a")])
    repository = repository_for(nearby)
    assert [user["user_id"] for user in display(repository)] == ["z", "a"]
    with DatabaseUnitOfWork(nearby[1]) as uow:
        uow.execute("DELETE FROM map_status WHERE rowid=0")
    with DatabaseUnitOfWork(nearby[2]) as uow:
        uow.execute("DELETE FROM user_xiuxian WHERE rowid=0")
    assert repository.upper_rowids() == (-4, -8)
    assert [user["user_id"] for user in display(repository)] == ["z"]


def test_cast_ids_deduplicate_binary_strings_and_exclude_only_exact_self(nearby):
    with DatabaseUnitOfWork(nearby[1]) as uow:
        uow.execute("DROP TABLE map_status")
        uow.execute("CREATE TABLE map_status(user_id COLLATE NOCASE,realm,heaven,node_id)")
        uow.executemany("INSERT INTO map_status VALUES (?,'realm','heaven','node')",
                        [(42,), ("42",), ("me",), ("ME",)])
    with DatabaseUnitOfWork(nearby[2]) as uow:
        uow.execute("INSERT INTO user_xiuxian VALUES ('42','numeric','level',5)")
        uow.execute("INSERT INTO user_xiuxian VALUES ('ME','uppercase','level',6)")
    assert [user["user_id"] for user in display(repository_for(nearby))] == ["42", "ME"]


def test_empty_and_quoted_identifiers_seek_without_losing_or_interpolating_them(nearby):
    name = "' OR 1=1 --"
    with DatabaseUnitOfWork(nearby[1]) as uow:
        uow.execute("UPDATE map_status SET user_id='' WHERE user_id='first'")
        uow.execute("UPDATE map_status SET user_id=? WHERE user_id='second'", (name,))
    with DatabaseUnitOfWork(nearby[2]) as uow:
        uow.execute("UPDATE user_xiuxian SET user_id='' WHERE user_id='first'")
        uow.execute("UPDATE user_xiuxian SET user_id=? WHERE user_id='second'", (name,))
    assert [user["user_id"] for user in display(repository_for(nearby))] == ["", name]


def test_zero_integer_uid_is_kept_and_deduplicated_with_string_zero(nearby):
    with DatabaseUnitOfWork(nearby[1]) as uow:
        uow.execute("DROP TABLE map_status")
        uow.execute("CREATE TABLE map_status(user_id,realm,heaven,node_id)")
        uow.executemany("INSERT INTO map_status VALUES (?,'realm','heaven','node')", [(0,), ("0",)])
    with DatabaseUnitOfWork(nearby[2]) as uow:
        uow.execute("INSERT INTO user_xiuxian VALUES ('0','zero','level',0)")
    assert display(repository_for(nearby)) == [{"user_id": "0", "user_name": "zero", "level": "level", "power": 0}]


def test_bad_power_in_noncanonical_duplicate_profile_is_not_loaded(nearby):
    with DatabaseUnitOfWork(nearby[2]) as uow:
        uow.execute("INSERT INTO user_xiuxian VALUES ('first','duplicate','later','invalid')")
    assert [user["user_id"] for user in display(repository_for(nearby))] == ["first", "second"]


def test_page_has_only_one_id_and_numeric_pair_not_public_fields(nearby, monkeypatch):
    add_users(nearby, 20)
    original = DatabaseUnitOfWork.query_all
    lengths = []

    def bounded(uow, sql, params=()):
        assert "user_name" not in sql and "power" not in sql and "level" not in sql
        assert sql.endswith("LIMIT ?") and params[-1] == 1
        rows = original(uow, sql, params)
        assert len(rows) <= 1
        assert all(set(row) == {"user_cursor", "map_cursor", "profile_cursor"} for row in rows)
        lengths.append(len(rows))
        return rows

    monkeypatch.setattr(DatabaseUnitOfWork, "query_all", bounded)
    assert len(display(repository_for(nearby))) == 10
    assert lengths == [1] * 22 + [0]


def test_current_fields_null_defaults_and_single_large_id_are_not_truncated(nearby):
    large_id, large_name = "x" * 16384, "n" * 65536
    with DatabaseUnitOfWork(nearby[1]) as uow:
        uow.execute("UPDATE map_status SET user_id=? WHERE user_id='first'", (large_id,))
    with DatabaseUnitOfWork(nearby[2]) as uow:
        uow.execute("UPDATE user_xiuxian SET user_id=?,user_name=?,level=NULL,power=NULL WHERE user_id='first'",
                    (large_id, large_name))
    result = display(repository_for(nearby))
    assert result[0] == {"user_id": large_id, "user_name": large_name, "level": "", "power": 0}


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
def test_candidate_rechecks_exact_pair_position_join_and_self(nearby, monkeypatch, mutation):
    repository = repository_for(nearby)
    original = repository.candidate

    def candidate(cursor, *args, **kwargs):
        if cursor == (2, 2):
            with DatabaseUnitOfWork(nearby[2 if "user_xiuxian" in mutation else 1]) as uow:
                uow.execute(mutation)
        return original(cursor, *args, **kwargs)

    monkeypatch.setattr(repository, "candidate", candidate)
    assert [user["user_id"] for user in display(repository)] == ["second"]


@pytest.mark.parametrize("table", ("map_status", "user_xiuxian"))
def test_deleted_first_pair_does_not_revisit_same_uid_at_a_later_pair(nearby, monkeypatch, table):
    with DatabaseUnitOfWork(nearby[1]) as uow:
        uow.execute("INSERT INTO map_status VALUES ('first','realm','heaven','node')")
    with DatabaseUnitOfWork(nearby[2]) as uow:
        uow.execute("INSERT INTO user_xiuxian VALUES ('first','later','level',9)")
    repository = repository_for(nearby)
    original = repository.candidate
    visited = []

    def candidate(cursor, *args, **kwargs):
        visited.append(kwargs["expected_user_id"])
        if cursor == (2, 2):
            with DatabaseUnitOfWork(nearby[1 if table == "map_status" else 2]) as uow:
                uow.execute(f"DELETE FROM {table} WHERE rowid=2")
        return original(cursor, *args, **kwargs)

    monkeypatch.setattr(repository, "candidate", candidate)
    assert [user["user_id"] for user in display(repository)] == ["second"]
    assert visited == ["first", "second"]


def test_evicted_uid_is_not_counted_again_when_its_canonical_map_row_moves(nearby, monkeypatch):
    ids = add_users(nearby, 10)
    with DatabaseUnitOfWork(nearby[1]) as uow:
        uow.execute("INSERT INTO map_status VALUES ('first','realm','heaven','node')")
    repository = repository_for(nearby)
    original = repository.page
    changed = False

    def page(after, *args):
        nonlocal changed
        if after == ids[8] and not changed:
            changed = True
            with DatabaseUnitOfWork(nearby[1]) as uow:
                uow.execute("DELETE FROM map_status WHERE rowid=2")
        return original(after, *args)

    monkeypatch.setattr(repository, "page", page)
    source = SimpleNamespace(randint=Mock(side_effect=[1, 12]), sample=Mock(side_effect=lambda items, count: list(items)))
    result = display(repository, source)
    assert changed and len(result) == 10
    assert "first" not in {user["user_id"] for user in result}
    assert source.randint.call_args_list == [((1, 11),), ((1, 12),)]


def test_uid_changed_between_page_and_candidate_must_match_frozen_page_identity(nearby, monkeypatch):
    repository = repository_for(nearby)
    original = repository.candidate
    results = []

    def candidate(cursor, *args, **kwargs):
        if kwargs["expected_user_id"] == "first":
            with DatabaseUnitOfWork(nearby[1]) as uow:
                uow.execute("UPDATE map_status SET user_id='zzz' WHERE rowid=2")
            with DatabaseUnitOfWork(nearby[2]) as uow:
                uow.execute("UPDATE user_xiuxian SET user_id='zzz' WHERE rowid=2")
        result = original(cursor, *args, **kwargs)
        results.append((kwargs["expected_user_id"], result))
        return result

    monkeypatch.setattr(repository, "candidate", candidate)
    assert [user["user_id"] for user in display(repository)] == ["zzz", "second"]
    assert results[0] == ("first", None)
    assert [uid for uid, _ in results] == ["first", "second", "zzz"]


def test_frozen_high_water_excludes_later_map_and_profile_appends(nearby, monkeypatch):
    repository = repository_for(nearby)
    original = repository.page
    bounds = []

    def page(after, upper, *args):
        bounds.append(upper)
        if after == "first":
            with DatabaseUnitOfWork(nearby[1]) as uow:
                uow.execute("INSERT INTO map_status VALUES ('second','realm','heaven','node')")
                uow.execute("INSERT INTO map_status VALUES ('zzz','realm','heaven','node')")
            with DatabaseUnitOfWork(nearby[2]) as uow:
                uow.execute("INSERT INTO user_xiuxian VALUES ('missing','new','level',1)")
                uow.execute("INSERT INTO user_xiuxian VALUES ('zzz','new','level',1)")
        return original(after, upper, *args)

    monkeypatch.setattr(repository, "page", page)
    assert [user["user_id"] for user in display(repository)] == ["first", "second"]
    assert bounds == [(4, 3)] * 3


def test_hole_insert_uses_uid_order_and_does_not_revisit_passed_identifiers(nearby, monkeypatch):
    repository = repository_for(nearby)
    original = repository.page
    changed = False

    def page(after, *args):
        nonlocal changed
        if after == "first" and not changed:
            changed = True
            with DatabaseUnitOfWork(nearby[1]) as uow:
                uow.execute("DELETE FROM map_status WHERE rowid IN (1,4)")
                uow.executemany("INSERT INTO map_status(rowid,user_id,realm,heaven,node_id) VALUES (?,?,'realm','heaven','node')",
                                [(1, "aaa"), (4, "zzz")])
            with DatabaseUnitOfWork(nearby[2]) as uow:
                uow.execute("DELETE FROM user_xiuxian WHERE rowid=1")
                uow.execute("INSERT INTO user_xiuxian(rowid,user_id,user_name,level,power) VALUES (1,'zzz','new','level',1)")
        return original(after, *args)

    monkeypatch.setattr(repository, "page", page)
    assert [user["user_id"] for user in display(repository)] == ["first", "second", "zzz"]


@pytest.mark.parametrize("missing", ("player", "game", "both"))
def test_missing_databases_fail_closed_without_creating_any_files(tmp_path, missing):
    player, game = tmp_path / "player.db", tmp_path / "game.db"
    if missing not in ("player", "both"):
        with sqlite3.connect(player) as connection:
            connection.execute("CREATE TABLE map_status(user_id,realm,heaven,node_id)")
    if missing not in ("game", "both"):
        with sqlite3.connect(game) as connection:
            connection.execute("CREATE TABLE user_xiuxian(user_id,user_name,level,power)")
    before = {path.name: path.read_bytes() for path in tmp_path.iterdir()}
    assert display(MapNearbyDisplaySqlQueryRepository(player, game)) == []
    assert {path.name: path.read_bytes() for path in tmp_path.iterdir()} == before


@pytest.mark.parametrize("table,column", (
    ("map_status", None), ("user_xiuxian", None),
    ("map_status", "user_id"), ("map_status", "realm"), ("map_status", "heaven"), ("map_status", "node_id"),
    ("user_xiuxian", "user_id"), ("user_xiuxian", "user_name"), ("user_xiuxian", "level"), ("user_xiuxian", "power"),
))
def test_missing_schema_fails_closed_without_repair_and_closes_connections(nearby, monkeypatch, table, column):
    with DatabaseUnitOfWork(nearby[1 if table == "map_status" else 2]) as uow:
        if column is None:
            uow.execute(f"DROP TABLE {table}")
        else:
            uow.execute(f"ALTER TABLE {table} RENAME COLUMN {column} TO absent")
    connections = track_connections(monkeypatch)
    assert display(repository_for(nearby)) == []
    assert_closed(connections)


def test_corrupt_attached_file_fails_closed_and_closes_main_connection(nearby, tmp_path, monkeypatch):
    corrupt = tmp_path / "corrupt.db"
    corrupt.write_bytes(b"not a SQLite database")
    connections = track_connections(monkeypatch)
    assert display(MapNearbyDisplaySqlQueryRepository(nearby[1], corrupt)) == []
    assert corrupt.read_bytes() == b"not a SQLite database"
    assert_closed(connections)


@pytest.mark.parametrize("stage", ("boundary", "attach", "page", "candidate"))
def test_read_failure_discards_partial_display_and_closes_all_connections(nearby, monkeypatch, stage):
    connections = track_connections(monkeypatch)
    original = DatabaseUnitOfWork.execute
    counts = Counter()

    def execute(uow, sql, params=()):
        kind = ("attach" if sql.startswith("ATTACH") else "boundary" if "MAX(rowid)" in sql
                else "page" if sql.startswith("SELECT CAST(map.user_id")
                else "candidate" if sql.startswith("SELECT map.user_id") else "other")
        counts[kind] += 1
        trigger = 1 if stage == "boundary" else 4 if stage == "attach" else 2
        if kind == stage and counts[kind] == trigger:
            raise sqlite3.OperationalError("injected failure")
        return original(uow, sql, params)

    monkeypatch.setattr(DatabaseUnitOfWork, "execute", execute)
    assert display(repository_for(nearby)) == []
    assert_closed(connections)


def test_bad_power_on_late_candidate_discards_earlier_display(nearby, monkeypatch):
    with DatabaseUnitOfWork(nearby[2]) as uow:
        uow.execute("UPDATE user_xiuxian SET power='invalid' WHERE user_id='second'")
    connections = track_connections(monkeypatch)
    assert display(repository_for(nearby)) == []
    assert_closed(connections)


@pytest.mark.parametrize("bad", (
    {"user_cursor": "second", "map_cursor": 5, "profile_cursor": 3},
    {"user_cursor": "second", "map_cursor": 3, "profile_cursor": 4},
    {"user_cursor": "first", "map_cursor": 3, "profile_cursor": 3},
    {"user_cursor": "aaa", "map_cursor": 3, "profile_cursor": 3},
    {"user_cursor": None, "map_cursor": 3, "profile_cursor": 3},
    {"user_cursor": "second", "map_cursor": "invalid", "profile_cursor": 3}, {},
))
def test_invalid_cursor_discards_partial_display(nearby, monkeypatch, bad):
    repository = repository_for(nearby)
    monkeypatch.setattr(repository, "page", Mock(side_effect=[
        [{"user_cursor": "first", "map_cursor": 2, "profile_cursor": 2}], [bad],
    ]))
    assert display(repository) == []


def test_oversized_page_or_mismatched_candidate_identity_fails_closed(monkeypatch):
    repository = MemoryDisplay(2)
    monkeypatch.setattr(repository, "page", Mock(return_value=[{}, {}]))
    assert display(repository) == []
    repository = MemoryDisplay(2)
    monkeypatch.setattr(repository, "candidate", Mock(return_value={"user_id": "wrong"}))
    assert display(repository) == []


def test_each_one_id_page_yields_even_when_candidate_is_ineligible(monkeypatch):
    repository = MemoryDisplay(40)
    original = repository.candidate
    yields = []

    def candidate(*args, **kwargs):
        original(*args, **kwargs)
        return None

    async def sleep(delay):
        assert delay == 0
        yields.append(repository.observed)

    monkeypatch.setattr(repository, "candidate", candidate)
    monkeypatch.setattr(nearby_display_query.asyncio, "sleep", sleep)
    assert display(repository) == []
    assert yields == list(range(1, 41))


@pytest.mark.parametrize("cancel", (False, True))
def test_connections_close_before_each_yield_and_cancellation_propagates(nearby, monkeypatch, cancel):
    connections = track_connections(monkeypatch)
    yielded = []

    async def sleep(delay):
        assert_closed(connections)
        yielded.append(delay)
        if cancel:
            raise asyncio.CancelledError()

    monkeypatch.setattr(nearby_display_query.asyncio, "sleep", sleep)
    if cancel:
        with pytest.raises(asyncio.CancelledError):
            display(repository_for(nearby))
    else:
        assert len(display(repository_for(nearby))) == 2
    assert yielded == ([0] if cancel else [0, 0])
    assert_closed(connections)


def test_cancelled_read_is_not_converted_to_empty_display(nearby, monkeypatch):
    connections = track_connections(monkeypatch)
    original = DatabaseUnitOfWork.execute

    def execute(uow, sql, params=()):
        if sql.startswith("SELECT map.user_id"):
            raise asyncio.CancelledError()
        return original(uow, sql, params)

    monkeypatch.setattr(DatabaseUnitOfWork, "execute", execute)
    with pytest.raises(asyncio.CancelledError):
        display(repository_for(nearby))
    assert_closed(connections)


def test_read_only_uri_attach_binds_identity_has_no_request_ddl_and_uses_spaces(nearby, monkeypatch):
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
    assert len(display(MapNearbyDisplaySqlQueryRepository(player, game))) == 2
    assert_closed(connections)
    uris = [params[0] for sql, params in statements if sql.startswith("ATTACH")]
    assert uris and all(uri.endswith("?mode=ro") and "%20" in uri for uri in uris)
    candidates = [(sql, params) for sql, params in statements if sql.startswith("SELECT map.user_id")]
    assert all("CAST(map.user_id AS TEXT) COLLATE BINARY=?" in sql and sql.endswith("LIMIT 1") for sql, _ in candidates)
    assert [params[-3] for _, params in candidates] == ["first", "second"]
    assert not any(sql.startswith(("CREATE", "ALTER", "INSERT", "UPDATE", "DELETE")) for sql, _ in statements)


def test_real_display_handler_caps_messages_and_preserves_application_order(nearby):
    add_users(nearby, 10)
    env = load_handler("nearby_users_cmd", None)
    env["map_application"] = MapApplication(nearby[2], nearby[1])
    env["runtime_random"].randint = Mock(side_effect=lambda low, high: high)
    env["runtime_random"].sample = Mock(side_effect=lambda items, count: list(reversed(items)))
    asyncio.run(env["_"]("bot", object()))
    env["_get_all_in_same_node"].assert_not_called()
    message = env["handle_send"].await_args.args[2]
    assert len(message.splitlines()) == 11
    assert message.splitlines()[0] == "【附近道友】"
    assert message.splitlines()[1] == "- u007（level）"
