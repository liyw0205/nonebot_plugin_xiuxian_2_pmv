import asyncio
import ast
from collections import Counter
import itertools
from pathlib import Path
import shutil
import sqlite3
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from ....infrastructure.database import DatabaseUnitOfWork
from ..application import DongfuApplication
from ..random_target_query import select_random_target
from ..random_target_repository import (
    CANDIDATE_PAGE_SIZE,
    DongfuCandidateReadError,
    DongfuRandomTargetSqlQueryRepository,
)
from ..repository import DongfuRepository
from ...map.migrations import apply_map_dongfu_status_schema


DAY = "2026-10-03"
OPTIONS = dict(
    user_id="me", day=DAY, target_limit=3, base_plot_count=1,
    max_plot_count=3, fertilizer_max=10, seed_names={1: "seed"},
)


class Draws:
    def __init__(self, values=None):
        self.values = iter(values) if values is not None else None
        self.bounds = []

    def randint(self, low, high):
        self.bounds.append((low, high))
        value = next(self.values) if self.values is not None else 1
        assert low <= value <= high
        return value


def select(repository, draws=None, **overrides):
    return asyncio.run(select_random_target(
        repository, random_source=draws or Draws(), **(OPTIONS | overrides),
    ))


@pytest.fixture
def candidates(tmp_path):
    player, game = tmp_path / "player.db", tmp_path / "game.db"
    with DatabaseUnitOfWork(player) as uow:
        uow.execute(
            "CREATE TABLE dongfu_status (user_id TEXT,built TEXT,plant_slots TEXT,"
            "plot_count TEXT,planting TEXT,plant_seed_id TEXT,plant_start TEXT,"
            "plant_finish TEXT,intrude_date TEXT,intrude_count TEXT)"
        )
    with DatabaseUnitOfWork(game) as uow:
        uow.execute("CREATE TABLE user_xiuxian (user_id TEXT,user_name TEXT)")
    return DongfuRandomTargetSqlQueryRepository(game, player), player, game


def add(candidates, user_id, rowid=None, profile=True, **fields):
    _, player, game = candidates
    data = dict(
        user_id=user_id, built="1", plant_slots='[{"seed_id":1}]', plot_count="1",
        planting="0", plant_seed_id="0", plant_start="", plant_finish="",
        intrude_date=DAY, intrude_count="0",
    ) | fields
    if rowid is not None:
        data = {"rowid": rowid} | data
    with DatabaseUnitOfWork(player) as uow:
        uow.execute(
            f"INSERT INTO dongfu_status ({','.join(data)}) VALUES ({','.join('?' for _ in data)})",
            tuple(data.values()),
        )
    if profile:
        with DatabaseUnitOfWork(game) as uow:
            uow.execute("INSERT INTO user_xiuxian VALUES (?,?)", (user_id, f"name-{user_id}"))


def test_empty_table_and_non_positive_rowids(candidates):
    repository, _, _ = candidates
    assert repository.upper_rowid() is None
    assert select(repository) is None
    add(candidates, "negative", rowid=-1)
    add(candidates, "zero", rowid=0)
    assert repository.upper_rowid() == 0
    assert [row["cursor"] for row in repository.page(None, 0)] == [-1, 0]
    assert select(repository)["user_id"] == "zero"


def test_raw_pages_are_bounded_and_late_candidate_is_not_truncated(candidates):
    repository, player, _ = candidates
    with DatabaseUnitOfWork(player) as uow:
        uow.executemany(
            "INSERT INTO dongfu_status (user_id,built) VALUES (?,0)",
            [(f"empty-{index}",) for index in range(600)],
        )
    add(candidates, "late")
    upper = repository.upper_rowid()
    first = repository.page(None, upper)
    assert len(first) == CANDIDATE_PAGE_SIZE == 256
    assert set(first[0]) == {"cursor", "user_id", "is_built"}
    assert repository.page(first[-1]["cursor"], first[-1]["cursor"]) == []
    assert select(repository)["user_id"] == "late"


@pytest.mark.parametrize("fields,expected", [
    ({"built": "0"}, False), ({"built": "true"}, False),
    ({"plant_slots": "[]"}, False), ({"plant_slots": "broken"}, False),
    ({"plant_slots": '[{"seed_id":999}]'}, False),
    ({"intrude_count": "3"}, False), ({"intrude_count": "bad"}, True),
    ({"intrude_count": '"3"', "intrude_date": '"2026-10-03"'}, False),
    ({"intrude_count": "999", "intrude_date": "2026-10-02"}, True),
    ({"plant_slots": "[]", "planting": "1", "plant_seed_id": "1"}, True),
    ({"plot_count": "999", "plant_slots": '[{},{},{},{"seed_id":1}]'}, False),
    ({"plant_finish": "2099-01-01"}, True),
    ({"plot_count": "0"}, True),
])
def test_legacy_eligibility_and_no_new_maturity_filter(candidates, fields, expected):
    add(candidates, "target", **fields)
    assert (select(candidates[0]) is not None) is expected


def test_self_missing_profile_and_first_profile_rules(candidates):
    repository, _, game = candidates
    add(candidates, "me")
    add(candidates, "missing", profile=False)
    assert select(repository) is None
    add(candidates, "target")
    with DatabaseUnitOfWork(game) as uow:
        uow.execute("INSERT INTO user_xiuxian VALUES ('target','later-name')")
    assert select(repository) == {"user_id": "target", "user_name": "name-target"}


def test_duplicate_legacy_caves_keep_weight_and_use_first_state(candidates):
    add(candidates, "duplicate")
    add(candidates, "duplicate", profile=False, plant_slots="[]")
    add(candidates, "other")
    draws = Draws([1, 2, 3])
    assert select(candidates[0], draws)["user_id"] == "duplicate"
    assert draws.bounds == [(1, 1), (1, 2), (1, 3)]


def test_reservoir_is_exactly_uniform_for_three_eligible_rows(candidates):
    for user_id in ("a", "b", "c"):
        add(candidates, user_id)
    results = Counter(
        select(candidates[0], Draws([1, second, third]))["user_id"]
        for second, third in itertools.product(range(1, 3), range(1, 4))
    )
    assert results == {"a": 2, "b": 2, "c": 2}


@pytest.mark.parametrize("kind", ("legacy", "slots", "minimal"))
def test_missing_optional_columns_use_defaults_without_schema_repair(candidates, kind):
    repository, player, game = candidates
    extra = {"legacy": ",planting TEXT,plant_seed_id TEXT", "slots": ",plant_slots TEXT", "minimal": ""}[kind]
    values = {"legacy": ",'1','1'", "slots": ",'[{\"seed_id\":1}]'", "minimal": ""}[kind]
    with DatabaseUnitOfWork(player) as uow:
        uow.execute("DROP TABLE dongfu_status")
        uow.execute(f"CREATE TABLE dongfu_status (user_id TEXT,built TEXT{extra})")
        uow.execute(f"INSERT INTO dongfu_status VALUES ('target','1'{values})")
    with DatabaseUnitOfWork(game) as uow:
        uow.execute("INSERT INTO user_xiuxian VALUES ('target','name')")
    before = player.read_bytes()
    assert (select(repository) is not None) is (kind != "minimal")
    assert player.read_bytes() == before


@pytest.mark.parametrize("missing", ("game", "player", "both"))
def test_missing_databases_are_not_created(tmp_path, missing):
    player, game = tmp_path / "player.db", tmp_path / "game.db"
    for label, path in (("game", game), ("player", player)):
        if missing not in (label, "both"):
            with DatabaseUnitOfWork(path) as uow:
                if label == "player":
                    uow.execute("CREATE TABLE dongfu_status (user_id TEXT,built TEXT)")
                    uow.execute("INSERT INTO dongfu_status VALUES ('target','1')")
            # A rollback-journal fixture can assert absence of new sidecars.
            with sqlite3.connect(path) as connection:
                connection.execute("PRAGMA journal_mode=DELETE")
    before = set(tmp_path.iterdir())
    assert select(DongfuRandomTargetSqlQueryRepository(game, player)) is None
    assert set(tmp_path.iterdir()) == before


@pytest.mark.parametrize("broken", ("cave_table", "cave_built", "cave_id", "profile_table", "profile_name"))
def test_missing_mandatory_schema_fails_closed_without_repair(candidates, broken):
    repository, player, game = candidates
    add(candidates, "target")
    path, table = (player, "dongfu_status") if broken.startswith("cave") else (game, "user_xiuxian")
    with DatabaseUnitOfWork(path) as uow:
        if broken.endswith("table"):
            uow.execute(f"DROP TABLE {table}")
        else:
            column = {"cave_built": "built", "cave_id": "user_id", "profile_name": "user_name"}[broken]
            uow.execute(f"ALTER TABLE {table} DROP COLUMN {column}")
    before = path.read_bytes()
    assert select(repository) is None
    assert path.read_bytes() == before


def test_read_only_sql_and_attachment_paths_with_spaces(candidates, tmp_path, monkeypatch):
    _, player, game = candidates
    add(candidates, "target")
    folder = tmp_path / "db space"
    folder.mkdir()
    new_player, new_game = folder / "player db.sqlite", folder / "game db.sqlite"
    shutil.copyfile(player, new_player)
    shutil.copyfile(game, new_game)
    statements = []
    execute = DatabaseUnitOfWork.execute

    def tracked(uow, sql, params=()):
        assert uow.read_only
        statements.append((sql, params))
        return execute(uow, sql, params)

    monkeypatch.setattr(DatabaseUnitOfWork, "execute", tracked)
    assert select(DongfuRandomTargetSqlQueryRepository(new_game, new_player))["user_id"] == "target"
    assert all(not sql.startswith(("CREATE", "ALTER", "INSERT", "UPDATE", "DELETE")) for sql, _ in statements)
    attach = [params[0] for sql, params in statements if sql.startswith("ATTACH")]
    assert len(attach) == 1 and attach[0].endswith("?mode=ro") and "%20" in attach[0]
    page_params = [params for sql, params in statements if "AS is_built" in sql]
    assert page_params[0][0] == "1" and page_params[0][-1] == 256


def test_startup_cave_primary_key_supports_indexed_single_state_read(candidates, monkeypatch):
    repository, player, _ = candidates
    with DatabaseUnitOfWork(player) as uow:
        uow.execute("DROP TABLE dongfu_status")
        apply_map_dongfu_status_schema(uow)
    add(candidates, "target")
    selects = []
    execute = DatabaseUnitOfWork.execute

    def tracked(uow, sql, params=()):
        if sql.startswith("SELECT cave."):
            selects.append((sql, params))
        return execute(uow, sql, params)

    monkeypatch.setattr(DatabaseUnitOfWork, "execute", tracked)
    assert select(repository)["user_id"] == "target"
    with DatabaseUnitOfWork(player, read_only=True) as uow:
        uow.attach_database(f"{repository.game_database.as_uri()}?mode=ro", "game_data")
        plan = uow.query_all("EXPLAIN QUERY PLAN " + selects[0][0], selects[0][1])
    assert any("SEARCH cave USING INDEX sqlite_autoindex_dongfu_status_1" in row["detail"] for row in plan)


def test_profile_lookup_binds_legacy_string_user_id(candidates):
    repository, player, game = candidates
    with DatabaseUnitOfWork(player) as uow:
        uow.execute("DROP TABLE dongfu_status")
        uow.execute("CREATE TABLE dongfu_status (user_id INTEGER,built INTEGER,planting INTEGER,plant_seed_id INTEGER)")
        uow.execute("INSERT INTO dongfu_status VALUES (2,1,1,1)")
    with DatabaseUnitOfWork(game) as uow:
        uow.execute("INSERT INTO user_xiuxian VALUES ('02','wrong-profile')")
    assert select(repository) is None
    with DatabaseUnitOfWork(game) as uow:
        uow.execute("INSERT INTO user_xiuxian VALUES ('2','right-profile')")
    assert select(repository) == {"user_id": "2", "user_name": "right-profile"}


def test_corrupted_profile_database_is_not_repaired(candidates):
    repository, _, game = candidates
    add(candidates, "target")
    game.write_bytes(b"not a database")
    assert select(repository) is None
    assert game.read_bytes() == b"not a database"


@pytest.mark.parametrize("failure", ("boundary", "page", "candidate", "attach"))
def test_read_errors_discard_partial_sample(candidates, monkeypatch, failure):
    repository, _, _ = candidates
    add(candidates, "a")
    add(candidates, "b")
    draws = Draws()
    execute = DatabaseUnitOfWork.execute
    pages = repository.page
    calls = 0

    def fail(uow, sql, params=()):
        nonlocal calls
        if failure == "boundary" and "MAX(rowid)" in sql:
            raise RuntimeError("broken boundary")
        if failure in ("candidate", "attach") and sql.startswith("ATTACH"):
            calls += 1
            if calls == 2:
                if failure == "attach":
                    return execute(uow, sql, ("file:/missing-test-game.db?mode=ro",))
                raise RuntimeError("broken candidate")
        return execute(uow, sql, params)

    def partial_page(after, upper):
        if after is not None:
            raise DongfuCandidateReadError("broken next page")
        return pages(after, upper)[:1]

    monkeypatch.setattr(DatabaseUnitOfWork, "execute", fail)
    if failure == "page":
        monkeypatch.setattr(repository, "page", partial_page)
    assert select(repository, draws) is None
    if failure != "boundary":
        assert draws.bounds == [(1, 1)]


@pytest.mark.parametrize("cancel", (False, True))
def test_cooperative_yields_and_cancellation_leave_no_connection(candidates, monkeypatch, cancel):
    repository, player, _ = candidates
    add(candidates, "target")
    with DatabaseUnitOfWork(player) as uow:
        uow.executemany("INSERT INTO dongfu_status (user_id,built) VALUES (?,0)", [(str(i),) for i in range(64)])
    active, entered, sleeps = [], [], []
    enter, leave = DatabaseUnitOfWork.__enter__, DatabaseUnitOfWork.__exit__

    def tracked_enter(uow):
        value = enter(uow)
        active.append(uow)
        entered.append(uow)
        return value

    def tracked_leave(uow, *args):
        try:
            return leave(uow, *args)
        finally:
            active.remove(uow)

    async def sleep(delay):
        assert delay == 0 and not active
        sleeps.append(delay)
        if cancel:
            raise asyncio.CancelledError

    monkeypatch.setattr(DatabaseUnitOfWork, "__enter__", tracked_enter)
    monkeypatch.setattr(DatabaseUnitOfWork, "__exit__", tracked_leave)
    monkeypatch.setattr(asyncio, "sleep", sleep)
    if cancel:
        with pytest.raises(asyncio.CancelledError):
            select(repository)
    else:
        assert select(repository)["user_id"] == "target"
        assert len(sleeps) == 3
    assert not active and all(uow.connection is None for uow in entered)


@pytest.mark.parametrize("mutation,expected", [
    ("above", "anchor"), ("gap", "gap"), ("reuse", "replacement"),
    ("delete", None), ("update", None), ("passed", "anchor"),
])
def test_short_transactions_have_explicit_non_snapshot_semantics(candidates, monkeypatch, mutation, expected):
    repository, player, _ = candidates
    with DatabaseUnitOfWork(player) as uow:
        uow.executemany("INSERT INTO dongfu_status (rowid,user_id,built) VALUES (?,?,0)", [(i, f"empty-{i}") for i in range(1, 257)])
    add(candidates, "anchor", rowid=300)
    page = repository.page

    def changed_page(after, upper):
        if after == 256:
            if mutation == "above":
                add(candidates, "new", rowid=301)
            elif mutation == "gap":
                add(candidates, "gap", rowid=257)
            elif mutation == "passed":
                with DatabaseUnitOfWork(player) as uow:
                    uow.execute("DELETE FROM dongfu_status WHERE rowid=1")
                add(candidates, "passed", rowid=1)
            elif mutation in ("delete", "reuse"):
                with DatabaseUnitOfWork(player) as uow:
                    uow.execute("DELETE FROM dongfu_status WHERE rowid=300")
                if mutation == "reuse":
                    add(candidates, "replacement", rowid=300)
            else:
                with DatabaseUnitOfWork(player) as uow:
                    uow.execute("UPDATE dongfu_status SET plant_slots='[]' WHERE rowid=300")
        return page(after, upper)

    monkeypatch.setattr(repository, "page", changed_page)
    result = select(repository, Draws([1, 2]) if mutation == "gap" else Draws())
    assert (result["user_id"] if result else None) == expected


@pytest.mark.parametrize("mutation", ("built", "profile"))
def test_candidate_state_is_rechecked_after_page_metadata(candidates, monkeypatch, mutation):
    repository, player, game = candidates
    add(candidates, "target")
    page = repository.page

    def changed_page(after, upper):
        rows = page(after, upper)
        path = player if mutation == "built" else game
        with DatabaseUnitOfWork(path) as uow:
            uow.execute(
                "UPDATE dongfu_status SET built='0'" if mutation == "built"
                else "DELETE FROM user_xiuxian"
            )
        return rows

    monkeypatch.setattr(repository, "page", changed_page)
    assert select(repository) is None


def test_seed_configuration_is_frozen_before_cooperative_yield(candidates, monkeypatch):
    repository, player, _ = candidates
    with DatabaseUnitOfWork(player) as uow:
        uow.executemany("INSERT INTO dongfu_status (user_id,built) VALUES (?,0)", [(str(i),) for i in range(31)])
    add(candidates, "target")
    seeds = {1: "seed"}

    async def sleep(_):
        seeds.clear()

    monkeypatch.setattr(asyncio, "sleep", sleep)
    assert select(repository, seed_names=seeds)["user_id"] == "target"


@pytest.mark.parametrize("cursors", ([1, 1], [3], [2, 1]))
def test_invalid_cursor_stream_fails_closed(cursors):
    repository = SimpleNamespace(
        upper_rowid=lambda: 2,
        page=lambda *_: [{"cursor": value, "user_id": "x", "is_built": False} for value in cursors],
    )
    assert select(repository) is None


def test_application_and_repository_async_delegation(candidates, tmp_path):
    query, player, game = candidates
    add(candidates, "target")
    app = DongfuApplication(game, repository=DongfuRepository(game, player))
    assert asyncio.run(app.random_target(random_source=Draws(), **OPTIONS)) == select(query)
    target = {"user_id": "x", "user_name": "name"}
    repository = SimpleNamespace(random_target=AsyncMock(return_value=target))
    app = DongfuApplication(tmp_path / "unused.db", repository=repository)
    assert asyncio.run(app.random_target(**OPTIONS)) is target
    repository.random_target.assert_awaited_once_with(**OPTIONS)
    assert not (tmp_path / "unused.db").exists()


def test_matcher_awaits_feature_and_has_no_full_list_or_profile_loop():
    source = (Path(__file__).resolve().parents[3] / "xiuxian/xiuxian_dongfu/__init__.py").read_text(encoding="utf-8")
    tree = ast.parse(source)
    helper = next(node for node in tree.body if isinstance(node, ast.AsyncFunctionDef) and node.name == "_get_random_dongfu_target")
    helper_source = ast.get_source_segment(source, helper)
    assert "await dongfu_application.random_target(" in helper_source
    assert "day=_today_str()" in helper_source and "random_source=runtime_random" in helper_source
    assert "await _get_random_dongfu_target(my_uid)" in source
    assert all(token not in helper_source for token in ("list_users_by_fields", "get_fields", "get_user_info_with_id", "choice(", "candidates"))
