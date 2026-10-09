from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
import random
import sqlite3
from types import SimpleNamespace
from unittest.mock import Mock, patch

import pytest

from ..opponent_application import ArenaOpponentApplication
from ..opponent_repository import ArenaOpponentRepository


class Clock:
    def __init__(self):
        self.current = datetime(2026, 10, 7, tzinfo=timezone.utc)

    def now(self):
        return self.current


@pytest.fixture
def runtime(tmp_path):
    game, player = tmp_path / "game.db", tmp_path / "player.db"
    with sqlite3.connect(game) as connection:
        connection.execute("CREATE TABLE user_xiuxian(user_id TEXT,user_name TEXT)")
        connection.executemany("INSERT INTO user_xiuxian VALUES (?,?)", [
            (uid, f"name-{uid}") for uid in ("self", "near1", "near2", "far", "bad")
        ])
    with sqlite3.connect(player) as connection:
        connection.execute("CREATE TABLE arena(user_id TEXT,score)")
        connection.executemany("INSERT INTO arena VALUES (?,?)", [
            ("self", 1000), ("near1", 900), ("near2", 1200), ("far", 1800),
            ("ghost", 1001), ("bad", "invalid-score"),
        ])
    clock = Clock()
    repository = ArenaOpponentRepository(game, player, clock=clock, capacity=2)
    state = SimpleNamespace(get=Mock(return_value={"score": 1000}))
    application = ArenaOpponentApplication(repository, state)
    return SimpleNamespace(game=game, player=player, repository=repository,
                           application=application, state=state, clock=clock)


def test_candidates_use_one_readonly_join_and_exclude_self_or_missing_profiles(runtime):
    statements = []
    connect = sqlite3.connect

    def traced(*args, **kwargs):
        assert "mode=ro" in args[0] and kwargs["uri"] is True
        connection = connect(*args, **kwargs)
        connection.set_trace_callback(statements.append)
        return connection

    with patch("nonebot_plugin_xiuxian_2.infrastructure.database.uow.sqlite3.connect", side_effect=traced):
        candidates = runtime.repository.candidates("self")
    assert {row["user_id"] for row in candidates} == {"near1", "near2", "far"}
    assert sum(statement.startswith("SELECT") for statement in statements) == 1
    assert any("ATTACH DATABASE" in statement and "mode=ro" in statement for statement in statements)
    assert not any(statement.startswith(("INSERT", "UPDATE", "DELETE", "CREATE")) for statement in statements)


def test_find_uses_seeded_nearby_selection_then_nearest_fallback(runtime):
    expected = random.Random("operation").choice(["near1", "near2"])
    assert runtime.application.find("self", "operation") == expected
    runtime.state.get.return_value = {"score": 2200}
    assert runtime.application.find("self", "operation") == "far"
    runtime.state.get.assert_called_with("self")


def test_join_can_use_player_identity_index(runtime):
    with sqlite3.connect(runtime.game) as connection:
        connection.execute("CREATE INDEX profile_user_id ON user_xiuxian(user_id)")
    with runtime.repository._connection() as connection:
        plan = connection.execute(
            "EXPLAIN QUERY PLAN SELECT a.user_id,p.user_name FROM arena AS a "
            "JOIN profiles.user_xiuxian AS p ON p.user_id=CAST(a.user_id AS TEXT) "
            "WHERE a.user_id<>?", ("self",),
        ).fetchall()
    assert any("SEARCH p USING INDEX profile_user_id" in row[3] for row in plan)


def test_empty_candidates_are_distinct_from_missing_or_broken_storage(runtime, tmp_path):
    with sqlite3.connect(runtime.player) as connection:
        connection.execute("DELETE FROM arena WHERE user_id<>'self'")
    assert runtime.application.find("self") is None
    missing = tmp_path / "missing.db"
    repository = ArenaOpponentRepository(missing, runtime.player)
    state = SimpleNamespace(get=Mock())
    application = ArenaOpponentApplication(repository, state)
    with pytest.raises(FileNotFoundError):
        application.find("self")
    with pytest.raises(FileNotFoundError):
        application.state("self")
    state.get.assert_not_called()
    assert not missing.exists()
    with sqlite3.connect(runtime.player) as connection:
        connection.execute("DROP TABLE arena")
    with pytest.raises(sqlite3.OperationalError):
        runtime.application.find("self")


def test_view_and_cache_methods_share_owner_and_keep_scores_until_expiry(runtime):
    first = runtime.application.view("self")
    assert not first["from_cache"]
    assert [row["user_id"] for row in first["targets"]] == ["near1", "near2", "far"]
    assert runtime.application.get_cache("self") == runtime.repository.get_cache("self")
    with sqlite3.connect(runtime.player) as connection:
        connection.execute("UPDATE arena SET score=3000 WHERE user_id='near1'")
    cached = runtime.application.view("self")
    assert cached["from_cache"]
    assert cached["targets"][0]["score"] == 900
    runtime.clock.current += timedelta(seconds=180)
    refreshed = runtime.application.view("self")
    assert not refreshed["from_cache"]
    assert [row["user_id"] for row in refreshed["targets"]] == ["near2", "far", "near1"]
    runtime.application.clear_cache("self")
    assert runtime.repository.get_cache("self") is None


def test_cache_is_max_three_copy_isolated_and_bounded_lru(runtime):
    targets = [{"user_id": str(index), "score": index} for index in range(5)]
    runtime.application.set_cache("one", targets)
    targets[0]["score"] = 999
    copied = runtime.application.get_cache("one")
    assert len(copied) == 3 and copied[0]["score"] == 0
    copied[0]["score"] = 888
    assert runtime.application.get_cache("one")[0]["score"] == 0
    runtime.application.set_cache("two", targets)
    runtime.application.get_cache("one")
    runtime.application.set_cache("three", targets)
    assert runtime.application.get_cache("two") is None
    assert runtime.application.get_cache("one") is not None
    assert len(runtime.repository._cache) == 2


def test_stale_view_targets_are_rebuilt_with_matching_challenge_indices(runtime):
    runtime.application.view("self")
    with sqlite3.connect(runtime.game) as connection:
        connection.execute("DELETE FROM user_xiuxian WHERE user_id='near1'")
    rebuilt = runtime.application.view("self")
    assert not rebuilt["from_cache"]
    assert [row["user_id"] for row in rebuilt["targets"]] == ["near2", "far"]
    assert [row["user_id"] for row in runtime.application.get_cache("self")] == ["near2", "far"]
    with sqlite3.connect(runtime.player) as connection:
        connection.execute("DROP TABLE arena")
    with pytest.raises(sqlite3.OperationalError):
        runtime.application.view("self")


def test_concurrent_cache_mutations_return_detached_bounded_snapshots(runtime):
    def worker(index):
        user_id = f"user-{index}"
        for iteration in range(30):
            runtime.application.set_cache(user_id, [{"user_id": "opponent", "score": iteration}])
            snapshot = runtime.application.get_cache(user_id)
            assert snapshot is None or snapshot[0]["user_id"] == "opponent"
            runtime.application.clear_cache(user_id)

    with ThreadPoolExecutor(max_workers=4) as executor:
        list(executor.map(worker, range(4)))
    assert len(runtime.repository._cache) <= 2
