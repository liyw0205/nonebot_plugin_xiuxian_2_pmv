from __future__ import annotations

import sqlite3
from unittest.mock import patch

import pytest

from nonebot_plugin_xiuxian_2.features.title.eligibility import TitleEligibilityApplication


@pytest.fixture
def eligibility(tmp_path):
    game, player = tmp_path / "game.db", tmp_path / "player.db"
    with sqlite3.connect(game) as conn:
        conn.execute("CREATE TABLE user_xiuxian(user_id TEXT, level TEXT)")
        conn.execute("INSERT INTO user_xiuxian VALUES('u','筑基初期')")
    with sqlite3.connect(player) as conn:
        conn.execute('CREATE TABLE statistics(user_id TEXT, "修仙签到" TEXT)')
        conn.execute("INSERT INTO statistics VALUES('u','9')")
        conn.execute("CREATE TABLE tower(user_id TEXT, max_floor INTEGER)")
        conn.execute("INSERT INTO tower VALUES('u',8)")
    return game, player, TitleEligibilityApplication(game, player)


def read(application):
    return application.read_snapshot(user_id="u", statistics_user_id="u", condition_keys=("境界", "修仙签到", "通天塔最高层"))


@pytest.mark.parametrize("table", ["user_xiuxian", "statistics", "tower"])
def test_actual_source_query_error_propagates_without_zero_snapshot_and_recovers(eligibility, table):
    game, player, application = eligibility
    database = game if table == "user_xiuxian" else player
    with sqlite3.connect(database) as conn:
        conn.execute(f'ALTER TABLE "{table}" RENAME TO isolated_saved_source')
        conn.execute(f'CREATE VIEW "{table}" AS SELECT * FROM "isolated_missing_{table}"')
    before = (game.read_bytes(), player.read_bytes())
    with pytest.raises(sqlite3.OperationalError, match=f"isolated_missing_{table}"):
        read(application)
    assert (game.read_bytes(), player.read_bytes()) == before
    with sqlite3.connect(database) as conn:
        conn.execute(f'DROP VIEW "{table}"')
        conn.execute(f'ALTER TABLE isolated_saved_source RENAME TO "{table}"')
    restored = (game.read_bytes(), player.read_bytes())
    snapshot = read(application)
    assert snapshot.profile["level"] == "筑基初期"
    assert snapshot.statistics == {"修仙签到": 9}
    assert snapshot.tower == {"max_floor": 8}
    assert (game.read_bytes(), player.read_bytes()) == restored


@pytest.mark.parametrize("source", ["game", "player"])
def test_corrupt_existing_source_keeps_original_database_error(eligibility, source):
    game, player, application = eligibility
    database = game if source == "game" else player
    database.write_bytes(b"isolated invalid SQLite database")
    before = (game.read_bytes(), player.read_bytes())
    with pytest.raises(sqlite3.DatabaseError, match="file is not a database"):
        read(application)
    assert (game.read_bytes(), player.read_bytes()) == before


def test_unexpected_reader_error_keeps_original_exception(eligibility):
    game, player, application = eligibility
    before = (game.read_bytes(), player.read_bytes())
    failure = RuntimeError("isolated unexpected eligibility reader failure")
    with patch.object(application.repository, "_read_fields", side_effect=failure):
        with pytest.raises(RuntimeError) as caught:
            read(application)
    assert caught.value is failure
    assert (game.read_bytes(), player.read_bytes()) == before
