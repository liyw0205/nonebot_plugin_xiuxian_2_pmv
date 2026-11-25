from __future__ import annotations

import sqlite3
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_past_life.past_life_limit import (
    PastLifeLimit,
)
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils.shared_read_cache import (
    process_read_cache,
)


def _paths(player_db: Path, game_db: Path | None = None):
    return SimpleNamespace(player_db=player_db, game_db=game_db or player_db)


def test_past_life_rank_read_does_not_create_missing_database(tmp_path: Path) -> None:
    database = tmp_path / "missing-player.db"
    with patch(
        "nonebot_plugin_xiuxian_2.xiuxian.xiuxian_past_life.past_life_limit.get_paths",
        return_value=_paths(database),
    ):
        assert PastLifeLimit().get_all_field_data("best_score") == []
    assert not database.exists()


def test_past_life_rank_read_does_not_create_missing_table(tmp_path: Path) -> None:
    database = tmp_path / "player.db"
    sqlite3.connect(database).close()
    with patch(
        "nonebot_plugin_xiuxian_2.xiuxian.xiuxian_past_life.past_life_limit.get_paths",
        return_value=_paths(database),
    ):
        assert PastLifeLimit().get_all_field_data("best_score") == []
    with sqlite3.connect(database) as connection:
        assert connection.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall() == []


def test_past_life_rank_read_uses_existing_schema_without_ddl(tmp_path: Path) -> None:
    database = tmp_path / "player.db"
    with sqlite3.connect(database) as connection:
        connection.execute(
            "CREATE TABLE past_life(user_id TEXT PRIMARY KEY, best_score INTEGER)"
        )
        connection.executemany(
            "INSERT INTO past_life(user_id, best_score) VALUES(?, ?)",
            [("u", 88), ("v", None)],
        )

    with patch(
        "nonebot_plugin_xiuxian_2.xiuxian.xiuxian_past_life.past_life_limit.get_paths",
        return_value=_paths(database),
    ):
        assert PastLifeLimit().get_all_field_data("best_score") == [
            ("u", 88),
            ("v", None),
        ]
        assert PastLifeLimit().get_all_field_data("missing_column") == []

    with sqlite3.connect(database) as connection:
        columns = {
            row[1]
            for row in connection.execute("PRAGMA table_info(past_life)")
        }
    assert columns == {"user_id", "best_score"}


def test_past_life_rank_handler_uses_limit_read_model() -> None:
    source = (
        Path(__file__).resolve().parents[1]
        / "nonebot_plugin_xiuxian_2"
        / "xiuxian"
        / "xiuxian_past_life"
        / "__init__.py"
    ).read_text(encoding="utf-8")
    handler = source.split("@past_rank_cmd.handle", 1)[1].split(
        "@reset_past_life_cmd.handle", 1
    )[0]
    assert "past_life_limit.public_rank_snapshot(30)" in handler
    assert "player_data_manager" not in handler
    reset_handler = source.split("@reset_past_life_cmd.handle", 1)[1].split(
        "# ═══ 工具函数 ═══", 1
    )[0]
    assert "if clear_history" in reset_handler
    assert "invalidate_snapshot_database(_paths.player_db)" in reset_handler


def test_past_life_public_rank_snapshot_batches_and_refreshes_explicitly(tmp_path: Path) -> None:
    player_db = tmp_path / "player.db"
    game_db = tmp_path / "game.db"
    game_db.touch()
    with sqlite3.connect(player_db) as connection:
        connection.execute(
            "CREATE TABLE past_life(user_id TEXT PRIMARY KEY, best_score INTEGER, best_ending TEXT)"
        )
        connection.executemany(
            "INSERT INTO past_life(user_id,best_score,best_ending) VALUES(?,?,?)",
            [("u1", 100, "旧结局"), ("u2", 90, "次席")],
        )
    with sqlite3.connect(game_db) as connection:
        connection.execute(
            "CREATE TABLE user_xiuxian(user_id TEXT,user_name TEXT)"
        )
        connection.executemany(
            "INSERT INTO user_xiuxian(user_id,user_name) VALUES(?,?)",
            [("u1", "旧道号"), ("u2", "次席道号")],
        )
    process_read_cache.clear()
    try:
        with patch(
            "nonebot_plugin_xiuxian_2.xiuxian.xiuxian_past_life.past_life_limit.get_paths",
            return_value=_paths(player_db, game_db),
        ):
            limit = PastLifeLimit()
            first = limit.public_rank_snapshot()
            assert first == {
                "has_scores": True,
                "rows": [(1, "旧道号", 100, "旧结局"), (2, "次席道号", 90, "次席")],
            }
            with sqlite3.connect(player_db) as connection:
                connection.execute(
                    "UPDATE past_life SET best_score=200,best_ending='新结局' WHERE user_id='u2'"
                )
            with sqlite3.connect(game_db) as connection:
                connection.execute(
                    "UPDATE user_xiuxian SET user_name='新道号' WHERE user_id='u2'"
                )
            assert limit.public_rank_snapshot() == first
            assert process_read_cache.stats()["hits"] == 1

            process_read_cache.invalidate_snapshot_database(player_db)
            refreshed = limit.public_rank_snapshot()
            assert refreshed["rows"][0] == (1, "新道号", 200, "新结局")
    finally:
        process_read_cache.clear()
