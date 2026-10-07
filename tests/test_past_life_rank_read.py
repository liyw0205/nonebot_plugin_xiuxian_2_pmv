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


def _paths(player_db: Path):
    return SimpleNamespace(player_db=player_db)


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
    assert 'past_life_limit.get_all_field_data("best_score")' in handler
    assert "player_data_manager" not in handler
