from __future__ import annotations

import sqlite3
import tempfile
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pytest

import nonebot

try:
    nonebot.get_driver()
except ValueError:
    nonebot.init(command_start={"", "/"}, log_level="ERROR")

from nonebot_plugin_xiuxian_2.core.errors import DomainError
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_natal_treasure import natal_data


def _paths(database: Path):
    return SimpleNamespace(player_db=database)


def test_read_existing_data_does_not_create_missing_database_or_table():
    with tempfile.TemporaryDirectory() as directory:
        database = Path(directory) / "player.db"
        with patch.object(natal_data, "get_paths", return_value=_paths(database)):
            assert natal_data.NatalTreasure.read_existing_data("u") is None
            assert not database.exists()

            with sqlite3.connect(database) as connection:
                connection.execute("CREATE TABLE unrelated(user_id TEXT PRIMARY KEY)")

            assert natal_data.NatalTreasure.read_existing_data("u") is None
            with sqlite3.connect(database) as connection:
                tables = connection.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                ).fetchall()
                assert tables == [("unrelated",)]


def test_read_existing_data_returns_active_treasure_without_initializing_missing_player():
    with tempfile.TemporaryDirectory() as directory:
        database = Path(directory) / "player.db"
        with sqlite3.connect(database) as connection:
            connection.execute(
                "CREATE TABLE natal_treasure(user_id TEXT PRIMARY KEY,form TEXT,name TEXT,level TEXT)"
            )
            connection.execute(
                "INSERT INTO natal_treasure VALUES('u', '1', '隔离法宝', '3')"
            )
            connection.execute("INSERT INTO natal_treasure VALUES('inactive', '0', '', '0')")

        with patch.object(natal_data, "get_paths", return_value=_paths(database)):
            assert natal_data.NatalTreasure.read_existing_data("u") == {
                "form": 1,
                "name": "隔离法宝",
                "level": "3",
            }
            assert natal_data.NatalTreasure.read_existing_data("missing") is None
            assert natal_data.NatalTreasure.read_existing_data("inactive") is None

        with sqlite3.connect(database) as connection:
            assert connection.execute(
                "SELECT user_id FROM natal_treasure ORDER BY user_id"
            ).fetchall() == [("inactive",), ("u",)]


def test_read_existing_data_preserves_a_specific_read_failure():
    with tempfile.TemporaryDirectory() as directory:
        database = Path(directory) / "player.db"
        with sqlite3.connect(database) as connection:
            connection.execute(
                "CREATE TABLE natal_treasure(user_id TEXT PRIMARY KEY,form INTEGER,name TEXT,level INTEGER)"
            )
            connection.execute("INSERT INTO natal_treasure VALUES('u', 1, '法宝', 1)")

        original = natal_data.DatabaseUnitOfWork

        class DenyNatalRead(original):
            def __enter__(self):
                result = super().__enter__()
                self.connection.set_authorizer(
                    lambda action, table, *_: sqlite3.SQLITE_DENY
                    if action == sqlite3.SQLITE_READ and table == "natal_treasure"
                    else sqlite3.SQLITE_OK
                )
                return result

        with (
            patch.object(natal_data, "get_paths", return_value=_paths(database)),
            patch.object(natal_data, "DatabaseUnitOfWork", DenyNatalRead),
            pytest.raises(DomainError) as raised,
        ):
            natal_data.NatalTreasure.read_existing_data("u")

        assert raised.value.code == "natal_treasure_read_failed"
        assert raised.value.details == {
            "reason": "natal_treasure_read_failed",
            "subject": "player_natal_treasure",
        }
