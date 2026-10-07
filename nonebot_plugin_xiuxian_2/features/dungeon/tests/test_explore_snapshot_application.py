from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from ..explore_snapshot import (
    DungeonExploreSnapshotApplication,
    DungeonExploreSnapshotError,
)


def _database(path: Path) -> None:
    with sqlite3.connect(path) as connection:
        connection.execute("CREATE TABLE user_cd(user_id TEXT,type INTEGER)")
        connection.execute(
            "CREATE TABLE back(user_id TEXT,goods_id INTEGER,goods_num INTEGER,bind_num INTEGER)"
        )
        connection.executemany(
            "INSERT INTO user_cd VALUES (?,?)", [("u", 1), ("u", 3), ("v", 2)]
        )
        connection.executemany(
            "INSERT INTO back VALUES (?,?,?,?)",
            [("u", 9, 3, 2), ("u", 10, 1, 1), ("v", 9, 4, 4)],
        )


def test_batch_snapshot_preserves_duplicate_cd_latest_and_inventory(tmp_path: Path):
    database = tmp_path / "game.db"
    _database(database)
    profiles = {"u": {"user_id": "u", "user_name": "U"}, "v": None}
    application = DungeonExploreSnapshotApplication(
        database,
        profile_reader=lambda user_id: profiles[user_id],
    )

    result = application.read(["u", "v", "u"])

    assert result["profiles"] == profiles
    assert result["cd_types"] == {"u": 3, "v": 2}
    assert result["inventory"]["u"]["9"] == {"goods_num": 3, "bind_num": 2}
    assert result["inventory"]["v"]["9"] == {"goods_num": 4, "bind_num": 4}


def test_supplied_profiles_skip_profile_reader(tmp_path: Path):
    database = tmp_path / "game.db"
    _database(database)
    application = DungeonExploreSnapshotApplication(
        database,
        profile_reader=lambda _user_id: (_ for _ in ()).throw(
            AssertionError("supplied profile should be used")
        ),
    )

    result = application.read(
        ["u"], supplied_profiles={"u": {"user_id": "u", "user_name": "U"}}
    )

    assert result["profiles"]["u"]["user_name"] == "U"


def test_missing_member_profile_is_explicit(tmp_path: Path):
    database = tmp_path / "game.db"
    _database(database)
    application = DungeonExploreSnapshotApplication(
        database, profile_reader=lambda _user_id: None
    )

    assert application.read(["missing"])["profiles"] == {"missing": None}


def test_empty_member_ids_do_not_open_or_create_database(tmp_path: Path):
    database = tmp_path / "missing" / "game.db"
    application = DungeonExploreSnapshotApplication(
        database,
        profile_reader=lambda _user_id: (_ for _ in ()).throw(
            AssertionError("empty input must not read profiles")
        ),
    )

    assert application.read([]) == {"profiles": {}, "cd_types": {}, "inventory": {}}
    assert not database.exists()
    assert not database.parent.exists()


@pytest.mark.parametrize("table_sql", [
    "CREATE TABLE user_cd(user_id TEXT,type INTEGER)",
    "CREATE TABLE back(user_id TEXT,goods_id INTEGER,goods_num INTEGER,bind_num INTEGER)",
])
def test_missing_required_table_fails_closed_without_creating_wal(
    tmp_path: Path, table_sql: str
):
    database = tmp_path / "partial.db"
    with sqlite3.connect(database) as connection:
        connection.execute(table_sql)
    before = set(path.name for path in tmp_path.iterdir())
    application = DungeonExploreSnapshotApplication(
        database, profile_reader=lambda _user_id: {"user_id": "u"}
    )

    with pytest.raises(DungeonExploreSnapshotError):
        application.read(["u"])

    assert set(path.name for path in tmp_path.iterdir()) == before
    assert not (tmp_path / "partial.db-wal").exists()


def test_missing_database_fails_closed_without_creating_file(tmp_path: Path):
    database = tmp_path / "missing.db"
    application = DungeonExploreSnapshotApplication(
        database, profile_reader=lambda _user_id: {"user_id": "u"}
    )

    with pytest.raises(DungeonExploreSnapshotError):
        application.read(["u"])

    assert not database.exists()
