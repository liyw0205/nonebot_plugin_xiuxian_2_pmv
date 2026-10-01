from __future__ import annotations

import sqlite3

from ..application import PlayerStateApplication
from ....infrastructure.database import DatabaseUnitOfWork


def _database(path):
    with sqlite3.connect(path) as connection:
        connection.execute(
            "CREATE TABLE user_xiuxian(" 
            "user_id TEXT,hp INTEGER,mp INTEGER,atk INTEGER,exp INTEGER)"
        )


def test_initializes_the_first_duplicate_user_row_only(tmp_path):
    database = tmp_path / "player.db"
    _database(database)
    with sqlite3.connect(database) as connection:
        connection.executemany(
            "INSERT INTO user_xiuxian(user_id,hp,mp,atk,exp) VALUES(?,?,?,?,?)",
            [("u", 0, 0, 0, 100), ("u", 0, 0, 0, 200)],
        )

    result = PlayerStateApplication(database).initialize_if_empty("u")

    assert result.status == "applied"
    assert (result.hp, result.mp, result.atk, result.exp) == (50, 100, 10, 100)
    with DatabaseUnitOfWork(database) as uow:
        rows = uow.query_all(
            "SELECT hp,mp,atk,exp FROM user_xiuxian WHERE user_id=? ORDER BY rowid",
            ("u",),
        )
    assert rows == [
        {"hp": 50, "mp": 100, "atk": 10, "exp": 100},
        {"hp": 0, "mp": 0, "atk": 0, "exp": 200},
    ]


def test_missing_schema_does_not_create_tables_and_uses_explicit_fallback(tmp_path):
    database = tmp_path / "player.db"
    database.touch()
    calls: list[str] = []

    result = PlayerStateApplication(database).initialize_if_empty(
        "u", fallback=calls.append
    )

    assert result.status == "legacy_fallback"
    assert calls == ["u"]
    with sqlite3.connect(database) as connection:
        assert connection.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall() == []


def test_initialized_player_is_not_overwritten(tmp_path):
    database = tmp_path / "player.db"
    _database(database)
    with sqlite3.connect(database) as connection:
        connection.execute(
            "INSERT INTO user_xiuxian(user_id,hp,mp,atk,exp) VALUES(?,?,?,?,?)",
            ("u", 9, 8, 7, 100),
        )

    result = PlayerStateApplication(database).initialize_if_empty("u")

    assert result.status == "already_initialized"
    assert (result.hp, result.mp, result.atk) == (9, 8, 7)


def test_updates_only_the_first_duplicate_user_vital_row(tmp_path):
    database = tmp_path / "player.db"
    _database(database)
    with sqlite3.connect(database) as connection:
        connection.executemany(
            "INSERT INTO user_xiuxian(user_id,hp,mp,atk,exp) VALUES(?,?,?,?,?)",
            [("u", 80, 70, 8, 100), ("u", 60, 50, 6, 100)],
        )

    result = PlayerStateApplication(database).update_vitals("u", 30, 20)

    assert result.status == "applied"
    with sqlite3.connect(database) as connection:
        assert connection.execute(
            "SELECT hp,mp FROM user_xiuxian WHERE user_id=? ORDER BY rowid",
            ("u",),
        ).fetchall() == [(30, 20), (60, 50)]


def test_vital_update_uses_cas_and_missing_schema_is_closed(tmp_path):
    database = tmp_path / "player.db"
    _database(database)
    with sqlite3.connect(database) as connection:
        connection.execute(
            "INSERT INTO user_xiuxian(user_id,hp,mp,atk,exp) VALUES(?,?,?,?,?)",
            ("u", 80, 70, 8, 100),
        )

    result = PlayerStateApplication(database).update_vitals(
        "u", 30, 20, expected_hp=1, expected_mp=70
    )
    assert result.status == "state_changed"

    missing = tmp_path / "missing.db"
    missing.touch()
    calls = []
    result = PlayerStateApplication(missing).update_vitals(
        "u", 30, 20, fallback=lambda *args: calls.append(args)
    )
    assert result.status == "legacy_fallback"
    assert calls == [("u", 30, 20)]
    with sqlite3.connect(missing) as connection:
        assert connection.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall() == []
