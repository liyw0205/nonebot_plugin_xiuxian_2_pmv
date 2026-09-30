from __future__ import annotations

import sqlite3
from pathlib import Path

from nonebot_plugin_xiuxian_2.features.base.economy_application import PlayerEconomyApplication


def _create_users(database: Path, *, contribution: bool = True) -> None:
    columns = "user_id TEXT, stone INTEGER, exp INTEGER"
    if contribution:
        columns += ", sect_contribution INTEGER"
    with sqlite3.connect(database) as connection:
        connection.execute(f"CREATE TABLE user_xiuxian ({columns})")
        connection.execute(
            "INSERT INTO user_xiuxian(user_id,stone,exp,sect_contribution) "
            "VALUES('u',10,8,3)"
            if contribution
            else "INSERT INTO user_xiuxian(user_id,stone,exp) VALUES('u',10,8)"
        )
        connection.commit()


def test_player_economy_grants_are_cas_and_capped(tmp_path: Path) -> None:
    database = tmp_path / "game.sqlite3"
    _create_users(database)
    application = PlayerEconomyApplication(database)

    stone = application.grant_stone("u", 5)
    experience = application.grant_experience(
        "u", 10, max_exp=10, expected_exp=8
    )
    contribution = application.grant_sect_contribution(
        "u", 4, expected_value=3
    )

    assert (stone.status, stone.applied, stone.value) == ("applied", 5, 15)
    assert (experience.status, experience.applied, experience.value) == (
        "applied",
        2,
        10,
    )
    assert (contribution.status, contribution.applied, contribution.value) == (
        "applied",
        4,
        7,
    )
    with sqlite3.connect(database) as connection:
        assert connection.execute(
            "SELECT stone,exp,sect_contribution FROM user_xiuxian WHERE user_id='u'"
        ).fetchone() == (15, 10, 7)

    stale = application.grant_experience("u", 1, max_exp=20, expected_exp=8)
    assert stale.status == "state_changed"
    assert stale.value == 10


def test_player_economy_uses_first_duplicate_user_row(tmp_path: Path) -> None:
    database = tmp_path / "game.sqlite3"
    _create_users(database)
    with sqlite3.connect(database) as connection:
        connection.execute(
            "INSERT INTO user_xiuxian(user_id,stone,exp,sect_contribution) "
            "VALUES('u',100,8,3)"
        )
        connection.commit()

    result = PlayerEconomyApplication(database).grant_stone("u", 2)

    assert (result.status, result.value) == ("applied", 12)
    with sqlite3.connect(database) as connection:
        assert connection.execute(
            "SELECT stone FROM user_xiuxian ORDER BY rowid"
        ).fetchall() == [(12,), (100,)]


def test_player_economy_fails_closed_without_schema_or_user(tmp_path: Path) -> None:
    missing_database = tmp_path / "missing.sqlite3"
    missing = PlayerEconomyApplication(missing_database).grant_stone("u", 1)
    assert missing.status == "schema_missing"
    assert not missing_database.exists()

    database = tmp_path / "unready.sqlite3"
    with sqlite3.connect(database) as connection:
        connection.execute("CREATE TABLE user_xiuxian(user_id TEXT)")
        connection.commit()
    unready = PlayerEconomyApplication(database).grant_stone("u", 1)
    assert unready.status == "schema_missing"
    with sqlite3.connect(database) as connection:
        assert [row[1] for row in connection.execute("PRAGMA table_info(user_xiuxian)")] == [
            "user_id"
        ]

    ready_database = tmp_path / "ready.sqlite3"
    _create_users(ready_database)
    absent = PlayerEconomyApplication(ready_database).grant_stone("absent", 1)
    assert absent.status == "user_missing"


def test_reward_and_compatibility_sources_no_longer_write_player_economy_directly() -> None:
    root = Path(__file__).parents[1] / "nonebot_plugin_xiuxian_2/xiuxian"
    sources = [
        (root / "xiuxian_utils/reward_service.py").read_text(encoding="utf-8"),
        (root / "xiuxian_compensation/common.py").read_text(encoding="utf-8"),
        (root / "xiuxian_buff/partner.py").read_text(encoding="utf-8"),
    ]
    for source in sources:
        assert ".update_ls(" not in source
        assert ".update_exp(" not in source
        assert ".update_user_sect_contribution(" not in source
