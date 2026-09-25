import sqlite3
from types import SimpleNamespace
from unittest.mock import patch

import nonebot

nonebot.init()

from ....xiuxian.xiuxian_utils import player_fight
from ....xiuxian.xiuxian_rift import riftmake


def test_player_attribute_builder_uses_explicit_item_provider():
    class Buffs:
        BuffInfo = {"faqi_buff": 7}

        def __init__(self, _user_id):
            pass

    class Natal:
        def __init__(self, _user_id):
            pass

        def exists(self):
            return False

    final = {
        "user_id": "u",
        "nickname": "道友",
        "max_hp": 100,
        "current_hp": 90,
        "max_mp": 80,
        "current_mp": 70,
        "final_atk": 20,
        "exp": 100,
        "crit_rate": 0,
        "crit_damage": 1.5,
        "boss_damage_bonus": 0,
        "damage_reduction": 0,
        "armor_penetration": 0,
    }
    calls = []
    pet_calls = []
    attribute_calls = []
    natal_calls = []

    def lookup(item_id):
        calls.append(item_id)
        return {"name": "法器", "mp_buff": 3}

    def pet_lookup(user_id):
        pet_calls.append(user_id)
        return {"name": "灵宠"}

    def attribute_lookup(user_id, *, ratio, include_current):
        attribute_calls.append((user_id, ratio, include_current))
        return final

    def natal_lookup(user_id):
        natal_calls.append(user_id)
        return {"name": "本命法宝"}

    with patch.object(player_fight, "UserBuffDate", Buffs), patch.object(
        player_fight, "get_final_attributes", return_value=final
    ), patch.object(player_fight, "NatalTreasure", Natal), patch.object(
        player_fight, "get_user_pet_for_battle", return_value=None
    ):
        result = player_fight.get_players_attributes(
            "u",
            level_ratios={"u": 0.5},
            item_provider=lookup,
            pet_provider=pet_lookup,
            attribute_provider=attribute_lookup,
            natal_provider=natal_lookup,
        )

    assert calls == [7]
    assert pet_calls == ["u"]
    assert attribute_calls == [("u", 0.5, True)]
    assert natal_calls == ["u"]
    assert result["本命法宝"]["name"] == "本命法宝"
    assert result["宠物"]["name"] == "灵宠"
    assert result["法器"]["mp_buff"] == 3


def test_rift_natal_provider_reads_awakened_row_without_schema_mutation(tmp_path):
    database = tmp_path / "player.db"
    with sqlite3.connect(database) as connection:
        connection.execute(
            "CREATE TABLE natal_treasure (user_id TEXT PRIMARY KEY, form INTEGER, name TEXT, level TEXT, soul_summon_count TEXT)"
        )
        connection.execute(
            "INSERT INTO natal_treasure VALUES (?, ?, ?, ?, ?)",
            ("u", 1, "玄一", "3", '{"ally": 2}'),
        )
        before = connection.execute(
            "SELECT type, name, sql FROM sqlite_master ORDER BY type, name"
        ).fetchall()

    with patch.object(riftmake, "get_paths", return_value=SimpleNamespace(player_db=database)):
        result = riftmake.get_rift_battle_natal_data("u")

    with sqlite3.connect(database) as connection:
        after = connection.execute(
            "SELECT type, name, sql FROM sqlite_master ORDER BY type, name"
        ).fetchall()
    assert result["form"] == 1
    assert result["level"] == 3
    assert result["soul_summon_count"] == {"ally": 2}
    assert before == after


def test_rift_natal_provider_returns_none_for_unawakened_or_missing_table(tmp_path):
    database = tmp_path / "player.db"
    with sqlite3.connect(database) as connection:
        connection.execute(
            "CREATE TABLE natal_treasure (user_id TEXT PRIMARY KEY, form INTEGER)"
        )
        connection.execute("INSERT INTO natal_treasure VALUES (?, ?)", ("u", 0))

    with patch.object(riftmake, "get_paths", return_value=SimpleNamespace(player_db=database)):
        assert riftmake.get_rift_battle_natal_data("u") is None

    missing = tmp_path / "missing.db"
    with patch.object(riftmake, "get_paths", return_value=SimpleNamespace(player_db=missing)):
        assert riftmake.get_rift_battle_natal_data("u") is None

    empty = tmp_path / "empty.db"
    sqlite3.connect(empty).close()
    with patch.object(riftmake, "get_paths", return_value=SimpleNamespace(player_db=empty)):
        assert riftmake.get_rift_battle_natal_data("u") is None
    with sqlite3.connect(empty) as connection:
        assert connection.execute(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table'"
        ).fetchone()[0] == 0


def test_rift_attribute_provider_injects_read_only_impart_provider():
    final = {"user_id": "u", "final_atk": 10}
    with patch.object(riftmake, "get_final_attributes", return_value=final) as calculate:
        result = riftmake.get_rift_battle_final_attributes(
            "u", ratio=0.5, include_current=True
        )

    assert result is final
    calculate.assert_called_once_with(
        "u",
        ratio=0.5,
        include_current=True,
        impart_provider=riftmake.get_rift_battle_impart_data,
    )


def test_rift_impart_provider_reads_existing_row_without_creating_user(tmp_path):
    database = tmp_path / "xiuxian_impart.db"
    with sqlite3.connect(database) as connection:
        connection.execute(
            "CREATE TABLE xiuxian_impart (user_id TEXT PRIMARY KEY, impart_atk_per REAL, boss_atk REAL)"
        )
        connection.execute(
            "INSERT INTO xiuxian_impart VALUES (?, ?, ?)", ("u", 0.25, 0.4)
        )
        before = connection.execute(
            "SELECT type, name, sql FROM sqlite_master ORDER BY type, name"
        ).fetchall()

    paths = SimpleNamespace(impart_db=database)
    with patch.object(riftmake, "get_paths", return_value=paths):
        result = riftmake.get_rift_battle_impart_data("u")
        missing = riftmake.get_rift_battle_impart_data("missing")

    with sqlite3.connect(database) as connection:
        after = connection.execute(
            "SELECT type, name, sql FROM sqlite_master ORDER BY type, name"
        ).fetchall()
    assert result["impart_atk_per"] == 0.25
    assert result["boss_atk"] == 0.4
    assert missing is None
    assert before == after
