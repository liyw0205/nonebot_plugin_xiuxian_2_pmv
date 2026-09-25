import json
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


def test_rift_boss_skill_provider_reads_current_json_without_cache(tmp_path):
    skill_dir = tmp_path / "功法"
    skill_dir.mkdir()
    skill_path = skill_dir / "boss神通.json"
    skill_path.write_text(json.dumps({"14501": {"name": "first"}}), encoding="utf-8")

    with patch.object(riftmake, "get_paths", return_value=SimpleNamespace(data=tmp_path)):
        first = riftmake.get_rift_battle_boss_skill_data()
        skill_path.write_text(json.dumps({"14502": {"name": "second"}}), encoding="utf-8")
        second = riftmake.get_rift_battle_boss_skill_data()

    assert first == {"14501": {"name": "first"}}
    assert second == {"14502": {"name": "second"}}


def test_rift_boss_skill_provider_degrades_for_missing_or_invalid_json(tmp_path):
    with patch.object(riftmake, "get_paths", return_value=SimpleNamespace(data=tmp_path)):
        assert riftmake.get_rift_battle_boss_skill_data() == {}

        skill_dir = tmp_path / "功法"
        skill_dir.mkdir()
        (skill_dir / "boss神通.json").write_text("not-json", encoding="utf-8")
        assert riftmake.get_rift_battle_boss_skill_data() == {}


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
        buff_info_provider=riftmake.get_rift_battle_buff_info,
        item_provider=riftmake.items.get_data_by_item_id,
        accessory_provider=riftmake.get_rift_battle_accessory_data,
        tianti_provider=riftmake.get_rift_battle_tianti_data,
        base_provider=riftmake.get_rift_battle_base_attributes,
    )


def test_rift_accessory_provider_reads_existing_projection_without_schema_mutation(tmp_path):
    database = tmp_path / "player.db"
    equipped = '{"戒指": {"set_type": "烈阳", "affixes": []}}'
    with sqlite3.connect(database) as connection:
        connection.execute(
            "CREATE TABLE player_accessory (user_id TEXT PRIMARY KEY, equipped TEXT, bag TEXT)"
        )
        connection.execute(
            "INSERT INTO player_accessory VALUES (?, ?, ?)", ("u", equipped, "[]")
        )
        before = connection.execute(
            "SELECT type, name, sql FROM sqlite_master ORDER BY type, name"
        ).fetchall()

    with patch.object(riftmake, "get_paths", return_value=SimpleNamespace(player_db=database)):
        result = riftmake.get_rift_battle_accessory_data("u")
        missing = riftmake.get_rift_battle_accessory_data("missing")

    with sqlite3.connect(database) as connection:
        after = connection.execute(
            "SELECT type, name, sql FROM sqlite_master ORDER BY type, name"
        ).fetchall()
    assert result["equipped"]["戒指"]["set_type"] == "烈阳"
    assert result["bag"] == []
    assert missing is None
    assert before == after


def test_rift_tianti_provider_reads_existing_hp_without_schema_mutation(tmp_path):
    database = tmp_path / "player.db"
    with sqlite3.connect(database) as connection:
        connection.execute(
            "CREATE TABLE tianti_info (user_id TEXT PRIMARY KEY, tianti_hp TEXT)"
        )
        connection.execute("INSERT INTO tianti_info VALUES (?, ?)", ("u", "123"))
        before = connection.execute(
            "SELECT type, name, sql FROM sqlite_master ORDER BY type, name"
        ).fetchall()

    with patch.object(riftmake, "get_paths", return_value=SimpleNamespace(player_db=database)):
        result = riftmake.get_rift_battle_tianti_data("u")
        missing = riftmake.get_rift_battle_tianti_data("missing")

    with sqlite3.connect(database) as connection:
        after = connection.execute(
            "SELECT type, name, sql FROM sqlite_master ORDER BY type, name"
        ).fetchall()
    assert result["tianti_hp"] == "123"
    assert missing is None
    assert before == after


def test_rift_base_provider_reads_existing_profile_without_schema_mutation(tmp_path):
    database = tmp_path / "xiuxian.db"
    with sqlite3.connect(database) as connection:
        connection.execute(
            "CREATE TABLE user_xiuxian (user_id TEXT PRIMARY KEY, user_name TEXT, level TEXT, exp TEXT, stone TEXT, hp TEXT, mp TEXT, atk TEXT, atkpractice TEXT, hppractice TEXT, mppractice TEXT)"
        )
        connection.execute(
            "INSERT INTO user_xiuxian VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            ("u", "道友", "练气境", "100", "200", "80", "70", "30", "2", "3", "4"),
        )
        before = connection.execute(
            "SELECT type, name, sql FROM sqlite_master ORDER BY type, name"
        ).fetchall()

    with patch.object(riftmake, "get_paths", return_value=SimpleNamespace(game_db=database)):
        result = riftmake.get_rift_battle_base_attributes("u")
        missing = riftmake.get_rift_battle_base_attributes("missing")

    with sqlite3.connect(database) as connection:
        after = connection.execute(
            "SELECT type, name, sql FROM sqlite_master ORDER BY type, name"
        ).fetchall()
    assert result["nickname"] == "道友"
    assert result["base_hp"] == 80
    assert result["atkpractice"] == 2
    assert missing is None
    assert before == after


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


def test_rift_buff_provider_reads_existing_row_without_schema_mutation(tmp_path):
    database = tmp_path / "xiuxian.db"
    with sqlite3.connect(database) as connection:
        connection.execute(
            "CREATE TABLE BuffInfo (user_id TEXT PRIMARY KEY, main_buff INTEGER, faqi_buff INTEGER)"
        )
        connection.execute("INSERT INTO BuffInfo VALUES (?, ?, ?)", ("u", 11, 22))
        before = connection.execute(
            "SELECT type, name, sql FROM sqlite_master ORDER BY type, name"
        ).fetchall()

    with patch.object(riftmake, "get_paths", return_value=SimpleNamespace(game_db=database)):
        result = riftmake.get_rift_battle_buff_info("u")
        missing = riftmake.get_rift_battle_buff_info("missing")

    with sqlite3.connect(database) as connection:
        after = connection.execute(
            "SELECT type, name, sql FROM sqlite_master ORDER BY type, name"
        ).fetchall()
    assert result["main_buff"] == 11
    assert result["faqi_buff"] == 22
    assert missing is None
    assert before == after


def test_rift_final_attributes_passes_buff_and_item_read_providers():
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
        buff_info_provider=riftmake.get_rift_battle_buff_info,
        item_provider=riftmake.items.get_data_by_item_id,
        accessory_provider=riftmake.get_rift_battle_accessory_data,
        tianti_provider=riftmake.get_rift_battle_tianti_data,
        base_provider=riftmake.get_rift_battle_base_attributes,
    )
