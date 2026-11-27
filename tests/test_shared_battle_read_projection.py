from __future__ import annotations

import json
import sqlite3
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import Mock, patch


def _modules():
    import nonebot

    try:
        nonebot.get_driver()
    except ValueError:
        nonebot.init(command_start={"", "/"}, log_level="ERROR")
    from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_natal_treasure import natal_data
    from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_rift import riftmake
    from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils import player_fight, xiuxian2_handle

    return player_fight, xiuxian2_handle, natal_data, riftmake


def _profile(user_id="u"):
    return {
        "user_id": user_id, "user_name": "Player", "level": "江湖好手",
        "exp": 1000, "stone": 2000, "hp": 400, "mp": 500, "atk": 100,
        "atkpractice": 0, "hppractice": 0, "mppractice": 0, "power": 9000,
    }


def _final(user_id="u"):
    return {
        "user_id": user_id, "nickname": "Player", "max_hp": 500,
        "current_hp": 400, "max_mp": 1000, "current_mp": 500,
        "final_atk": 100, "exp": 1000, "crit_rate": 0,
        "crit_damage": 1.5, "boss_damage_bonus": 0,
        "damage_reduction": 0, "armor_penetration": 0,
    }


class SharedBattleReadProjectionTests(unittest.TestCase):
    def test_default_builder_reuses_buff_row_and_next_call_reads_fresh(self):
        battle, handle, _, _ = _modules()
        snapshots = [{"faqi_buff": 1}, {"faqi_buff": 2}, {"faqi_buff": 3}]
        buff_reader = Mock(side_effect=snapshots)
        catalog = {
            1: {"atk_buff": 0.1, "mp_buff": 1},
            2: {"atk_buff": 0.2, "mp_buff": 2},
            3: {"atk_buff": 0.3, "mp_buff": 3},
        }

        def attributes(user_id, *, ratio, include_current, **providers):
            return handle.get_final_attributes(
                user_id, ratio=ratio, include_current=include_current,
                base_provider=lambda uid: handle.get_base_attributes(uid, user_info=_profile(uid)),
                item_provider=catalog.get, impart_provider=lambda uid: {},
                accessory_provider=lambda uid: {"equipped": {}},
                tianti_provider=lambda uid: {}, **providers,
            )

        with (
            patch.object(battle, "get_player_attributes", side_effect=attributes),
            patch.object(handle, "get_user_buff", side_effect=AssertionError("duplicate buff read")),
        ):
            results = [
                battle.get_players_attributes(
                    uid, item_provider=catalog.get, buff_info_provider=buff_reader,
                    natal_provider=lambda uid: None, pet_provider=lambda uid: None,
                )
                for uid in ("u", "v", "u")
            ]

        self.assertEqual([110, 120, 130], [item["属性"]["attack"] for item in results])
        self.assertEqual([1, 2, 3], [item["属性"]["mp_cost_modifier"] for item in results])
        self.assertEqual(["u", "v", "u"], [call.args[0] for call in buff_reader.call_args_list])
        for result, snapshot in zip(results, snapshots):
            self.assertIs(snapshot, result["其他"])

    def test_custom_attribute_provider_retains_narrow_signature(self):
        battle, _, _, _ = _modules()
        calls = []

        def attributes(user_id, *, ratio, include_current):
            calls.append((user_id, ratio, include_current))
            return _final(user_id)

        result = battle.get_players_attributes(
            "u", {"u": 0.5}, attribute_provider=attributes,
            buff_info_provider=lambda uid: {}, natal_provider=lambda uid: None,
            pet_provider=lambda uid: None,
        )
        self.assertEqual([("u", 0.5, True)], calls)
        self.assertEqual(100, result["属性"]["attack"])

    def test_buff_read_failure_prevents_attribute_and_asset_reads(self):
        battle, _, _, _ = _modules()
        reader = Mock(side_effect=sqlite3.OperationalError("buff denied"))
        with (
            patch.object(battle, "get_player_attributes") as attributes,
            patch.object(battle, "get_natal_data_for_battle") as natal,
            self.assertRaisesRegex(sqlite3.OperationalError, "buff denied"),
        ):
            battle.get_players_attributes("u", buff_info_provider=reader)
        attributes.assert_not_called()
        natal.assert_not_called()

    def test_final_user_data_reuses_full_row_and_keeps_unmodified_fields(self):
        _, handle, _, _ = _modules()
        profile = _profile()
        base_rows = []

        def calculate(user_id, *, ratio, include_current, base_provider):
            base_rows.append(base_provider(user_id))
            return {"current_hp": 321, "current_mp": 456, "final_atk": 123}

        with (
            patch.object(handle, "get_final_attributes", side_effect=calculate),
            patch.object(handle, "sql_message") as manager,
        ):
            result = handle.final_user_data(
                tuple(profile.values()), [(key,) for key in profile]
            )
        manager.get_user_info_with_id.assert_not_called()
        self.assertEqual(400, base_rows[0]["base_hp"])
        self.assertEqual(100, base_rows[0]["base_atk"])
        self.assertEqual({**profile, "hp": 321, "mp": 456, "atk": 123}, result)

    def test_legacy_player_data_uses_one_buff_row_and_only_required_impart(self):
        _, handle, _, _ = _modules()
        expected = {
            "user_id": "u", "道号": "Player", "气血": 400, "攻击": 100,
            "真元": 500, "会心": 25, "防御": 0, "exp": 1000,
        }
        with (
            patch.object(handle, "sql_message") as manager,
            patch.object(handle, "xiuxian_impart") as impart,
            patch.object(handle, "items") as items,
        ):
            manager.get_user_real_info.return_value = _profile()
            manager.get_user_buff_info.return_value = {"main_buff": 1, "armor_buff": 2}
            items.get_data_by_item_id.side_effect = lambda item_id: {
                1: {"crit_buff": 0.1}, 2: {"crit_buff": 0.15}
            }[item_id]
            result = handle.XiuxianDateManage.get_player_data(manager, "u")
            self.assertEqual(expected, result)
            manager.get_user_buff_info.assert_called_once_with("u")
            impart.get_user_impart_info_with_id.assert_not_called()

            manager.get_user_buff_info.reset_mock()
            impart.get_user_impart_info_with_id.return_value = {"boss_atk": 0.5}
            boosted = handle.XiuxianDateManage.get_player_data(manager, "u", boss=True)
            self.assertEqual({**expected, "攻击": 150}, boosted)
            manager.get_user_buff_info.assert_called_once_with("u")
            impart.get_user_impart_info_with_id.assert_called_once_with("u")

    def test_rift_assets_reuse_read_only_buff_provider_without_default_fallback(self):
        battle, handle, _, rift = _modules()
        reader = Mock(side_effect=[{"faqi_buff": 7}, {"faqi_buff": 8}])
        captured = []

        def calculate(user_id, **providers):
            captured.append(providers)
            info = providers["buff_info_provider"](user_id)
            result = _final(user_id)
            result["final_atk"] = info["faqi_buff"]
            return result

        with (
            patch.object(rift, "get_rift_battle_buff_info", reader),
            patch.object(rift, "get_final_attributes", side_effect=calculate),
            patch.object(rift, "get_rift_battle_item_data", return_value={"mp_buff": 2}),
            patch.object(rift, "get_rift_battle_natal_data", return_value=None),
            patch.object(rift, "get_user_pet_for_battle", return_value=None),
            patch.object(battle, "UserBuffDate", side_effect=AssertionError("mutable buff fallback")),
            patch.object(handle, "sql_message", side_effect=AssertionError("legacy fallback")),
        ):
            first = rift.get_rift_battle_player_assets("u")
            second = rift.get_rift_battle_player_assets("u")
        self.assertEqual([7, 8], [first["属性"]["attack"], second["属性"]["attack"]])
        self.assertEqual(2, reader.call_count)
        for providers in captured:
            self.assertIs(rift.get_rift_battle_impart_data, providers["impart_provider"])
            self.assertIs(rift.get_rift_battle_accessory_data, providers["accessory_provider"])
            self.assertIs(rift.get_rift_battle_base_attributes, providers["base_provider"])


class NatalBattleProjectionTests(unittest.TestCase):
    def setUp(self):
        _, _, self.natal, _ = _modules()
        from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils import player_data_manager

        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        manager = object.__new__(player_data_manager.PlayerDataManager)
        manager.database_path = Path(self.temp.name) / "player.db"
        manager.conn = player_data_manager.db_backend.connect(manager.database_path)
        self.addCleanup(manager.conn.close)
        manager._conn_lock = threading.RLock()
        manager._ensured_tables = set()
        manager._ensured_fields = set()
        manager._field_list_cache = {}
        manager._field_list_cache_charged_bytes = 0
        self.manager = manager
        binding = patch.object(self.natal, "_player_data_manager", return_value=manager)
        binding.start()
        self.addCleanup(binding.stop)

    def _seed_complete(self, user_id="u", *, form=1):
        treasure = self.natal.NatalTreasure(user_id)
        treasure._ensure_record()
        self.manager.update_or_write_data(user_id, treasure.table, "form", form, data_type="INTEGER")
        return treasure

    def test_awakened_projection_reads_once_and_returns_all_combat_effects(self):
        treasure = self._seed_complete()
        self.manager.update_or_write_data("u", treasure.table, "effect1_base_value", 0.5, data_type="REAL")
        self.manager.update_or_write_data("u", treasure.table, "effect1_level", 3, data_type="INTEGER")
        before = self.manager.conn.execute("SELECT * FROM natal_treasure").fetchone()
        with (
            patch.object(self.manager, "get_fields", wraps=self.manager.get_fields) as reads,
            patch.object(self.manager, "get_field_data", wraps=self.manager.get_field_data) as scalar_reads,
        ):
            data = treasure.read_battle_data()
        self.assertEqual(1, reads.call_count)
        scalar_reads.assert_not_called()
        self.assertEqual(0.5, data["effect1_base_value"])
        self.assertEqual(3, data["effect1_level"])
        self.assertEqual({}, data["soul_summon_count"])
        self.assertEqual(before, self.manager.conn.execute("SELECT * FROM natal_treasure").fetchone())
        self.manager.update_or_write_data("u", treasure.table, "level", 9, data_type="INTEGER")
        self.assertEqual(9, treasure.read_battle_data()["level"])

    def test_missing_row_and_columns_are_repaired_and_read_back(self):
        treasure = self.natal.NatalTreasure("u")
        self.assertIsNone(treasure.read_battle_data())
        self.assertEqual(0, self.manager.get_fields("u", treasure.table)["form"])
        self.manager.conn.execute("CREATE TABLE partial(user_id TEXT PRIMARY KEY,form INTEGER)")
        self.manager.conn.execute("INSERT INTO partial VALUES('v',1)")
        self.manager.conn.commit()
        treasure = self.natal.NatalTreasure("v")
        treasure.table = "partial"
        with patch.object(self.manager, "get_fields", wraps=self.manager.get_fields) as reads:
            data = treasure.read_battle_data()
        self.assertEqual(2, reads.call_count)
        self.assertEqual(1, data["form"])
        self.assertEqual({}, data["enlightenment_count"])
        self.assertIn("effect3_level", data)
        self.assertEqual([("ok",)], self.manager.conn.execute("PRAGMA integrity_check").fetchall())

    def test_unawakened_invalid_combat_json_is_not_decoded(self):
        treasure = self._seed_complete(form=0)
        self.manager.update_or_write_data("u", treasure.table, "soul_summon_count", "broken-json")
        self.assertIsNone(treasure.read_battle_data())
        self.manager.update_or_write_data("u", treasure.table, "form", 1, data_type="INTEGER")
        with self.assertRaises(json.JSONDecodeError):
            treasure.read_battle_data()

    def test_failed_repair_is_read_back_instead_of_assumed_successful(self):
        treasure = self.natal.NatalTreasure("u")
        manager = Mock()
        manager.get_fields.side_effect = [None, None]
        with patch.object(self.natal, "_player_data_manager", return_value=manager):
            self.assertIsNone(treasure.read_battle_data())
        self.assertEqual(2, manager.get_fields.call_count)

    def test_read_exception_propagates_and_next_call_can_recover(self):
        treasure = self._seed_complete()
        self.manager.conn._raw.set_authorizer(
            lambda action, table, *_: sqlite3.SQLITE_DENY
            if action == sqlite3.SQLITE_READ and table == treasure.table
            else sqlite3.SQLITE_OK
        )
        try:
            with self.assertRaises(sqlite3.DatabaseError):
                treasure.read_battle_data()
        finally:
            self.manager.conn._raw.set_authorizer(None)
        self.assertEqual(1, treasure.read_battle_data()["form"])


if __name__ == "__main__":
    unittest.main()
