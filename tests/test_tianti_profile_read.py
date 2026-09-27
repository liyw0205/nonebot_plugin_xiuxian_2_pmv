from __future__ import annotations

import unittest
from unittest.mock import Mock, patch

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_tianti.tianti_data import TiantiDataManager
from scripts.check_full_refactor_progress import _slice_status


class TiantiProfileReadTests(unittest.TestCase):
    def test_default_profile_paths_have_a_shared_feature_writer(self):
        tianti = _slice_status()["tianti"]
        self.assertTrue(tianti["settlement_command_application_owned"])
        self.assertTrue(tianti["training_command_application_owned"])
        self.assertTrue(tianti["default_facade_has_no_profile_manager"])
        self.assertTrue(tianti["stone_repository_has_no_legacy_manager_injection"])
        self.assertTrue(tianti["profile_upserts_share_feature_writer"])
        self.assertTrue(tianti["settlement_default_repository_is_feature_owned"])
        self.assertTrue(tianti["gain_display_rules_are_feature_owned"])
        self.assertTrue(tianti["settlement_and_display_share_rules"])
        self.assertTrue(tianti["profile_cap_query_feature_owned"])
        self.assertTrue(tianti["sect_bonus_display_is_feature_owned"])
        self.assertTrue(tianti["legacy_profile_write_through_is_named"])
        self.assertTrue(tianti["legacy_transaction_adapters_remain_explicit"])

    def test_read_only_profile_lookup_does_not_persist_missing_default(self):
        manager = TiantiDataManager()
        store = Mock()
        store.get_fields.return_value = None
        manager.save_user_tianti_info = Mock()
        with (
            patch(
                "nonebot_plugin_xiuxian_2.xiuxian.xiuxian_tianti.tianti_data._player_data_manager",
                return_value=store,
            ),
            patch.object(manager, "_default", return_value={"tianti_hp": 0}),
        ):
            self.assertEqual(manager.read_user_tianti_info("u"), {"tianti_hp": 0})
        manager.save_user_tianti_info.assert_not_called()
        store.get_fields.assert_called_once_with("u", manager.TABLE)

    def test_legacy_getter_keeps_default_initialization_behavior(self):
        manager = TiantiDataManager()
        profile = {"tianti_hp": 0}
        manager.read_user_tianti_info = Mock(return_value=profile)
        manager.save_user_tianti_info = Mock()
        self.assertEqual(manager.get_user_tianti_info("u"), profile)
        manager.save_user_tianti_info.assert_called_once_with("u", profile)

if __name__ == "__main__":
    unittest.main()
