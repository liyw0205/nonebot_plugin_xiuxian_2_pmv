from __future__ import annotations

import unittest
from unittest.mock import Mock, patch

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_tianti.tianti_data import TiantiDataManager


class TiantiProfileReadTests(unittest.TestCase):
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
