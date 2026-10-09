from pathlib import Path
import unittest


class BossFacadeLazyReaderTests(unittest.TestCase):
    def test_boss_facade_defers_sql_manager_construction(self):
        source = (
            Path(__file__).parents[1]
            / "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_boss/__init__.py"
        ).read_text(encoding="utf-8")
        self.assertIn("_sql_message_instance = None", source)
        self.assertIn("def _sql_message(", source)
        self.assertIn("_sql_message().update_user_hp(", source)
        self.assertIn("get_last_check_info_time(", source)
        self.assertNotIn("_sql_message().get_last_check_info_time(", source)
        self.assertIn("_sql_message().get_player_data(", source)
        self.assertNotIn("sql_message = XiuxianDateManage()", source)

    def test_boss_player_state_defaults_to_composition_owner_without_legacy_fallback(self):
        root = Path(__file__).parents[1]
        source = (root / "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_boss/__init__.py").read_text(encoding="utf-8")
        plugin = (root / "nonebot_plugin_xiuxian_2/plugin.py").read_text(encoding="utf-8")
        helper = source[source.index("def _initialize_player_state("):source.index("def _legacy_initialize_player_state(")]
        self.assertIn("PlayerStateApplication(get_paths().game_db)", source)
        self.assertIn("def configure_player_state_application(", source)
        self.assertIn("configure_boss_player_state_application(context.services[\"player_state\"])", plugin)
        self.assertIn("initialize_if_empty(user_id)", helper)
        self.assertNotIn("fallback=", helper)
        self.assertIn("def _legacy_initialize_player_state(", source)


if __name__ == "__main__":
    unittest.main()
