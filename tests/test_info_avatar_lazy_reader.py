from pathlib import Path
import unittest


class AvatarLazyReaderTests(unittest.TestCase):
    def test_avatar_facade_uses_profile_and_avatar_applications(self):
        source = (
            Path(__file__).parents[1]
            / "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_info/avatar.py"
        ).read_text(encoding="utf-8")
        self.assertIn("get_user_profile(main_id)", source)
        self.assertIn("get_user_profile(str(avatar_id))", source)
        self.assertIn("get_user_profile(new_id)", source)
        self.assertNotIn("XiuxianDateManage", source)
        self.assertNotIn("get_user_info_with_id", source)
        self.assertIn("_player_avatar_application().get_active_user_id(", source)
        self.assertIn("_player_avatar_application().get_avatar_info(", source)
        self.assertIn("_player_avatar_application().initialize(", source)
        self.assertNotIn("PlayerDataManager", source)
        self.assertNotIn("_run_info_action(", source)
        self.assertIn("toggle_active(", source)
        self.assertIn("restore_active(", source)
        self.assertNotIn('_player_data_manager().update_or_write_data(main_id, "avatar", "active_id"', source)

    def test_my_id_handler_uses_avatar_application_for_identity_read(self):
        source = (
            Path(__file__).parents[1]
            / "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_info/avatar.py"
        ).read_text(encoding="utf-8")
        start = source.index("async def my_id_cmd_(")
        end = source.index("def _generate_unique_avatar_id", start)
        handler = source[start:end]
        self.assertIn("get_impersonating_target(real_user_id)", handler)
        self.assertIn("_player_avatar_application().get_active_user_id(real_user_id)", handler)
        self.assertIn("await handle_send(", handler)
        self.assertNotIn("_sql_message()", handler)
        self.assertNotIn("get_user_info_with_id", handler)


if __name__ == "__main__":
    unittest.main()
