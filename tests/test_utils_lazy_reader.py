from pathlib import Path
import unittest


class UtilsLazyReaderTests(unittest.TestCase):
    def test_utils_defers_sql_manager_construction(self):
        source = (
            Path(__file__).parents[1]
            / "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_utils/utils.py"
        ).read_text(encoding="utf-8")
        self.assertIn("_sql_message_instance = None", source)
        self.assertIn("def _sql_message(", source)
        self.assertIn("_sql_message().get_user_cd(", source)
        self.assertIn("_sql_message().get_user_info_with_id(", source)
        self.assertNotIn("sql_message = XiuxianDateManage()", source)
        self.assertIn("_player_data_manager_instance = None", source)
        self.assertIn("def _player_data_manager(", source)
        self.assertNotIn("player_data_manager = PlayerDataManager()", source)
        self.assertIn("_player_data_manager().get_field_data(", source)
        self.assertIn("_player_data_manager().get_fields(", source)
        self.assertIn("_player_data_manager().update_or_write_data(", source)


if __name__ == "__main__":
    unittest.main()
