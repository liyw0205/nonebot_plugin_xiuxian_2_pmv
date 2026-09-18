from pathlib import Path
import unittest


class BaseFacadeLazyReaderTests(unittest.TestCase):
    def test_base_facade_defers_sql_manager_construction(self):
        source = (
            Path(__file__).parents[1]
            / "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_base/__init__.py"
        ).read_text(encoding="utf-8")
        self.assertIn("_sql_message_instance = None", source)
        self.assertIn("def _sql_message(", source)
        self.assertIn("_sql_message().get_user_info_with_id(", source)
        self.assertIn("_sql_message().power_top(", source)
        self.assertNotIn("sql_message = XiuxianDateManage()", source)
        self.assertIn("_player_data_manager_instance = None", source)
        self.assertIn("def _player_data_manager(", source)
        self.assertNotIn("player_data_manager = PlayerDataManager()", source)
        self.assertIn("_player_data_manager().get_field_data(", source)
        self.assertIn("RegistrationBatcher(_sql_message)", source)

    def test_registration_batcher_accepts_lazy_manager_resolver(self):
        source = (
            Path(__file__).parents[1]
            / "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_base/registration_batch.py"
        ).read_text(encoding="utf-8")
        self.assertIn("self._manager_resolver", source)
        self.assertIn("self._manager_resolver()", source)


if __name__ == "__main__":
    unittest.main()
