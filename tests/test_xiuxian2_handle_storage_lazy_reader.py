from pathlib import Path
import unittest


class Xiuxian2HandleStorageLazyReaderTests(unittest.TestCase):
    def test_core_manager_aliases_defer_construction(self):
        source = (
            Path(__file__).parents[1]
            / "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_utils/xiuxian2_handle.py"
        ).read_text(encoding="utf-8")
        self.assertIn("_sql_message_instance = None", source)
        self.assertIn("_player_data_manager_instance = None", source)
        self.assertNotIn("sql_message = XiuxianDateManage()", source)
        self.assertNotIn("player_data_manager = PlayerDataManager()", source)
        self.assertIn("class _LazyManagerProxy", source)
        self.assertIn("sql_message = _LazyManagerProxy(_sql_message)", source)
        self.assertIn("player_data_manager = _LazyManagerProxy(_player_data_manager)", source)


if __name__ == "__main__":
    unittest.main()
