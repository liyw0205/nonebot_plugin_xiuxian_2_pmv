from pathlib import Path
import unittest


class LayoutLazyReaderTests(unittest.TestCase):
    def test_layout_defers_sql_manager_construction(self):
        source = (
            Path(__file__).parents[1]
            / "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_utils/lay_out.py"
        ).read_text(encoding="utf-8")
        self.assertNotIn("_sql_message_instance", source)
        self.assertNotIn("XiuxianDateManage", source)
        self.assertIn("recover_player_stamina(", source)
        self.assertIn("get_user_profile(", source)
        self.assertIn("consume_player_stamina(", source)
        self.assertNotIn("_sql_message().update_user_stamina(", source)


if __name__ == "__main__":
    unittest.main()
