from pathlib import Path
import unittest


class TowerLazyReaderTests(unittest.TestCase):
    def test_tower_facade_defers_sql_manager_construction(self):
        source = (
            Path(__file__).parents[1]
            / "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_tower/__init__.py"
        ).read_text(encoding="utf-8")
        self.assertIn("_sql_message_instance = None", source)
        self.assertIn("def _sql_message(", source)
        self.assertNotIn("sql_message = XiuxianDateManage()", source)
        self.assertIn("_sql_message().update_user_hp(", source)
        self.assertIn("_sql_message().update_user_stamina(", source)
        self.assertIn("_sql_message().get_user_info_with_id(", source)


if __name__ == "__main__":
    unittest.main()
