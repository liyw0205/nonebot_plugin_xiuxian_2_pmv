from pathlib import Path
import unittest


class SectMemberUtilsLazyReaderTests(unittest.TestCase):
    def test_sect_member_utils_defers_default_sql_manager(self):
        source = (
            Path(__file__).parents[1]
            / "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_sect/sect_member_utils.py"
        ).read_text(encoding="utf-8")
        self.assertIn("_sql_message_instance = None", source)
        self.assertIn("def _sql_message(", source)
        self.assertNotIn("sql_message = XiuxianDateManage()", source)
        self.assertIn("if sql_manager is not None:", source)
        self.assertIn("sql_message = sql_manager", source)
        self.assertIn("if sect_app is not None:", source)
        self.assertIn("sect_application = sect_app", source)
        self.assertIn("sect_application.get_user_profile(user_id)", source)
        self.assertIn("_sql_message().get_user_info_with_id(user_id)", source)


if __name__ == "__main__":
    unittest.main()
