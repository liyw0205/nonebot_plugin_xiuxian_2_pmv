from pathlib import Path
import unittest


class SectMemberUtilsLazyReaderTests(unittest.TestCase):
    def test_sect_member_utils_requires_feature_application(self):
        source = (
            Path(__file__).parents[1]
            / "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_sect/sect_member_utils.py"
        ).read_text(encoding="utf-8")
        self.assertNotIn("XiuxianDateManage", source)
        self.assertNotIn("_sql_message(", source)
        self.assertIn('if sect_app is None:', source)
        self.assertIn('raise ValueError("sect_app is required")', source)
        self.assertIn("sect_application = sect_app", source)
        self.assertIn("sect_application.get_user_profile(user_id)", source)
        self.assertNotIn("sect_task_state_manager", source)


if __name__ == "__main__":
    unittest.main()
