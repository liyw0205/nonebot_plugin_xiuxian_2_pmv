from pathlib import Path
import unittest


class SectFacadeLazyReaderTests(unittest.TestCase):
    def test_sect_facade_does_not_construct_legacy_sql_manager(self):
        source = (
            Path(__file__).parents[1]
            / "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_sect/__init__.py"
        ).read_text(encoding="utf-8")
        self.assertNotIn("XiuxianDateManage", source)
        self.assertNotIn("_sql_message(", source)
        self.assertIn("sect_application.get_sect_info(", source)
        self.assertIn("sect_task_state_manager.bind_application(sect_application)", source)

    def test_sect_member_binding_requires_application(self):
        helper_source = (
            Path(__file__).parents[1]
            / "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_sect/sect_member_utils.py"
        ).read_text(encoding="utf-8")
        facade_source = (
            Path(__file__).parents[1]
            / "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_sect/__init__.py"
        ).read_text(encoding="utf-8")
        self.assertIn('if sect_app is None:', helper_source)
        self.assertIn('raise ValueError("sect_app is required")', helper_source)
        self.assertNotIn("sql_manager", helper_source)
        self.assertIn("sect_app=sect_application", facade_source)


if __name__ == "__main__":
    unittest.main()
