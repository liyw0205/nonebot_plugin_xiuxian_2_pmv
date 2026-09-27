from pathlib import Path
import unittest


class SectStateManagerLazyTests(unittest.TestCase):
    def test_sect_task_manager_delegates_without_database_construction(self):
        source = (
            Path(__file__).parents[1]
            / "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_sect/sect_tasks.py"
        ).read_text(encoding="utf-8")
        self.assertNotIn("XiuxianDateManage", source)
        self.assertNotIn("CREATE TABLE", source)
        self.assertIn("return self._application().get_active_task(user_id)", source)
        self.assertIn("return self._application().accept_task(user_id, sect_id, task_config)", source)

    def test_sect_weekly_manager_has_no_legacy_database_constructor(self):
        source = (
            Path(__file__).parents[1]
            / "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_sect/sect_weekly.py"
        ).read_text(encoding="utf-8")
        self.assertNotIn("XiuxianDateManage", source)
        self.assertNotIn("def _sql_message(", source)
        self.assertIn("def ensure_table(self)", source)
        self.assertIn("_sect_application().assert_weekly_progress_schema()", source)
        self.assertIn("self.sql_message = None", source)


if __name__ == "__main__":
    unittest.main()
