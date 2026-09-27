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

    def test_sect_weekly_manager_defers_database_construction(self):
        source = (
            Path(__file__).parents[1]
            / "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_sect/sect_weekly.py"
        ).read_text(encoding="utf-8")
        self.assertIn("_sql_message_instance = None", source)
        self.assertIn("def _sql_message(", source)
        self.assertNotIn("self.sql_message = XiuxianDateManage()", source)
        self.assertNotIn("self.ensure_table()\n\n    @staticmethod", source)


if __name__ == "__main__":
    unittest.main()
