from pathlib import Path
import unittest


class WorkFacadeLazyReaderTests(unittest.TestCase):
    def test_work_facade_defers_sql_manager_construction(self):
        source = (
            Path(__file__).parents[1]
            / "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_work/__init__.py"
        ).read_text(encoding="utf-8")
        self.assertIn("_sql_message_instance = None", source)
        self.assertIn("def _sql_message(", source)
        self.assertIn("_sql_message().get_user_cd(", source)
        self.assertIn("_sql_message().get_work_num(", source)
        self.assertIn("_sql_message().update_last_check_info_time(", source)
        self.assertNotIn("sql_message = XiuxianDateManage()", source)


if __name__ == "__main__":
    unittest.main()
