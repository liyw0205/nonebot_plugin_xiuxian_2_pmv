from pathlib import Path
import unittest


class ActivityStorageLazyReaderTests(unittest.TestCase):
    def test_activity_storage_defers_sql_manager_construction(self):
        source = (
            Path(__file__).parents[1]
            / "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_activity/activity_storage.py"
        ).read_text(encoding="utf-8")
        self.assertIn("_sql_message_instance = None", source)
        self.assertIn("def _sql_message(", source)
        self.assertNotIn("_sql_message = XiuxianDateManage()", source)
        self.assertIn("_sql_message().", source)


if __name__ == "__main__":
    unittest.main()
