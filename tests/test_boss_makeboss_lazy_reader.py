from pathlib import Path
import unittest


class MakeBossLazyReaderTests(unittest.TestCase):
    def test_makeboss_defers_sql_manager_construction(self):
        source = (
            Path(__file__).parents[1]
            / "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_boss/makeboss.py"
        ).read_text(encoding="utf-8")
        self.assertIn("_sql_message_instance = None", source)
        self.assertIn("def _sql_message(", source)
        self.assertIn("_sql_message().get_realm_top1_user(", source)
        self.assertNotIn("sql_message = XiuxianDateManage()", source)


if __name__ == "__main__":
    unittest.main()
