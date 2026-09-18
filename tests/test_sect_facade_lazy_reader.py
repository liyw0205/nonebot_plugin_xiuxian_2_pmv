from pathlib import Path
import unittest


class SectFacadeLazyReaderTests(unittest.TestCase):
    def test_sect_facade_defers_sql_manager_construction(self):
        source = (
            Path(__file__).parents[1]
            / "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_sect/__init__.py"
        ).read_text(encoding="utf-8")
        self.assertIn("_sql_message_instance = None", source)
        self.assertIn("def _sql_message(", source)
        self.assertIn("_sql_message().get_sect_info(", source)
        self.assertNotIn("sql_message = XiuxianDateManage()", source)

    def test_sect_member_binding_accepts_a_resolver(self):
        source = (
            Path(__file__).parents[1]
            / "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_sect/sect_member_utils.py"
        ).read_text(encoding="utf-8")
        self.assertIn("if callable(sql_message):", source)
        self.assertIn("return sql_message()", source)


if __name__ == "__main__":
    unittest.main()
