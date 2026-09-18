from pathlib import Path
import unittest


class PartnerFacadeLazyReaderTests(unittest.TestCase):
    def test_partner_facade_defers_sql_manager_construction(self):
        source = (
            Path(__file__).parents[1]
            / "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_buff/partner.py"
        ).read_text(encoding="utf-8")
        self.assertIn("_sql_message_instance = None", source)
        self.assertIn("def _sql_message(", source)
        self.assertIn("_sql_message().get_user_info_with_id(", source)
        self.assertIn("_sql_message().get_user_real_info(", source)
        self.assertNotIn("sql_message = XiuxianDateManage()", source)


if __name__ == "__main__":
    unittest.main()
