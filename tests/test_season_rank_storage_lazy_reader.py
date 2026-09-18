from pathlib import Path
import unittest


class SeasonRankStorageLazyReaderTests(unittest.TestCase):
    def test_season_rank_defers_sql_manager_construction(self):
        source = (
            Path(__file__).parents[1]
            / "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_utils/season_rank_service.py"
        ).read_text(encoding="utf-8")
        self.assertIn("_sql_message_instance = None", source)
        self.assertIn("def _sql_message(", source)
        self.assertNotIn("sql_message = XiuxianDateManage()", source)
        self.assertIn("_sql_message().", source)


if __name__ == "__main__":
    unittest.main()
