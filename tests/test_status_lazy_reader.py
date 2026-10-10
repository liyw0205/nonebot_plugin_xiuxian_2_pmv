from pathlib import Path
import unittest


class StatusLazyReaderTests(unittest.TestCase):
    def test_status_facade_defers_sql_manager_construction(self):
        source = (
            Path(__file__).parents[1]
            / "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_status/__init__.py"
        ).read_text(encoding="utf-8")
        self.assertIn("overview = status_application.bot_overview()", source)
        repository = (
            Path(__file__).parents[1]
            / "nonebot_plugin_xiuxian_2/features/status/bot_overview_repository.py"
        ).read_text(encoding="utf-8")
        self.assertIn("DatabaseUnitOfWork(self.game_database, read_only=True)", repository)
        self.assertIn("DatabaseUnitOfWork(self.trade_database, read_only=True)", repository)
        self.assertNotIn("XiuxianDateManage", source)
        self.assertNotIn("XiuxianDateManage", repository)
        self.assertNotIn("_sql_message().", source)
        self.assertNotIn("sql_message = XiuxianDateManage()", source)


if __name__ == "__main__":
    unittest.main()
