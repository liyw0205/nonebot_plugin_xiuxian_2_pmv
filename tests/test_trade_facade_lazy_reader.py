from pathlib import Path
import unittest


class TradeFacadeLazyReaderTests(unittest.TestCase):
    def test_trade_facade_defers_legacy_manager_construction(self):
        source = (
            Path(__file__).parents[1]
            / "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_trade/__init__.py"
        ).read_text(encoding="utf-8")
        self.assertIn("_sql_message_instance = None", source)
        self.assertIn("_trade_manager_instance = None", source)
        self.assertIn("def _sql_message(", source)
        self.assertIn("def _trade_manager(", source)
        self.assertNotIn("sql_message = XiuxianDateManage()", source)
        self.assertNotIn("trade_manager = TradeDataManager()", source)

    def test_auction_dependencies_accept_resolvers(self):
        source = (
            Path(__file__).parents[1]
            / "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_trade/transaction_service.py"
        ).read_text(encoding="utf-8")
        self.assertIn("def _resolve_dependency(", source)
        self.assertIn("sql_message = _resolve_dependency(_sql_message)", source)
        self.assertIn("trade_manager = _resolve_dependency(_trade_manager)", source)


if __name__ == "__main__":
    unittest.main()
