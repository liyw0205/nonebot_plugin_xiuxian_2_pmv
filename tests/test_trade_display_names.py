"""Trade display batches names without reading assets or altering settlement."""
from __future__ import annotations

import asyncio
from contextlib import ExitStack
import sqlite3
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, Mock, patch

from nonebot.exception import FinishedException
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils.repositories import UserRepository


class TradeDisplayNamesTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        import nonebot
        try:
            nonebot.get_driver()
        except ValueError:
            nonebot.init()
        from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_trade
        cls.trade = xiuxian_trade

    def test_guishi_reads_one_name_projection_for_repeated_participants_and_keeps_events(self):
        connection = sqlite3.connect(":memory:")
        self.addCleanup(connection.close)
        connection.row_factory = sqlite3.Row
        connection.execute("CREATE TABLE user_xiuxian(user_id,user_name,exp)")
        connection.executemany("INSERT INTO user_xiuxian VALUES(?,?,?)", [
            ("buyer", "First", "invalid asset"), ("buyer", "Later", 0), ("seller", "Seller", 0)])
        queries = []
        def read(sql, params, **kwargs):
            queries.append(sql)
            return [dict(row) for row in connection.execute(sql.replace("%s", "?"), params)]
        users = UserRepository(read, None, None)
        manager = SimpleNamespace(get_user_names_by_ids=users.get_names_by_ids)
        fills = [SimpleNamespace(buyer_id="buyer", seller_id=seller, item_name="Item",
                                 quantity=2, amount=10**80 + 1) for seller in ("seller", "missing", "seller")]
        with patch.object(self.trade, "_sql_message", return_value=manager), \
                patch.object(self.trade, "_record_guishi_match_event") as record:
            text = self.trade._guishi_match_lines(iter(fills))
            self.assertEqual(1, len(queries))
            self.assertIn("SELECT user_id,user_name", queries[0])
            self.assertIn("First 从 Seller", text)
            self.assertIn("First 从 missing", text)
            self.assertEqual(3, record.call_count)
            self.assertEqual((fills[0], "First", "Seller"), record.call_args_list[0].args)
            connection.execute("UPDATE user_xiuxian SET user_name='Changed' WHERE user_id='buyer'")
            self.assertIn("Changed 从 Seller", self.trade._guishi_match_lines(fills))
            self.assertEqual(2, len(queries))
            self.assertEqual("", self.trade._guishi_match_lines([]))
            self.assertEqual(2, len(queries))

    def test_name_failure_falls_back_to_ids_but_flow_exception_propagates(self):
        manager = SimpleNamespace(get_user_names_by_ids=Mock(side_effect=RuntimeError("read failure")))
        fill = SimpleNamespace(buyer_id="buyer", seller_id="seller", item_name="Item", quantity=1, amount=2)
        with patch.object(self.trade, "_sql_message", return_value=manager), \
                patch.object(self.trade, "_record_guishi_match_event") as record:
            self.assertIn("buyer 从 seller", self.trade._guishi_match_lines([fill]))
            record.assert_called_once_with(fill, "buyer", "seller")
            manager.get_user_names_by_ids.side_effect = FinishedException
            with self.assertRaises(FinishedException):
                self.trade._guishi_match_lines([fill])

    def test_auction_detail_batches_only_five_visible_bidders_for_both_renderers(self):
        bids = {f"u{i}": 10**80 + i for i in range(7)}
        item = {"id": "a001", "name": "Item", "current_price": 10**80 + 7,
                "start_price": 1, "bids": bids, "bid_times": {key: i for i, key in enumerate(bids)}}
        for markdown in (False, True):
            with self.subTest(markdown=markdown), ExitStack() as stack:
                names = Mock(return_value={"u6": "Latest"})
                sender = AsyncMock()
                for key, value in {
                    "assign_bot": AsyncMock(return_value=("bot", None)),
                    "_auction_query_application": Mock(return_value=SimpleNamespace(get_current_auction=lambda aid: item)),
                    "_sql_message": Mock(return_value=SimpleNamespace(get_user_names_by_ids=names)),
                    "XiuConfig": Mock(return_value=SimpleNamespace(markdown_status=markdown)),
                    "handle_send": sender, "number_to": str,
                }.items():
                    stack.enter_context(patch.object(self.trade, key, value))
                args = SimpleNamespace(extract_plain_text=lambda: "a001")
                with self.assertRaises(FinishedException):
                    asyncio.run(self.trade.auction_view_("bot", "event", args))
                names.assert_called_once_with(["u6", "u5", "u4", "u3", "u2"])
                text = sender.call_args.args[2]
                self.assertIn("Latest", text)
                self.assertIn("u5", text)
                self.assertNotIn("u1", text)
                self.assertIn(str(10**80 + 6), text)

    def test_auction_finish_queries_unique_visible_winners_after_one_settlement(self):
        results = [{"winner_id": "winner", "final_price": 10**80 + i,
                    "item_name": f"Item{i}", "fee": 1} for i in range(7)]
        settle = AsyncMock(return_value=results)
        names, sender = Mock(return_value={"winner": "Winner"}), AsyncMock()
        with ExitStack() as stack:
            for key, value in {
                "assign_bot": AsyncMock(return_value=("bot", None)), "SUPERUSER": AsyncMock(return_value=True),
                "_auction_session_service": Mock(return_value=SimpleNamespace(get_active_session=lambda: {})),
                "_auction_query_application": Mock(return_value=SimpleNamespace(count_current_auctions=lambda: 7)),
                "_end_auction_with_application": settle, "_auction_session_operation_id": Mock(return_value="operation"),
                "_sql_message": Mock(return_value=SimpleNamespace(get_user_names_by_ids=names)),
                "handle_send": sender, "number_to": str,
            }.items():
                stack.enter_context(patch.object(self.trade, key, value))
            with self.assertRaises(FinishedException):
                asyncio.run(self.trade.auction_end_("bot", "event"))
        settle.assert_awaited_once_with("operation")
        names.assert_called_once_with(["winner"])
        self.assertEqual(5, sender.call_args.args[2].count("买家: Winner"))
        self.assertIn("共 7 件", sender.call_args.args[2])

    def test_scheduled_matching_batches_names_without_deferring_each_match_event(self):
        buy = {"id": "q", "user_id": "buyer", "item_name": "Item", "price": 100,
               "quantity": 3, "filled_quantity": 0}
        sells = [{"id": str(i), "user_id": "seller", "price": 10 + i} for i in range(3)]
        fills = [SimpleNamespace(status="applied", matched=True, buyer_id="buyer", seller_id="seller",
                                 quantity=1, item_name="Item", amount=10 + i,
                                 baitan_completed=True, qiugou_completed=i == 2) for i in range(3)]
        names, sequence = Mock(return_value={"buyer": "Buyer", "seller": "Seller"}), []
        def settle(**kwargs):
            sequence.append("settle")
            return fills[(len(sequence) - 1) // 2]
        with (
            patch.object(self.trade.trade_application, "guishi_get_orders", side_effect=[[buy], sells]),
            patch.object(self.trade.trade_application, "guishi_match", side_effect=settle) as match,
            patch.object(self.trade, "_sql_message", return_value=SimpleNamespace(get_user_names_by_ids=names)),
            patch.object(self.trade, "_record_guishi_match_event", side_effect=lambda *args: sequence.append("event")) as record,
        ):
            text = asyncio.run(self.trade.process_guishi_transactions("buyer"))
        names.assert_called_once_with(["buyer", "seller"])
        self.assertEqual(3, match.call_count)
        self.assertEqual(3, record.call_count)
        self.assertEqual(["settle", "event"] * 3, sequence)
        self.assertEqual(3, text.count("Buyer 从 Seller"))
