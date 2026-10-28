from __future__ import annotations

import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from nonebot_plugin_xiuxian_2.features.auction.migrations import (
    apply_auction_bid_operations,
    apply_auction_player_queue,
    apply_auction_queue_operations,
    apply_auction_settlement,
)
from nonebot_plugin_xiuxian_2.features.auction.my_auction_view import (
    format_remaining,
    render_live_lots,
    render_settlements,
)
from nonebot_plugin_xiuxian_2.features.auction.query_application import AuctionQueryApplication
from nonebot_plugin_xiuxian_2.features.auction.queue_repository import AuctionQueueSqlRepository
from nonebot_plugin_xiuxian_2.features.auction.session_start_application import (
    AuctionSessionStartApplication,
)
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork

NOW = 1_600.0
SESSION_END = 4_600.0


class _SnapshotFixture:
    def __init__(self, root: Path) -> None:
        self.database = root / "game.sqlite3"
        with DatabaseUnitOfWork(self.database) as uow:
            apply_auction_settlement(uow)
            apply_auction_bid_operations(uow)
            uow.executemany(
                "INSERT INTO auction_current(id,item_id,name,start_price,current_price,"
                "seller_id,seller_name,bids,is_system) VALUES(?,?,?,?,?,?,?,?,?)",
                (
                    ("a1", 1001, "灵剑", 500, 500, "seller-1", "卖家甲", json.dumps({"u1": 600}), 0),
                    (
                        "a2",
                        1002,
                        "灵草",
                        100,
                        700,
                        "seller-1",
                        "卖家甲",
                        json.dumps({"u2": 700}),
                        0,
                    ),
                    ("a3", 1003, "护符", 200, 200, "seller-2", "卖家乙", "{}", 0),
                    ("a4", 1004, "系统丹", 50, 50, "0", "系统", "{}", 1),
                ),
            )
            uow.execute(
                "INSERT INTO auction_sessions(session_id,status,start_time,end_time,"
                "items_count,start_operation_id) VALUES('s1','active',1000,?,4,'op-start')",
                (SESSION_END,),
            )
            uow.executemany(
                "INSERT INTO auction_bid_operations(operation_id,payload,auction_id,"
                "bidder_id,bid_price,debit,refunded_bidder,refunded_amount) "
                "VALUES(?,?,?,?,?,?,?,?)",
                (
                    ("b1", "[]", "a1", "u1", 500, 500, "", 0),
                    ("b2", "[]", "a1", "u1", 600, 100, "u1", 500),
                    ("b3", "[]", "a2", "u2", 400, 400, "", 0),
                    ("b4", "[]", "a2", "u3", 550, 550, "u2", 400),
                    ("b5", "[]", "a2", "u2", 700, 150, "u3", 550),
                ),
            )
            uow.executemany(
                "INSERT INTO auction_history(auction_id,item_id,item_name,start_price,"
                "final_price,seller_id,seller_name,winner_id,winner_name,status,fee,"
                "seller_earnings,start_time,end_time) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    ("a1", 1001, "灵剑", 500, 800, "seller-1", "卖家甲", "buyer-1", "买家甲", "成交", 80, 720, 1000, 2000),
                    ("a9", 1009, "灵草", 100, None, "seller-1", "卖家甲", None, None, "流拍", 0, 0, 1000, 1900),
                    ("a8", 1008, "灵果", 100, None, "seller-1", "卖家甲", None, None, "待领取", 0, 0, 1000, 1800),
                    ("a7", 1007, "护符", 200, 500, "seller-2", "卖家乙", "buyer-1", "买家甲", "成交", 50, 450, 1000, 1700),
                    ("a6", 1006, "别人的", 1, 2, "seller-9", "卖家丙", "buyer-9", "买家丙", "成交", 0, 2, 1000, 2100),
                ),
            )

    def snapshot(self, user_id: str, **kwargs):
        return AuctionQueryApplication(self.database).my_auction_snapshot(
            user_id, now_epoch=NOW, **kwargs
        )


class AuctionMyLotsRepositoryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.fixture = _SnapshotFixture(Path(self.temp_dir.name))

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def test_live_lots_only_contain_the_players_non_system_lots(self) -> None:
        lots = self.fixture.snapshot("seller-1")["lots"]

        self.assertEqual(["a1", "a2"], [lot["auction_id"] for lot in lots])

    def test_live_lot_prices_and_bid_counts_come_from_the_snapshot_and_ledger(self) -> None:
        lots = {lot["auction_id"]: lot for lot in self.fixture.snapshot("seller-1")["lots"]}

        # a1 has current_price 500 but the standing bid is 600.
        self.assertEqual(600, lots["a1"]["current_price"])
        self.assertEqual((2, 1), (lots["a1"]["bid_count"], lots["a1"]["bidder_count"]))
        self.assertEqual(700, lots["a2"]["current_price"])
        self.assertEqual((3, 2), (lots["a2"]["bid_count"], lots["a2"]["bidder_count"]))

    def test_remaining_time_comes_from_the_active_session(self) -> None:
        lots = self.fixture.snapshot("seller-1")["lots"]

        self.assertEqual([3000, 3000], [lot["remaining_seconds"] for lot in lots])

    def test_missing_session_table_omits_remaining_without_ddl(self) -> None:
        with DatabaseUnitOfWork(self.fixture.database) as uow:
            uow.execute("DROP TABLE auction_sessions")

        snapshot = self.fixture.snapshot("seller-1")

        self.assertEqual([None, None], [lot["remaining_seconds"] for lot in snapshot["lots"]])
        with sqlite3.connect(self.fixture.database) as connection:
            names = {row[0] for row in connection.execute("SELECT name FROM sqlite_master")}
        self.assertNotIn("auction_sessions", names)

    def test_missing_bid_ledger_falls_back_to_the_bid_snapshot(self) -> None:
        with DatabaseUnitOfWork(self.fixture.database) as uow:
            uow.execute("DROP TABLE auction_bid_operations")

        lots = {lot["auction_id"]: lot for lot in self.fixture.snapshot("seller-1")["lots"]}

        self.assertEqual((1, 1), (lots["a1"]["bid_count"], lots["a1"]["bidder_count"]))
        self.assertEqual(1, lots["a2"]["bid_count"])

    def test_settlement_records_cover_both_roles_in_reverse_time_order(self) -> None:
        seller_records = self.fixture.snapshot("seller-1")["records"]
        buyer_records = self.fixture.snapshot("buyer-1")["records"]

        self.assertEqual([2000, 1900, 1800], [row["end_time"] for row in seller_records])
        self.assertEqual(["seller", "seller", "seller"], [row["role"] for row in seller_records])
        self.assertEqual(["winner", "winner"], [row["role"] for row in buyer_records])
        self.assertEqual(["a1", "a7"], [row["auction_id"] for row in buyer_records])
        self.assertEqual(450, buyer_records[1]["seller_earnings"])

    def test_records_are_limited_and_exclude_other_players(self) -> None:
        limited = self.fixture.snapshot("seller-1", limit=2)["records"]

        self.assertEqual(["a1", "a9"], [row["auction_id"] for row in limited])
        self.assertNotIn(
            "a6",
            [row["auction_id"] for row in self.fixture.snapshot("seller-1")["records"]],
        )

    def test_seller_record_carries_fee_earnings_and_buyer(self) -> None:
        sold = self.fixture.snapshot("seller-1")["records"][0]

        self.assertEqual((800, 80, 720), (sold["final_price"], sold["fee"], sold["seller_earnings"]))
        self.assertEqual("买家甲", sold["counterparty"])
        self.assertEqual("成交", sold["status"])

    def test_flow_and_pending_records_survive_without_a_final_price(self) -> None:
        records = {row["auction_id"]: row for row in self.fixture.snapshot("seller-1")["records"]}

        self.assertIsNone(records["a9"]["final_price"])
        self.assertEqual("流拍", records["a9"]["status"])
        self.assertIsNone(records["a8"]["final_price"])
        self.assertEqual("待领取", records["a8"]["status"])

    def test_missing_history_table_keeps_live_lots_without_records(self) -> None:
        with DatabaseUnitOfWork(self.fixture.database) as uow:
            uow.execute("DROP TABLE auction_history")

        snapshot = self.fixture.snapshot("seller-1")

        self.assertEqual(2, len(snapshot["lots"]))
        self.assertEqual([], snapshot["records"])

    def test_missing_database_and_empty_database_return_empty_without_creating_files(self) -> None:
        missing = Path(self.temp_dir.name) / "absent.sqlite3"
        self.assertEqual(
            {"lots": [], "records": []},
            AuctionQueryApplication(missing).my_auction_snapshot("seller-1"),
        )
        self.assertFalse(missing.exists())

        empty = Path(self.temp_dir.name) / "empty.sqlite3"
        sqlite3.connect(empty).close()
        self.assertEqual(
            {"lots": [], "records": []},
            AuctionQueryApplication(empty).my_auction_snapshot("seller-1"),
        )
        with sqlite3.connect(empty) as connection:
            self.assertEqual([], connection.execute("SELECT name FROM sqlite_master").fetchall())

    def test_blank_user_returns_empty(self) -> None:
        self.assertEqual(
            {"lots": [], "records": []},
            self.fixture.snapshot(""),
        )


class AuctionSessionHandoffTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.root = Path(self.temp_dir.name)
        self.game_db = self.root / "game.sqlite3"
        self.trade_db = self.root / "trade.sqlite3"
        with DatabaseUnitOfWork(self.game_db) as uow:
            apply_auction_settlement(uow)
            apply_auction_queue_operations(uow)
        with DatabaseUnitOfWork(self.trade_db) as uow:
            apply_auction_player_queue(uow)
        with DatabaseUnitOfWork(self.trade_db) as uow:
            uow.execute(
                "INSERT INTO auction_player_upload(user_id,item_id,item_name,"
                "start_price,user_name) VALUES('seller-1',1001,'灵剑',500,'卖家甲')"
            )

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def test_live_lot_survives_the_queue_to_session_handoff(self) -> None:
        queue = AuctionQueueSqlRepository(self.game_db, self.trade_db, 1000)
        self.assertEqual(1, len(queue.get_player_items("seller-1")))

        result = AuctionSessionStartApplication(self.game_db, self.trade_db).start(
            "op-start", system_items_config={}, duration_hours=1, system_item_count=0
        )
        self.assertEqual("started", result.status)
        self.assertEqual([], queue.get_player_items("seller-1"))

        snapshot = AuctionQueryApplication(self.game_db).my_auction_snapshot(
            "seller-1", now_epoch=result.start_time + 60
        )

        self.assertEqual(1, len(snapshot["lots"]))
        self.assertEqual("灵剑", snapshot["lots"][0]["item_name"])
        self.assertEqual(3540, snapshot["lots"][0]["remaining_seconds"])
        self.assertTrue(snapshot["lots"])


class MyAuctionRenderingTests(unittest.TestCase):
    def test_live_lot_line_contains_price_bids_and_remaining(self) -> None:
        lines = render_live_lots(
            (
                {
                    "auction_id": "a1",
                    "item_name": "灵剑",
                    "start_price": 500,
                    "current_price": 800,
                    "bid_count": 3,
                    "bidder_count": 2,
                    "remaining_seconds": 3900,
                },
            )
        )

        self.assertEqual(
            "拍卖ID:a1 灵剑 起拍:500 当前价:800 出价:3次/2人 剩余:1小时5分",
            lines[0],
        )
        self.assertEqual("拍卖进行中，拍品暂不可下架", lines[1])

    def test_live_lot_without_remaining_omits_the_field(self) -> None:
        lines = render_live_lots(
            (
                {
                    "auction_id": "a1",
                    "item_name": "灵剑",
                    "start_price": 500,
                    "current_price": 500,
                    "bid_count": 0,
                    "bidder_count": 0,
                    "remaining_seconds": None,
                },
            )
        )

        self.assertNotIn("剩余", lines[0])
        self.assertEqual(2, len(lines))

    def test_no_live_lots_renders_no_section(self) -> None:
        self.assertEqual((), render_live_lots(()))

    def test_seller_sale_line_contains_fee_earnings_and_buyer(self) -> None:
        lines = render_settlements(
            (
                {
                    "role": "seller",
                    "item_name": "灵剑",
                    "status": "成交",
                    "final_price": 800,
                    "fee": 80,
                    "seller_earnings": 720,
                    "counterparty": "买家甲",
                    "end_time": 2000,
                },
            )
        )

        self.assertIn("卖出｜灵剑｜成交｜成交价:800｜手续费:80｜实收:720｜买家:买家甲", lines[0])

    def test_unsold_and_pending_lines_do_not_claim_a_sale(self) -> None:
        lines = render_settlements(
            (
                {
                    "role": "seller",
                    "item_name": "灵草",
                    "status": "流拍",
                    "final_price": None,
                    "fee": 0,
                    "seller_earnings": 0,
                    "counterparty": "",
                    "end_time": 1900,
                },
                {
                    "role": "seller",
                    "item_name": "灵果",
                    "status": "待领取",
                    "final_price": None,
                    "fee": 0,
                    "seller_earnings": 0,
                    "counterparty": "",
                    "end_time": 1800,
                },
            )
        )

        self.assertIn("流拍｜未售出，物品已退回背包", lines[0])
        self.assertIn("待领取｜物品在拍卖待领取", lines[1])
        self.assertNotIn("实收", lines[0])

    def test_winner_line_names_the_seller_and_flags_pending_delivery(self) -> None:
        sold, pending = render_settlements(
            (
                {
                    "role": "winner",
                    "item_name": "护符",
                    "status": "成交",
                    "final_price": 500,
                    "fee": 50,
                    "seller_earnings": 450,
                    "counterparty": "卖家乙",
                    "end_time": 1700,
                },
                {
                    "role": "winner",
                    "item_name": "灵剑",
                    "status": "待领取",
                    "final_price": 800,
                    "fee": 80,
                    "seller_earnings": 720,
                    "counterparty": "卖家甲",
                    "end_time": 1600,
                },
            )
        )

        self.assertIn("拍得｜护符｜成交｜成交价:500｜卖家:卖家乙", sold)
        self.assertIn("待领取｜成交价:800｜物品在拍卖待领取", pending)

    def test_remaining_duration_formats_units(self) -> None:
        self.assertEqual("30秒", format_remaining(30))
        self.assertEqual("1分钟", format_remaining(90))
        self.assertEqual("1小时5分", format_remaining(3900))
        self.assertEqual("1天1小时", format_remaining(90000))
        self.assertEqual("0秒", format_remaining(-1))


class MyAuctionWiringTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        root = Path(__file__).resolve().parents[1]
        cls.command_source = (
            root / "nonebot_plugin_xiuxian_2" / "xiuxian" / "xiuxian_trade" / "__init__.py"
        ).read_text(encoding="utf-8")
        cls.help_source = (
            root / "nonebot_plugin_xiuxian_2" / "xiuxian" / "xiuxian_trade" / "trade_help.py"
        ).read_text(encoding="utf-8")

    def test_handler_merges_the_personal_snapshot_into_my_auction(self) -> None:
        start = self.command_source.index("async def my_auction_(")
        handler = self.command_source[start : self.command_source.index("@auction_info.handle", start)]

        self.assertIn("my_auction_snapshot(", handler)
        self.assertIn("render_live_lots(", handler)
        self.assertIn("render_settlements(", handler)
        self.assertIn("【本场拍品】", handler)
        self.assertIn("【最近拍卖记录】", handler)
        self.assertIn("拍卖进行中", self.command_source)

    def test_empty_state_covers_all_five_sources(self) -> None:
        start = self.command_source.index("async def my_auction_(")
        handler = self.command_source[start : self.command_source.index("@auction_info.handle", start)]

        self.assertIn(
            "not player_auction_items and not pending_items and not my_bids "
            "and not my_lots and not my_records",
            handler,
        )
        self.assertIn("本场在拍的拍品", handler)

    def test_handler_still_uses_the_shared_delivery_seam(self) -> None:
        start = self.command_source.index("async def my_auction_(")
        handler = self.command_source[start : self.command_source.index("@auction_info.handle", start)]

        self.assertIn("await handle_send(", handler)
        self.assertNotIn("bot.send(", handler)

    def test_help_documents_live_lots_and_records(self) -> None:
        self.assertIn("本场拍品", self.help_source)
        self.assertIn("成交记录", self.help_source)


if __name__ == "__main__":
    unittest.main()
