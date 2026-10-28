from __future__ import annotations

import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from nonebot_plugin_xiuxian_2.compatibility.auction_bid_effects import (
    LegacyAuctionBidEffects,
)
from nonebot_plugin_xiuxian_2.features.auction.migrations import apply_auction_settlement
from nonebot_plugin_xiuxian_2.features.auction.query_application import AuctionQueryApplication
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork


class _BidLogRecorder:
    def __init__(self) -> None:
        self.calls: list[dict] = []

    def __call__(self, **kwargs) -> bool:
        self.calls.append(dict(kwargs))
        return True


class _BidStatistics:
    def record_bid(self, **kwargs) -> None:  # noqa: D401 - test double
        return None


class AuctionMyBidsFixture:
    def __init__(self, root: Path) -> None:
        self.database = root / "game.sqlite3"
        with DatabaseUnitOfWork(self.database) as uow:
            apply_auction_settlement(uow)
            uow.executemany(
                "INSERT INTO auction_current(id,item_id,name,start_price,current_price,"
                "seller_id,seller_name,bids,is_system) VALUES(?,?,?,?,?,?,?,?,0)",
                (
                    (
                        "a1",
                        1001,
                        "灵剑",
                        100,
                        300,
                        "seller",
                        "卖家",
                        json.dumps({"me": 300, "other": 200}),
                    ),
                    (
                        "a2",
                        1002,
                        "灵草",
                        100,
                        900,
                        "seller",
                        "卖家",
                        json.dumps({"other": 900, "me": 500}),
                    ),
                    (
                        "a3",
                        1003,
                        "灵果",
                        100,
                        700,
                        "seller",
                        "卖家",
                        json.dumps({"someone": 700}),
                    ),
                ),
            )


class AuctionMyBidsTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.fixture = AuctionMyBidsFixture(Path(self.temp_dir.name))

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def test_only_the_players_own_bids_are_returned(self) -> None:
        bids = AuctionQueryApplication(self.fixture.database).my_bids("me")

        self.assertEqual(["a1", "a2"], [bid["auction_id"] for bid in bids])
        self.assertNotIn("a3", [bid["auction_id"] for bid in bids])

    def test_leading_and_outbid_states_come_from_the_bid_snapshot(self) -> None:
        bids = {bid["auction_id"]: bid for bid in AuctionQueryApplication(self.fixture.database).my_bids("me")}

        self.assertTrue(bids["a1"]["leading"])
        self.assertEqual((300, 300), (bids["a1"]["my_price"], bids["a1"]["current_price"]))
        self.assertFalse(bids["a2"]["leading"])
        self.assertEqual((500, 900), (bids["a2"]["my_price"], bids["a2"]["current_price"]))
        self.assertEqual(2, bids["a2"]["bid_count"])

    def test_current_price_never_drops_below_the_top_bid(self) -> None:
        with DatabaseUnitOfWork(self.fixture.database) as uow:
            uow.execute(
                "UPDATE auction_current SET current_price=100 WHERE id='a1'"
            )

        bid = AuctionQueryApplication(self.fixture.database).my_bids("me")[0]

        self.assertEqual(300, bid["current_price"])

    def test_outbid_player_still_sees_their_bid_from_the_operation_ledger(self) -> None:
        # The bid repository drops the previous leader from auction_current.bids;
        # the ledger is the only place their bid survives.
        with DatabaseUnitOfWork(self.fixture.database) as uow:
            uow.execute(
                "CREATE TABLE auction_bid_operations (operation_id TEXT PRIMARY KEY,"
                "payload TEXT,auction_id TEXT,bidder_id TEXT,bid_price INTEGER,debit INTEGER,"
                "refunded_bidder TEXT,refunded_amount INTEGER)"
            )
            uow.execute(
                "INSERT INTO auction_bid_operations VALUES('b1','[]','a2','me',500,500,'',0)"
            )
            uow.execute("UPDATE auction_current SET bids=? WHERE id='a2'", ('{"other": 900}',))

        bids = AuctionQueryApplication(self.fixture.database).my_bids("me")
        a2 = [bid for bid in bids if bid["auction_id"] == "a2"][0]

        self.assertFalse(a2["leading"])
        self.assertEqual((500, 900), (a2["my_price"], a2["current_price"]))

    def test_player_without_bids_gets_an_empty_list(self) -> None:
        self.assertEqual([], AuctionQueryApplication(self.fixture.database).my_bids("nobody"))
        self.assertEqual([], AuctionQueryApplication(self.fixture.database).my_bids(""))

    def test_missing_database_and_table_return_empty_without_ddl(self) -> None:
        missing = Path(self.temp_dir.name) / "absent.sqlite3"
        self.assertEqual([], AuctionQueryApplication(missing).my_bids("me"))
        self.assertFalse(missing.exists())

        empty = Path(self.temp_dir.name) / "empty.sqlite3"
        sqlite3.connect(empty).close()
        self.assertEqual([], AuctionQueryApplication(empty).my_bids("me"))
        with sqlite3.connect(empty) as connection:
            self.assertEqual(
                [], connection.execute("SELECT name FROM sqlite_master").fetchall()
            )


class LegacyAuctionBidEffectsTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.player_database = Path(self.temp_dir.name) / "player.sqlite3"
        sqlite3.connect(self.player_database).close()
        self.log = _BidLogRecorder()
        self.effects = LegacyAuctionBidEffects(
            self.player_database,
            log_writer=self.log,
            statistics=_BidStatistics(),
        )

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def _bid(self, **overrides) -> None:
        kwargs = {
            "operation_id": "op-1",
            "auction_id": "a1",
            "bidder_id": "me",
            "item_name": "灵剑",
            "bid_price": 500,
            "replayed": False,
            "occurred_at": "2026-10-27T12:00:00+00:00",
        }
        kwargs.update(overrides)
        self.effects.on_bid(**kwargs)

    def test_outbid_player_gets_a_refund_notice(self) -> None:
        self._bid(refunded_bidder="rival", refunded_amount=400)

        self.assertEqual(["me", "rival"], [call["user_id"] for call in self.log.calls])
        outbid = self.log.calls[1]
        self.assertIn("[拍卖被超价]", outbid["message"])
        self.assertIn("400", outbid["message"])
        self.assertIn("已退回", outbid["message"])
        self.assertEqual("auction.outbid:op-1", outbid["event_id"])
        self.assertEqual("auction.bid:op-1", self.log.calls[0]["event_id"])

    def test_no_notice_when_nobody_was_outbid(self) -> None:
        self._bid()

        self.assertEqual(1, len(self.log.calls))
        self.assertEqual("auction.bid:op-1", self.log.calls[0]["event_id"])

    def test_zero_refund_amount_writes_only_the_bid_line(self) -> None:
        self._bid(refunded_bidder="rival", refunded_amount=0)

        self.assertEqual(1, len(self.log.calls))

    def test_event_ids_are_stable_for_replay_dedupe(self) -> None:
        self._bid(refunded_bidder="rival", refunded_amount=400)
        first = [call["event_id"] for call in self.log.calls]
        self.log.calls.clear()
        self._bid(refunded_bidder="rival", refunded_amount=400)
        second = [call["event_id"] for call in self.log.calls]

        self.assertEqual(first, second)
        self.assertEqual(first, ["auction.bid:op-1", "auction.outbid:op-1"])

    def test_reconcile_event_forwards_the_refund_fields(self) -> None:
        self.effects.reconcile_event(
            {
                "payload": {
                    "operation_id": "op-2",
                    "auction_id": "a1",
                    "bidder_id": "me",
                    "item_name": "灵剑",
                    "bid_price": 500,
                    "refunded_bidder": "rival",
                    "refunded_amount": 400,
                    "occurred_at": "2026-10-27T12:00:00+00:00",
                }
            }
        )

        self.assertEqual(["me", "rival"], [call["user_id"] for call in self.log.calls])
        self.assertEqual("auction.outbid:op-2", self.log.calls[1]["event_id"])


class AuctionMyBidsWiringTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        root = Path(__file__).resolve().parents[1]
        cls.command_source = (
            root / "nonebot_plugin_xiuxian_2" / "xiuxian" / "xiuxian_trade" / "__init__.py"
        ).read_text(encoding="utf-8")
        cls.application_source = (
            root / "nonebot_plugin_xiuxian_2" / "features" / "auction" / "application.py"
        ).read_text(encoding="utf-8")

    def test_my_auction_handler_renders_the_bid_section(self) -> None:
        start = self.command_source.index("async def my_auction_(")
        handler = self.command_source[start : self.command_source.index("@auction_info.handle", start)]

        self.assertIn("_auction_query_application().my_bids(", handler)
        self.assertIn("【我的竞价】", handler)
        self.assertIn("已被超价", handler)
        self.assertIn("拍卖竞拍", handler)

    def test_bid_outbox_payload_carries_the_refund(self) -> None:
        self.assertIn('"refunded_bidder"', self.application_source)
        self.assertIn('"refunded_amount"', self.application_source)


if __name__ == "__main__":
    unittest.main()
