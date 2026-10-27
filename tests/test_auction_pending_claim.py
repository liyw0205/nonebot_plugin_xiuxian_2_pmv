from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path

from nonebot_plugin_xiuxian_2.features.auction.migrations import (
    apply_auction_pending_delivery,
    apply_auction_settlement,
)
from nonebot_plugin_xiuxian_2.features.auction.pending import AuctionPendingApplication
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork


class PendingClaimFixture:
    def __init__(self, root: Path) -> None:
        self.root = root
        self.database = root / "game.sqlite3"
        with DatabaseUnitOfWork(self.database) as uow:
            uow.execute(
                "CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,user_name TEXT,stone INTEGER)"
            )
            uow.execute(
                "CREATE TABLE back(user_id TEXT,goods_id INTEGER,goods_name TEXT,goods_type TEXT,"
                "goods_num INTEGER,create_time TEXT,update_time TEXT,"
                "bind_num INTEGER DEFAULT 0,UNIQUE(user_id,goods_id))"
            )
            apply_auction_settlement(uow)
            apply_auction_pending_delivery(uow)

    def park(
        self,
        *,
        auction_id: str,
        owner_id: str,
        role: str = "winner",
        item_id: int = 1001,
        item_name: str = "玩家法器",
        reason: str = "inventory_full",
    ) -> None:
        with DatabaseUnitOfWork(self.database) as uow:
            uow.execute(
                "INSERT INTO auction_pending_items(auction_id,item_id,item_name,item_type,"
                "owner_id,owner_name,role,reason) VALUES(?,?,?,?,?,?,?,?)",
                (auction_id, item_id, item_name, "装备", owner_id, f"名-{owner_id}", role, reason),
            )

    def give(self, user_id: str, item_id: int, quantity: int) -> None:
        with DatabaseUnitOfWork(self.database) as uow:
            uow.execute(
                "INSERT INTO back(user_id,goods_id,goods_name,goods_type,goods_num,"
                "create_time,update_time,bind_num) VALUES(?,?,?,?,?,?,?,0) "
                "ON CONFLICT(user_id,goods_id) DO UPDATE SET goods_num=?",
                (user_id, item_id, "玩家法器", "装备", quantity, "t", "t", quantity),
            )

    def copies(self, user_id: str, item_id: int) -> int:
        with DatabaseUnitOfWork(self.database) as uow:
            row = uow.query_one(
                "SELECT goods_num,bind_num FROM back WHERE user_id=? AND goods_id=?",
                (user_id, item_id),
            )
        return int(row["goods_num"]) if row else 0

    def binds(self, user_id: str, item_id: int) -> int:
        with DatabaseUnitOfWork(self.database) as uow:
            row = uow.query_one(
                "SELECT bind_num FROM back WHERE user_id=? AND goods_id=?",
                (user_id, item_id),
            )
        return int(row["bind_num"]) if row else 0

    def unclaimed(self, user_id: str) -> int:
        with DatabaseUnitOfWork(self.database) as uow:
            row = uow.query_one(
                "SELECT COUNT(*) AS n FROM auction_pending_items "
                "WHERE owner_id=? AND claimed_at IS NULL",
                (user_id,),
            )
        return int(row["n"])

    def claim_receipts(self) -> int:
        with DatabaseUnitOfWork(self.database) as uow:
            return int(
                uow.query_one(
                    "SELECT COUNT(*) AS n FROM auction_pending_claim_operations"
                )["n"]
            )


class AuctionPendingClaimTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.fixture = PendingClaimFixture(Path(self.temp_dir.name))
        self.application = AuctionPendingApplication(
            self.fixture.database, max_goods_num=10
        )

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def test_list_only_shows_the_players_unclaimed_items(self) -> None:
        self.fixture.park(auction_id="a1", owner_id="buyer", role="winner")
        self.fixture.park(auction_id="a2", owner_id="buyer", role="seller", item_id=2002, item_name="灵草")
        self.fixture.park(auction_id="a3", owner_id="other", role="winner")

        rows = self.application.list_for("buyer")

        self.assertEqual(["a1", "a2"], [row["auction_id"] for row in rows])
        self.assertEqual(["中标", "流拍"], [row["role_label"] for row in rows])
        self.assertEqual([], self.application.list_for("nobody"))

    def test_claim_moves_a_single_parked_item_into_the_backpack(self) -> None:
        self.fixture.park(auction_id="a1", owner_id="buyer")

        result = self.application.claim(operation_id="claim-1", user_id="buyer")

        self.assertEqual("claimed", result.status)
        self.assertEqual(["a1"], [row["auction_id"] for row in result.claimed])
        self.assertEqual(0, result.remaining)
        self.assertEqual(1, self.fixture.copies("buyer", 1001))
        self.assertEqual(1, self.fixture.binds("buyer", 1001))
        self.assertEqual(0, self.fixture.unclaimed("buyer"))
        self.assertEqual(1, self.fixture.claim_receipts())

    def test_claim_adds_to_an_existing_stack(self) -> None:
        self.fixture.park(auction_id="a1", owner_id="buyer")
        self.fixture.give("buyer", 1001, 3)

        self.application.claim(operation_id="claim-stack", user_id="buyer")

        self.assertEqual(4, self.fixture.copies("buyer", 1001))

    def test_full_stack_keeps_the_item_pending(self) -> None:
        self.application = AuctionPendingApplication(self.fixture.database, max_goods_num=1)
        self.fixture.park(auction_id="a1", owner_id="buyer")
        self.fixture.give("buyer", 1001, 1)

        result = self.application.claim(operation_id="claim-full", user_id="buyer")

        self.assertEqual("blocked", result.status)
        self.assertEqual(["a1"], [row["auction_id"] for row in result.blocked])
        self.assertEqual(1, result.remaining)
        self.assertEqual(1, self.fixture.copies("buyer", 1001))
        self.assertEqual(1, self.fixture.unclaimed("buyer"))

    def test_claim_takes_every_claimable_item_and_reports_remainder(self) -> None:
        self.fixture.park(auction_id="a1", owner_id="buyer", item_id=1001)
        self.fixture.park(auction_id="a2", owner_id="buyer", item_id=2002, item_name="灵草")
        self.fixture.give("buyer", 2002, 10)  # that stack is already at the cap

        result = self.application.claim(operation_id="claim-many", user_id="buyer")

        self.assertEqual("claimed", result.status)
        self.assertEqual(["a1"], [row["auction_id"] for row in result.claimed])
        self.assertEqual(["a2"], [row["auction_id"] for row in result.blocked])
        self.assertEqual(1, result.remaining)

    def test_claim_with_auction_id_only_takes_that_item(self) -> None:
        self.fixture.park(auction_id="a1", owner_id="buyer", item_id=1001)
        self.fixture.park(auction_id="a2", owner_id="buyer", item_id=2002, item_name="灵草")

        result = self.application.claim(
            operation_id="claim-one", user_id="buyer", auction_id="a2"
        )

        self.assertEqual(["a2"], [row["auction_id"] for row in result.claimed])
        self.assertEqual(1, result.remaining)
        self.assertEqual(0, self.fixture.copies("buyer", 1001))
        self.assertEqual(1, self.fixture.copies("buyer", 2002))

    def test_claim_cannot_touch_another_players_item(self) -> None:
        self.fixture.park(auction_id="a1", owner_id="other")

        result = self.application.claim(
            operation_id="claim-other", user_id="buyer", auction_id="a1"
        )

        self.assertEqual("empty", result.status)
        self.assertEqual(0, self.fixture.copies("buyer", 1001))
        self.assertEqual(1, self.fixture.unclaimed("other"))

    def test_replay_does_not_hand_out_a_second_copy(self) -> None:
        self.fixture.park(auction_id="a1", owner_id="buyer")

        first = self.application.claim(operation_id="claim-replay", user_id="buyer")
        replay = self.application.claim(operation_id="claim-replay", user_id="buyer")

        self.assertEqual("claimed", first.status)
        self.assertEqual("duplicate", replay.status)
        self.assertEqual(["a1"], [row["auction_id"] for row in replay.claimed])
        self.assertEqual(1, self.fixture.copies("buyer", 1001))
        self.assertEqual(1, self.fixture.claim_receipts())

    def test_empty_mailbox_reports_empty_without_a_receipt(self) -> None:
        result = self.application.claim(operation_id="claim-empty", user_id="buyer")

        self.assertEqual("empty", result.status)
        self.assertEqual((), result.claimed)
        self.assertEqual(0, self.fixture.claim_receipts())

    def test_operation_conflict_when_the_same_id_changes_scope(self) -> None:
        self.fixture.park(auction_id="a1", owner_id="buyer")

        self.application.claim(operation_id="claim-scope", user_id="buyer")
        conflict = self.application.claim(
            operation_id="claim-scope", user_id="buyer", auction_id="a1"
        )

        self.assertEqual("operation_conflict", conflict.status)
        self.assertEqual(1, self.fixture.copies("buyer", 1001))

    def test_missing_tables_fail_closed_without_creating_schema(self) -> None:
        missing = Path(self.temp_dir.name) / "absent.sqlite3"
        application = AuctionPendingApplication(missing)

        self.assertEqual([], application.list_for("buyer"))
        result = application.claim(operation_id="claim-missing", user_id="buyer")
        self.assertEqual("schema_missing", result.status)
        self.assertFalse(missing.exists())

        empty = Path(self.temp_dir.name) / "empty.sqlite3"
        sqlite3.connect(empty).close()
        application = AuctionPendingApplication(empty)
        self.assertEqual([], application.list_for("buyer"))
        self.assertEqual(
            "schema_missing", application.claim(operation_id="x", user_id="buyer").status
        )
        with sqlite3.connect(empty) as connection:
            self.assertEqual(
                [], connection.execute("SELECT name FROM sqlite_master").fetchall()
            )

    def test_default_capacity_is_not_one(self) -> None:
        application = AuctionPendingApplication(self.fixture.database)
        self.assertGreater(application.max_goods_num, 1)


class AuctionPendingWiringTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        root = Path(__file__).resolve().parents[1]
        cls.command_source = (
            root / "nonebot_plugin_xiuxian_2" / "xiuxian" / "xiuxian_trade" / "__init__.py"
        ).read_text(encoding="utf-8")
        cls.help_source = (
            root / "nonebot_plugin_xiuxian_2" / "xiuxian" / "xiuxian_trade" / "trade_help.py"
        ).read_text(encoding="utf-8")

    def test_my_auction_handler_claims_and_lists_pending_items(self) -> None:
        start = self.command_source.index("async def my_auction_(")
        handler = self.command_source[start : self.command_source.index("@auction_info.handle", start)]

        self.assertIn("_auction_pending_application().claim(", handler)
        self.assertIn("_auction_pending_application().list_for(", handler)
        self.assertIn("我的拍卖 领取", handler)

    def test_help_documents_the_claim_action(self) -> None:
        self.assertIn("我的拍卖 领取 [拍卖ID]", self.help_source)

    def test_pending_application_uses_the_configured_cap(self) -> None:
        start = self.command_source.index("def _auction_pending_application()")
        factory = self.command_source[start : start + 500]

        self.assertIn("AuctionPendingApplication(", factory)
        self.assertIn("max_goods_num=XiuConfig().max_goods_num", factory)


if __name__ == "__main__":
    unittest.main()
