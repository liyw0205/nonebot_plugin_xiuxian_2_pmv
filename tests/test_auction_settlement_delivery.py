from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from nonebot_plugin_xiuxian_2.features.auction.migrations import (
    apply_auction_pending_delivery,
    apply_auction_settlement,
)
from nonebot_plugin_xiuxian_2.features.auction.settlement_repository import (
    DEFAULT_MAX_GOODS_NUM,
    AuctionSettlementSqlRepository,
    configured_max_goods_num,
)
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork


class AuctionDeliveryFixture:
    """Shared auction-settlement database for the delivery phases."""

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
            uow.executemany(
                "INSERT INTO user_xiuxian(user_id,user_name,stone) VALUES(?,?,?)",
                (("seller", "卖家", 0), ("winner", "买家", 1000), ("loser", "落败者", 500)),
            )
            apply_auction_settlement(uow)
            apply_auction_pending_delivery(uow)
            uow.execute(
                "INSERT INTO auction_sessions(session_id,status,start_time,end_time,items_count,"
                "start_operation_id) VALUES('session','active',100.0,200.0,1,'start')"
            )

    def add_auction(
        self,
        *,
        auction_id: str = "auction-1",
        bids: dict | None = None,
        item_id: int = 1001,
        item_name: str = "玩家法器",
        start_price: int = 600,
        current_price: int = 900,
    ) -> None:
        with DatabaseUnitOfWork(self.database) as uow:
            uow.execute(
                "INSERT INTO auction_current(id,item_id,name,start_price,current_price,"
                "seller_id,seller_name,bids,is_system,last_bid_time) "
                "VALUES(?,?,?,?,?,?,?,?,?,?)",
                (
                    auction_id,
                    item_id,
                    item_name,
                    start_price,
                    current_price,
                    "seller",
                    "卖家",
                    json.dumps(bids or {}),
                    0,
                    100.0,
                ),
            )

    def pending(self) -> list[dict]:
        with DatabaseUnitOfWork(self.database) as uow:
            return [
                dict(row)
                for row in uow.query_all(
                    "SELECT * FROM auction_pending_items ORDER BY id"
                )
            ]

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
                "SELECT goods_num FROM back WHERE user_id=? AND goods_id=?",
                (user_id, item_id),
            )
        return int(row["goods_num"]) if row else 0

    def stone(self, user_id: str) -> int:
        with DatabaseUnitOfWork(self.database) as uow:
            row = uow.query_one(
                "SELECT stone FROM user_xiuxian WHERE user_id=?", (user_id,)
            )
        return int(row["stone"]) if row else 0

    def status(self) -> str:
        with DatabaseUnitOfWork(self.database) as uow:
            row = uow.query_one("SELECT status FROM auction_sessions WHERE session_id='session'")
        return str(row["status"]) if row else ""

    def history(self) -> list[dict]:
        with DatabaseUnitOfWork(self.database) as uow:
            return [
                dict(row)
                for row in uow.query_all("SELECT * FROM auction_history ORDER BY id")
            ]

    def auction_rows(self) -> int:
        with DatabaseUnitOfWork(self.database) as uow:
            return int(uow.query_one("SELECT COUNT(*) AS n FROM auction_current")["n"])


class AuctionSettlementCapacityTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.fixture = AuctionDeliveryFixture(Path(self.temp_dir.name))
        self.fixture.add_auction(bids={"winner": 900, "loser": 700})

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def test_default_capacity_follows_the_configured_backpack_limit(self) -> None:
        configured = configured_max_goods_num()
        self.assertGreater(configured, 1)
        self.assertEqual(
            configured, AuctionSettlementSqlRepository(self.fixture.database).max_goods_num
        )
        self.assertGreater(DEFAULT_MAX_GOODS_NUM, 1)

    def test_explicit_capacity_still_wins(self) -> None:
        repository = AuctionSettlementSqlRepository(
            self.fixture.database, max_goods_num=7
        )
        self.assertEqual(7, repository.max_goods_num)

    def test_unsold_item_returns_to_a_seller_who_already_owns_copies(self) -> None:
        self.fixture.add_auction(auction_id="unsold", bids={})
        self.fixture.give("seller", 1001, 2)

        result = AuctionSettlementSqlRepository(self.fixture.database).settle_active(
            "settle-unsold", end_time=300.0, fee_rate=0.1, item_types={1001: "装备"}
        )

        self.assertEqual("settled", result.status)
        self.assertEqual(3, self.fixture.copies("seller", 1001))
        statuses = [record["status"] for record in result.results]
        self.assertIn("流拍", statuses)
        self.assertEqual(0, self.fixture.auction_rows())
        self.assertEqual("settled", self.fixture.status())

    def test_sold_item_reaches_a_winner_who_already_owns_copies(self) -> None:
        self.fixture.give("winner", 1001, 2)

        result = AuctionSettlementSqlRepository(self.fixture.database).settle_active(
            "settle-sold", end_time=300.0, fee_rate=0.1, item_types={1001: "装备"}
        )

        self.assertEqual("settled", result.status)
        self.assertIn("成交", [record["status"] for record in result.results])
        self.assertEqual(3, self.fixture.copies("winner", 1001))
        self.assertEqual(810, self.fixture.stone("seller"))
        self.assertEqual(1000, self.fixture.stone("winner"))
        self.assertEqual(1200, self.fixture.stone("loser"))


class AuctionSettlementParkingTests(unittest.TestCase):
    """A2: an item that cannot enter a backpack is parked, never destroyed."""

    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.fixture = AuctionDeliveryFixture(Path(self.temp_dir.name))

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def _repository(self, cap: int = 1) -> AuctionSettlementSqlRepository:
        return AuctionSettlementSqlRepository(self.fixture.database, max_goods_num=cap)

    def test_unsold_item_for_a_full_seller_is_parked_not_destroyed(self) -> None:
        self.fixture.add_auction(bids={})
        self.fixture.give("seller", 1001, 1)

        result = self._repository().settle_active(
            "settle-park-seller", end_time=300.0, fee_rate=0.1, item_types={1001: "装备"}
        )

        self.assertEqual("settled", result.status)
        self.assertEqual("待领取", result.results[0]["status"])
        self.assertEqual("pending", result.results[0]["delivery"])
        self.assertEqual(1, self.fixture.copies("seller", 1001))
        rows = self.fixture.pending()
        self.assertEqual(1, len(rows))
        self.assertEqual(("seller", "inventory_full", 1001), (
            rows[0]["role"], rows[0]["reason"], rows[0]["item_id"]
        ))
        self.assertEqual("卖家", rows[0]["owner_name"])
        self.assertIsNone(rows[0]["claimed_at"])
        self.assertEqual(0, self.fixture.auction_rows())
        self.assertEqual("settled", self.fixture.status())

    def test_full_winner_parks_the_item_instead_of_blocking_the_session(self) -> None:
        self.fixture.add_auction(bids={"winner": 900, "loser": 700})
        self.fixture.give("winner", 1001, 1)

        result = self._repository().settle_active(
            "settle-park-winner", end_time=300.0, fee_rate=0.1, item_types={1001: "装备"}
        )

        self.assertEqual("settled", result.status)
        self.assertEqual("待领取", result.results[0]["status"])
        self.assertEqual("pending", result.results[0]["delivery"])
        self.assertEqual(1, self.fixture.copies("winner", 1001))
        self.assertEqual(810, self.fixture.stone("seller"))
        self.assertEqual(1000, self.fixture.stone("winner"))
        self.assertEqual(1200, self.fixture.stone("loser"))
        rows = self.fixture.pending()
        self.assertEqual(1, len(rows))
        self.assertEqual(("winner", "买家"), (rows[0]["role"], rows[0]["owner_name"]))
        self.assertEqual(0, self.fixture.auction_rows())
        self.assertEqual("settled", self.fixture.status())

    def test_one_full_winner_does_not_block_the_other_item(self) -> None:
        self.fixture.add_auction(
            auction_id="full-winner", bids={"winner": 900}, item_id=1001, item_name="玩家法器"
        )
        self.fixture.add_auction(
            auction_id="room", bids={"loser": 300}, item_id=2002, item_name="灵草"
        )
        self.fixture.give("winner", 1001, 1)

        result = self._repository().settle_active(
            "settle-mixed",
            end_time=300.0,
            fee_rate=0.1,
            item_types={1001: "装备", 2002: "药材"},
        )

        self.assertEqual("settled", result.status)
        by_auction = {record["auction_id"]: record for record in result.results}
        self.assertEqual("待领取", by_auction["full-winner"]["status"])
        self.assertEqual("成交", by_auction["room"]["status"])
        self.assertEqual(1, self.fixture.copies("loser", 2002))
        self.assertEqual(["winner"], [row["role"] for row in self.fixture.pending()])
        self.assertEqual("settled", self.fixture.status())

    def test_missing_pending_table_rolls_the_settlement_back(self) -> None:
        self.fixture.add_auction(bids={"winner": 900})
        self.fixture.give("winner", 1001, 1)
        with DatabaseUnitOfWork(self.fixture.database) as uow:
            uow.execute("DROP TABLE auction_pending_items")

        with self.assertRaises(Exception):
            self._repository().settle_active(
                "settle-unmigrated", end_time=300.0, fee_rate=0.1, item_types={1001: "装备"}
            )

        self.assertEqual(1, self.fixture.copies("winner", 1001))
        self.assertEqual(1000, self.fixture.stone("winner"))
        self.assertEqual(0, self.fixture.stone("seller"))
        self.assertEqual(1, self.fixture.auction_rows())
        self.assertEqual("active", self.fixture.status())
        self.assertEqual([], self.fixture.history())

    def test_replay_keeps_a_single_pending_row(self) -> None:
        self.fixture.add_auction(bids={})
        self.fixture.give("seller", 1001, 1)
        repository = self._repository()

        first = repository.settle_active(
            "settle-replay", end_time=300.0, fee_rate=0.1, item_types={1001: "装备"}
        )
        replay = repository.settle_active(
            "settle-replay", end_time=300.0, fee_rate=0.1, item_types={1001: "装备"}
        )

        self.assertEqual("settled", first.status)
        self.assertEqual("duplicate", replay.status)
        self.assertEqual(1, len(self.fixture.pending()))
        self.assertEqual(1, self.fixture.copies("seller", 1001))


    def test_pending_tables_are_created_by_the_game_database_migration(self) -> None:
        from nonebot_plugin_xiuxian_2.plugin import build_migrations, migrations_for_database

        migrations = build_migrations()
        game = {item.version for item in migrations_for_database(migrations, "game_db")}
        self.assertIn("auction.009", game)
        with DatabaseUnitOfWork(self.fixture.database) as uow:
            tables = {
                str(row[0])
                for row in uow.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                ).fetchall()
            }
        self.assertIn("auction_pending_items", tables)
        self.assertIn("auction_pending_claim_operations", tables)


if __name__ == "__main__":
    unittest.main()
