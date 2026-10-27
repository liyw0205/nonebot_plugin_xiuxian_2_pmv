from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from nonebot_plugin_xiuxian_2.features.auction.migrations import apply_auction_settlement
from nonebot_plugin_xiuxian_2.features.auction.settlement_repository import (
    DEFAULT_MAX_GOODS_NUM,
    AuctionSettlementSqlRepository,
    _configured_max_goods_num,
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
            uow.execute(
                "INSERT INTO auction_sessions(session_id,status,start_time,end_time,items_count,"
                "start_operation_id) VALUES('session','active',100.0,200.0,1,'start')"
            )

    def add_auction(self, *, auction_id: str = "auction-1", bids: dict | None = None) -> None:
        with DatabaseUnitOfWork(self.database) as uow:
            uow.execute(
                "INSERT INTO auction_current(id,item_id,name,start_price,current_price,"
                "seller_id,seller_name,bids,is_system,last_bid_time) "
                "VALUES(?,?,?,?,?,?,?,?,?,?)",
                (
                    auction_id,
                    1001,
                    "玩家法器",
                    600,
                    900,
                    "seller",
                    "卖家",
                    json.dumps(bids or {}),
                    0,
                    100.0,
                ),
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
        configured = _configured_max_goods_num()
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


if __name__ == "__main__":
    unittest.main()
