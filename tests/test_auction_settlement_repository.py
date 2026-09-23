from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from nonebot_plugin_xiuxian_2.features.auction.migrations import apply_auction_settlement
from nonebot_plugin_xiuxian_2.features.auction.settlement import AuctionSettlementApplication
from nonebot_plugin_xiuxian_2.features.auction.settlement_repository import AuctionSettlementSqlRepository
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork
from nonebot_plugin_xiuxian_2.plugin import apply_platform_schema


class AuctionSettlementSqlRepositoryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.database = Path(self.temp_dir.name) / "game.sqlite3"
        with DatabaseUnitOfWork(self.database) as uow:
            uow.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,user_name TEXT,stone INTEGER)")
            uow.execute(
                "CREATE TABLE back(user_id TEXT,goods_id INTEGER,goods_name TEXT,goods_type TEXT,"
                "goods_num INTEGER,create_time TEXT,update_time TEXT,bind_num INTEGER DEFAULT 0,"
                "UNIQUE(user_id,goods_id))"
            )
            uow.executemany(
                "INSERT INTO user_xiuxian(user_id,user_name,stone) VALUES(?,?,?)",
                (("seller", "卖家", 0), ("winner", "买家", 1000), ("loser", "落败者", 500)),
            )
            apply_auction_settlement(uow)
            uow.execute(
                "INSERT INTO auction_sessions(session_id,status,start_time,end_time,items_count,start_operation_id) "
                "VALUES(?,?,?,?,?,?)",
                ("session", "active", 100.0, 200.0, 1, "start"),
            )
            uow.execute(
                "INSERT INTO auction_current(id,item_id,name,start_price,current_price,seller_id,seller_name,bids,is_system,last_bid_time) "
                "VALUES(?,?,?,?,?,?,?,?,?,?)",
                ("auction-1", 1001, "玩家法器", 600, 900, "seller", "卖家", json.dumps({"winner": 900, "loser": 700}), 0, 100.0),
            )
        self.repository = AuctionSettlementSqlRepository(self.database, max_goods_num=99)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def settle(self, operation_id: str = "finish"):
        return self.repository.settle_active(
            operation_id, end_time=300.0, fee_rate=0.1, item_types={1001: "装备"}
        )

    def test_settles_assets_history_and_session_atomically(self) -> None:
        result = self.settle()
        self.assertEqual(result.status, "settled")
        self.assertEqual(result.results[0]["status"], "成交")
        with DatabaseUnitOfWork(self.database) as uow:
            users = {row["user_id"]: row["stone"] for row in uow.query_all("SELECT user_id,stone FROM user_xiuxian")}
            self.assertEqual(users, {"seller": 810, "winner": 1000, "loser": 1200})
            self.assertEqual(uow.query_one("SELECT goods_num FROM back WHERE user_id=?", ("winner",))["goods_num"], 1)
            self.assertEqual(uow.query_one("SELECT status FROM auction_sessions")["status"], "settled")
            self.assertEqual(uow.query_one("SELECT COUNT(*) AS n FROM auction_current")["n"], 0)
            self.assertEqual(uow.query_one("SELECT COUNT(*) AS n FROM auction_history")["n"], 1)

    def test_replay_is_idempotent(self) -> None:
        first = self.settle()
        replay = self.settle()
        self.assertEqual(first.status, "settled")
        self.assertEqual(replay.status, "duplicate")
        with DatabaseUnitOfWork(self.database) as uow:
            self.assertEqual(uow.query_one("SELECT stone FROM user_xiuxian WHERE user_id=?", ("seller",))["stone"], 810)
            self.assertEqual(uow.query_one("SELECT COUNT(*) AS n FROM auction_history")["n"], 1)

    def test_inventory_full_keeps_everything_pending(self) -> None:
        with DatabaseUnitOfWork(self.database) as uow:
            uow.execute(
                "INSERT INTO back(user_id,goods_id,goods_name,goods_type,goods_num,bind_num) VALUES(?,?,?,?,?,?)",
                ("winner", 1001, "玩家法器", "装备", 99, 99),
            )
        result = self.settle()
        self.assertEqual(result.status, "inventory_full")
        with DatabaseUnitOfWork(self.database) as uow:
            self.assertEqual(uow.query_one("SELECT stone FROM user_xiuxian WHERE user_id=?", ("seller",))["stone"], 0)
            self.assertEqual(uow.query_one("SELECT status FROM auction_sessions")["status"], "active")
            self.assertEqual(uow.query_one("SELECT COUNT(*) AS n FROM auction_current")["n"], 1)

    def test_later_business_rejection_rolls_back_earlier_item_settlement(self) -> None:
        with DatabaseUnitOfWork(self.database) as uow:
            uow.execute(
                "INSERT INTO auction_current(id,item_id,name,start_price,current_price,seller_id,seller_name,bids,is_system,last_bid_time) "
                "VALUES(?,?,?,?,?,?,?,?,?,?)",
                ("auction-2", 1002, "第二件拍品", 600, 900, "seller", "卖家", json.dumps({"missing": 900}), 0, 100.0),
            )
        result = self.repository.settle_active(
            "finish", end_time=300.0, fee_rate=0.1, item_types={1001: "装备", 1002: "装备"}
        )
        self.assertEqual(result.status, "participant_missing")
        with DatabaseUnitOfWork(self.database) as uow:
            users = {row["user_id"]: row["stone"] for row in uow.query_all("SELECT user_id,stone FROM user_xiuxian")}
            self.assertEqual(users, {"seller": 0, "winner": 1000, "loser": 500})
            self.assertEqual(uow.query_one("SELECT COUNT(*) AS n FROM back")["n"], 0)
            self.assertEqual(uow.query_one("SELECT COUNT(*) AS n FROM auction_history")["n"], 0)
            self.assertEqual(uow.query_one("SELECT COUNT(*) AS n FROM auction_current")["n"], 2)
            self.assertEqual(uow.query_one("SELECT status FROM auction_sessions")["status"], "active")

    def test_operation_insert_failure_rolls_back_all_asset_changes(self) -> None:
        with DatabaseUnitOfWork(self.database) as uow:
            uow.execute(
                "CREATE TRIGGER reject_finish BEFORE INSERT ON auction_session_operations "
                "WHEN NEW.action='finish' BEGIN SELECT RAISE(ABORT,'reject'); END"
            )
        with self.assertRaises(Exception):
            self.settle()
        with DatabaseUnitOfWork(self.database) as uow:
            self.assertEqual(uow.query_one("SELECT stone FROM user_xiuxian WHERE user_id=?", ("seller",))["stone"], 0)
            self.assertIsNone(uow.query_one("SELECT goods_num FROM back WHERE user_id=?", ("winner",)))
            self.assertEqual(uow.query_one("SELECT COUNT(*) AS n FROM auction_history")["n"], 0)
            self.assertEqual(uow.query_one("SELECT status FROM auction_sessions")["status"], "active")

    def test_application_default_uses_feature_repository(self) -> None:
        with DatabaseUnitOfWork(self.database) as uow:
            apply_platform_schema(uow)
        outcome = AuctionSettlementApplication(self.database).settle_active(
            operation_id="application-finish",
            end_time=300.0,
            fee_rate=0.1,
            item_types={1001: "装备"},
        )
        self.assertTrue(outcome.ok)
        self.assertEqual(outcome.data["status"], "settled")
        with DatabaseUnitOfWork(self.database) as uow:
            self.assertEqual(
                uow.query_one(
                    "SELECT status FROM operation_ledger WHERE operation_id=?",
                    ("application-finish",),
                )["status"],
                "applied",
            )


if __name__ == "__main__":
    unittest.main()
