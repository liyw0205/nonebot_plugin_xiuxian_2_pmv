import tempfile
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.features.trade.application import TradeApplication
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_trade.repository import TradeRepository
from tests.test_db_backend import db_backend
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork
from nonebot_plugin_xiuxian_2.plugin import apply_platform_schema


class TradeApplicationPurchaseTests(unittest.TestCase):
    def test_default_purchase_uses_feature_repository(self):
        with tempfile.TemporaryDirectory() as temp:
            database = Path(temp) / "trade.db"
            with db_backend.transaction(database) as conn:
                conn.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,stone INTEGER)")
                conn.execute("INSERT INTO user_xiuxian VALUES('buyer',1000)")
                conn.execute("INSERT INTO user_xiuxian VALUES('seller',100)")
                conn.execute("CREATE TABLE back(user_id TEXT,goods_id INTEGER,goods_name TEXT,goods_type TEXT,goods_num INTEGER,create_time TEXT,update_time TEXT,bind_num INTEGER,PRIMARY KEY(user_id,goods_id))")
            repository = TradeRepository(database, max_goods_num=99)
            with DatabaseUnitOfWork(database) as uow:
                apply_platform_schema(uow)
            repository.initialize()
            listing = repository.add_xianshi_item("seller", 1001, "法器", "装备", 200, 2)
            outcome = TradeApplication(database, database).purchase(
                operation_id="trade-feature-purchase", user_id="buyer", listing_id=listing, quantity=1, max_goods_num=99
            )
            replay = TradeApplication(database, database).purchase(
                operation_id="trade-feature-purchase", user_id="buyer", listing_id=listing, quantity=1, max_goods_num=99
            )
            self.assertEqual((outcome.status, replay.status), ("applied", "replayed"))
