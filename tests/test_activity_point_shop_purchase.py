import tempfile
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_activity.point_shop_service import ActivityPointShopPurchaseService
from tests.test_db_backend import db_backend


class ActivityPointShopPurchaseTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(); root = Path(self.tmp.name)
        self.activity, self.game = root / "activity.db", root / "game.db"
        with db_backend.transaction(self.activity) as conn:
            conn.execute("CREATE TABLE activity_point_balance(activity_key TEXT,user_id TEXT,points INTEGER,update_time TEXT,PRIMARY KEY(activity_key,user_id))")
            conn.execute("CREATE TABLE activity_point_purchase(activity_key TEXT,user_id TEXT,item_key TEXT,count INTEGER,update_time TEXT,PRIMARY KEY(activity_key,user_id,item_key))")
            conn.execute("INSERT INTO activity_point_balance VALUES('a','u',1000,'')")
        with db_backend.transaction(self.game) as conn:
            conn.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,stone INTEGER)")
            conn.execute("INSERT INTO user_xiuxian VALUES('u',10)")
            conn.execute("CREATE TABLE back(user_id TEXT,goods_id INTEGER,goods_name TEXT,goods_type TEXT,goods_num INTEGER,create_time TEXT,update_time TEXT,bind_num INTEGER,UNIQUE(user_id,goods_id))")
        self.service = ActivityPointShopPurchaseService(self.activity, self.game)
        self.rewards = [{"type": "stone", "quantity": 50}, {"id": 101, "name": "活动令", "type": "道具", "quantity": 2}]

    def tearDown(self): self.tmp.cleanup()

    def purchase(self, operation="op", **changes):
        args = dict(operation_id=operation,user_id="u",activity_key="a",item_key="i",quantity=2,unit_cost=100,
                    personal_limit=3,stock_limit=4,rewards=self.rewards,max_goods_num=100)
        args.update(changes); return self.service.purchase(**args)

    def test_atomic_purchase_and_idempotency(self):
        self.assertEqual("applied", self.purchase().status); self.assertEqual("duplicate", self.purchase().status)
        with db_backend.connection(self.activity) as conn:
            self.assertEqual((800, 2), tuple(conn.execute("SELECT b.points,p.count FROM activity_point_balance b JOIN activity_point_purchase p ON 1=1").fetchone()))
        with db_backend.connection(self.game) as conn:
            self.assertEqual(60, conn.execute("SELECT stone FROM user_xiuxian").fetchone()[0]); self.assertEqual(2, conn.execute("SELECT goods_num FROM back").fetchone()[0])

    def test_limits_and_rollback(self):
        self.assertEqual("personal_limit", self.purchase(quantity=4).status)
        with db_backend.transaction(self.activity) as conn:
            conn.execute("CREATE TABLE activity_point_purchase_operations(operation_id TEXT PRIMARY KEY,payload TEXT,quantity INTEGER,cost INTEGER,points INTEGER,personal_count INTEGER,total_count INTEGER)")
            conn.execute("CREATE TRIGGER fail_shop BEFORE INSERT ON activity_point_purchase_operations BEGIN SELECT RAISE(ABORT,'x'); END")
        with self.assertRaises(Exception): self.purchase("rollback")
        with db_backend.connection(self.activity) as conn: self.assertEqual(1000, conn.execute("SELECT points FROM activity_point_balance").fetchone()[0])
        with db_backend.connection(self.game) as conn: self.assertEqual(10, conn.execute("SELECT stone FROM user_xiuxian").fetchone()[0])


if __name__ == "__main__": unittest.main()
