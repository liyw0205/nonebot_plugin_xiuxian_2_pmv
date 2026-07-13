import tempfile
import unittest
from pathlib import Path

import nonebot
nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_trade.repository import TradeRepository
from tests.test_db_backend import db_backend


class AuctionBidTransactionTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(); self.db = Path(self.temp.name)/"bid.db"
        with db_backend.transaction(self.db) as conn:
            conn.execute("CREATE TABLE user_xiuxian (user_id TEXT PRIMARY KEY,stone INTEGER)")
            conn.executemany("INSERT INTO user_xiuxian VALUES (%s,%s)", (("seller",0),("old",50),("new",500)))
        self.repo = TradeRepository(self.db,max_goods_num=99); self.repo.initialize()
        self.repo.set_current_auction([{"id":"a","item_id":1,"name":"item","start_price":100,"current_price":200,"seller_id":"seller","seller_name":"s","bids":{"old":200},"bid_times":{},"is_system":False,"last_bid_time":1}])
    def tearDown(self): self.temp.cleanup()
    def stones(self,user):
        with db_backend.connection(self.db) as conn: return int(conn.execute("SELECT stone FROM user_xiuxian WHERE user_id=%s",(user,)).fetchone()[0])
    def test_bid_debits_updates_and_refunds_atomically(self):
        result=self.repo.place_auction_bid("op","a","new",300,200,{"old":200},2)
        self.assertEqual((result.status,self.stones("new"),self.stones("old")),("bid",200,250))
        self.assertEqual(self.repo.get_current_auction("a")["bids"],{"new":300})
    def test_duplicate_and_conflict(self):
        self.repo.place_auction_bid("op","a","new",300,200,{"old":200},2)
        duplicate=self.repo.place_auction_bid("op","a","new",300,200,{"old":200},3)
        conflict=self.repo.place_auction_bid("op","a","new",301,300,{"new":300},3)
        self.assertEqual((duplicate.status,conflict.status),("duplicate","state_changed"))
    def test_operation_failure_rolls_back(self):
        with db_backend.transaction(self.db) as conn:
            conn.execute("CREATE TABLE auction_bid_operations (operation_id TEXT PRIMARY KEY,payload TEXT,auction_id TEXT,bidder_id TEXT,bid_price INTEGER,debit INTEGER,refunded_bidder TEXT,refunded_amount INTEGER)")
            conn.execute("CREATE TRIGGER fail_bid BEFORE INSERT ON auction_bid_operations BEGIN SELECT RAISE(ABORT,'failed'); END")
        with self.assertRaises(db_backend.IntegrityError): self.repo.place_auction_bid("fail","a","new",300,200,{"old":200},2)
        self.assertEqual((self.stones("new"),self.stones("old")),(500,50))
        self.assertEqual(self.repo.get_current_auction("a")["current_price"],200)

if __name__=='__main__': unittest.main()
