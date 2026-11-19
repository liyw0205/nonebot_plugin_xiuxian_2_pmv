import sqlite3
import tempfile
import unittest
from datetime import datetime
from pathlib import Path

from ....core.errors import DomainError
from ..repository import DungeonPurchaseSqlRepository
from tests.test_db_backend import db_backend


class Clock:
    def now(self): return datetime(2026, 9, 15, 12)


class DungeonPurchaseRepositoryTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.db=Path(self.temp.name)/'game.db'
        with db_backend.transaction(self.db) as c:
            c.execute('CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,stone INTEGER)');c.execute("INSERT INTO user_xiuxian VALUES('u',100)")
            c.execute('CREATE TABLE back(user_id TEXT,goods_id INTEGER,goods_name TEXT,goods_type TEXT,goods_num INTEGER,create_time TEXT,update_time TEXT,bind_num INTEGER,UNIQUE(user_id,goods_id))')
            c.execute("CREATE TABLE dungeon_purchase_operations(operation_id TEXT PRIMARY KEY,payload TEXT,result_status TEXT,quantity INTEGER,cost INTEGER,stone INTEGER,inventory INTEGER,response TEXT)")
        self.repo=DungeonPurchaseSqlRepository(self.db,self.db,clock=Clock())
    def tearDown(self):self.temp.cleanup()
    def buy(self,op='x',stone=100):return self.repo.purchase(op,'u',1,'item','type',2,10,stone,99,1)
    def test_success_and_replay(self):
        self.assertEqual('applied',self.buy()['status']);self.assertEqual('duplicate',self.buy()['status'])
        with db_backend.connection(self.db) as c:self.assertEqual((80,2),tuple(c.execute('SELECT stone,(SELECT goods_num FROM back) FROM user_xiuxian').fetchone()))
    def test_stale_wallet_rejected(self):self.assertEqual('state_changed',self.buy('stale',99)['status'])
    def test_operation_failure_rolls_back(self):
        with db_backend.transaction(self.db) as c:c.execute("CREATE TRIGGER fail_dp BEFORE INSERT ON dungeon_purchase_operations BEGIN SELECT RAISE(ABORT,'failed'); END")
        before = self.snapshot()
        with self.assertRaisesRegex(sqlite3.IntegrityError, "failed"):
            self.buy('fail')
        self.assertEqual(before, self.snapshot())
        with db_backend.connection(self.db) as c:
            self.assertEqual("ok", c.execute("PRAGMA integrity_check").fetchone()[0])

    def test_write_rejections_roll_back_and_recover(self):
        cases = (
            (
                "reject_stone",
                "CREATE TRIGGER reject_stone BEFORE UPDATE OF stone ON user_xiuxian "
                "WHEN NEW.user_id='u' BEGIN SELECT RAISE(IGNORE); END",
                "dungeon_purchase_stone_write_rejected",
            ),
            (
                "rewrite_stone",
                "CREATE TRIGGER rewrite_stone AFTER UPDATE OF stone ON user_xiuxian "
                "WHEN NEW.user_id='u' BEGIN UPDATE user_xiuxian SET stone=stone+1 WHERE user_id='u'; END",
                "dungeon_purchase_stone_write_rejected",
            ),
            (
                "reject_inventory",
                "CREATE TRIGGER reject_inventory BEFORE INSERT ON back "
                "WHEN NEW.user_id='u' AND NEW.goods_id=1 BEGIN SELECT RAISE(IGNORE); END",
                "dungeon_purchase_inventory_write_rejected",
            ),
            (
                "rewrite_inventory",
                "CREATE TRIGGER rewrite_inventory AFTER INSERT ON back "
                "WHEN NEW.user_id='u' AND NEW.goods_id=1 "
                "BEGIN UPDATE back SET goods_num=99 WHERE user_id='u' AND goods_id=1; END",
                "dungeon_purchase_inventory_write_rejected",
            ),
            (
                "reject_receipt",
                "CREATE TRIGGER reject_receipt BEFORE INSERT ON dungeon_purchase_operations "
                "BEGIN SELECT RAISE(IGNORE); END",
                "dungeon_purchase_receipt_write_rejected",
            ),
            (
                "rewrite_receipt",
                "CREATE TRIGGER rewrite_receipt AFTER INSERT ON dungeon_purchase_operations "
                "BEGIN UPDATE dungeon_purchase_operations SET quantity=99 WHERE operation_id='x'; END",
                "dungeon_purchase_receipt_write_rejected",
            ),
        )
        for name, trigger, reason in cases:
            with self.subTest(trigger=name):
                self.temp.cleanup()
                self.setUp()
                with db_backend.transaction(self.db) as conn:
                    conn.execute(trigger)
                before = self.snapshot()
                try:
                    with self.assertRaises(DomainError) as caught:
                        self.buy()
                    self.assertEqual(reason, caught.exception.code)
                    self.assertEqual(reason, caught.exception.details["reason"])
                    self.assertEqual(before, self.snapshot())
                    with db_backend.connection(self.db) as conn:
                        self.assertEqual("ok", conn.execute("PRAGMA integrity_check").fetchone()[0])
                finally:
                    with db_backend.transaction(self.db) as conn:
                        conn.execute(f"DROP TRIGGER {name}")
                self.assertEqual("applied", self.buy()["status"])
                after = self.snapshot()
                self.assertEqual("duplicate", self.buy()["status"])
                self.assertEqual(after, self.snapshot())

    def snapshot(self):
        with db_backend.connection(self.db) as conn:
            stone = conn.execute("SELECT stone FROM user_xiuxian WHERE user_id='u'").fetchall()
            inventory = conn.execute(
                "SELECT goods_name,goods_type,goods_num,bind_num FROM back WHERE user_id='u' AND goods_id=1"
            ).fetchall()
            receipt = conn.execute(
                "SELECT payload,result_status,quantity,cost,stone,inventory,response "
                "FROM dungeon_purchase_operations WHERE operation_id='x'"
            ).fetchall()
        return stone, inventory, receipt


if __name__=='__main__':unittest.main()
