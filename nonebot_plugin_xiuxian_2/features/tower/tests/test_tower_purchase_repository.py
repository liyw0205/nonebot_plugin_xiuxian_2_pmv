import json
import tempfile
import unittest
from datetime import date
from pathlib import Path

from ..repository import TowerPurchaseSqlRepository
from tests.test_db_backend import db_backend


class TowerPurchaseSqlRepositoryTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        root = Path(self.temp.name)
        self.game, self.player = root / "game.db", root / "player.db"
        with db_backend.transaction(self.game) as conn:
            conn.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY)")
            conn.execute("INSERT INTO user_xiuxian VALUES('u')")
            conn.execute("CREATE TABLE back(user_id TEXT,goods_id INTEGER,goods_name TEXT,goods_type TEXT,goods_num INTEGER,create_time TEXT,update_time TEXT,bind_num INTEGER,UNIQUE(user_id,goods_id))")
            conn.execute("CREATE TABLE tower_purchase_operations(operation_id TEXT PRIMARY KEY,payload TEXT,quantity INTEGER,cost INTEGER,score INTEGER,purchased INTEGER,inventory INTEGER)")
        with db_backend.transaction(self.player) as conn:
            conn.execute("CREATE TABLE tower(user_id TEXT PRIMARY KEY,score INTEGER,weekly_purchases TEXT)")
            conn.execute("INSERT INTO tower VALUES('u',100,?)", (json.dumps({'_last_reset':'2026-09-15','1':1}),))
        self.repo = TowerPurchaseSqlRepository(self.game, self.player)

    def tearDown(self):
        self.temp.cleanup()

    def buy(self, operation="op", **values):
        params = dict(item_id=1,item_name="item",item_type="type",quantity=2,unit_cost=10,weekly_limit=5,expected_score=100,expected_weekly_purchases={'_last_reset':'2026-09-15','1':1},max_goods_num=99,bind_flag=1,today=date(2026,9,15))
        params.update(values)
        return self.repo.purchase(operation, "u", **params)

    def state(self):
        with db_backend.connection(self.game) as conn:
            item = conn.execute("SELECT goods_num,bind_num FROM back").fetchone()
        with db_backend.connection(self.player) as conn:
            tower = conn.execute("SELECT score,weekly_purchases FROM tower").fetchone()
        return int(tower[0]), json.loads(str(tower[1])), tuple(item) if item else None

    def test_success_duplicate_conflict(self):
        first = self.buy()
        duplicate = self.buy()
        conflict = self.buy(quantity=1)
        self.assertEqual((first['status'],duplicate['status'],conflict['status']), ('applied','duplicate','state_changed'))
        self.assertEqual(self.state(), (80, {'_last_reset':'2026-09-15','1':3}, (2,2)))

    def test_rejections_and_rollback(self):
        self.assertEqual('limit_reached', self.buy('limit', quantity=5)['status'])
        self.assertEqual('score_insufficient', self.buy('poor', unit_cost=60)['status'])
        with db_backend.transaction(self.game) as conn:
            conn.execute("CREATE TRIGGER fail_tower BEFORE INSERT ON tower_purchase_operations BEGIN SELECT RAISE(ABORT,'failed'); END")
        with self.assertRaises(Exception):
            self.buy('rollback')
        self.assertEqual(self.state(), (100, {'_last_reset':'2026-09-15','1':1}, None))


if __name__ == '__main__':
    unittest.main()
