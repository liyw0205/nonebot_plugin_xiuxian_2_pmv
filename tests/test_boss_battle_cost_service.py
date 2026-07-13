from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_boss.battle_cost_service import BossBattleCostService
from tests.test_db_backend import db_backend


class BossBattleCostServiceTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        root = Path(self.tmp.name)
        self.game, self.player = root / "game.db", root / "player.db"
        with db_backend.transaction(self.game) as conn:
            conn.execute("CREATE TABLE user_xiuxian (user_id TEXT PRIMARY KEY,user_stamina INTEGER,hp INTEGER,exp INTEGER)")
            conn.execute("INSERT INTO user_xiuxian VALUES ('u',50,100,500)")
            conn.execute("CREATE TABLE user_cd (user_id TEXT PRIMARY KEY,last_check_info_time TEXT)")
            conn.execute("INSERT INTO user_cd VALUES ('u','old')")
        with db_backend.transaction(self.player) as conn:
            conn.execute("CREATE TABLE boss (user_id TEXT PRIMARY KEY,boss_battle_count INTEGER)")
            conn.execute("INSERT INTO boss VALUES ('u',2)")
        self.service = BossBattleCostService(self.game, self.player)

    def tearDown(self): self.tmp.cleanup()

    def state(self):
        with db_backend.connection(self.game) as conn:
            user = conn.execute("SELECT user_stamina,last_check_info_time FROM user_xiuxian JOIN user_cd USING(user_id)").fetchone()
        with db_backend.connection(self.player) as conn:
            count = conn.execute("SELECT boss_battle_count FROM boss").fetchone()[0]
        return int(user[0]), str(user[1]), int(count)

    def consume(self, operation="op", **kw):
        values = dict(stamina=50, hp=100, exp=500, count=2)
        values.update(kw)
        return self.service.consume(operation, "u", 10, 30, values["stamina"], values["hp"],
                                    values["exp"], values["count"], "old", "new")

    def test_success_duplicate_and_snapshot_recheck(self):
        self.assertEqual(self.consume().status, "applied")
        self.assertEqual(self.state(), (40, "new", 3))
        self.assertEqual(self.consume().status, "duplicate")
        self.assertEqual(self.consume("stale", stamina=50).status, "state_changed")

    def test_business_rejections(self):
        with db_backend.transaction(self.game) as conn:
            conn.execute("UPDATE user_xiuxian SET user_stamina=5")
        self.assertEqual(self.consume("low", stamina=5).status, "stamina_insufficient")
        with db_backend.transaction(self.game) as conn:
            conn.execute("UPDATE user_xiuxian SET user_stamina=50,hp=50")
        self.assertEqual(self.consume("hurt", hp=50).status, "hp_insufficient")
        with db_backend.transaction(self.game) as conn:
            conn.execute("UPDATE user_xiuxian SET hp=100")
        with db_backend.transaction(self.player) as conn:
            conn.execute("UPDATE boss SET boss_battle_count=30")
        self.assertEqual(self.consume("limit", count=30).status, "limit_reached")
        with db_backend.transaction(self.player) as conn:
            conn.execute("UPDATE boss SET boss_battle_count=2")
        self.assertEqual(self.state(), (50, "old", 2))

    def test_operation_failure_rolls_back(self):
        with db_backend.transaction(self.game) as conn:
            conn.execute("CREATE TABLE boss_battle_cost_operations (operation_id TEXT PRIMARY KEY,payload TEXT,stamina INTEGER,battle_count INTEGER,checked_at TEXT,created_at TEXT)")
            conn.execute("CREATE TRIGGER fail_cost BEFORE INSERT ON boss_battle_cost_operations BEGIN SELECT RAISE(ABORT,'failed'); END")
        with self.assertRaises(db_backend.IntegrityError): self.consume("rollback")
        self.assertEqual(self.state(), (50, "old", 2))


if __name__ == "__main__": unittest.main()
