import json
import tempfile
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_rift.demon_token_battle_settlement_service import RiftDemonTokenBattleSettlementService
from tests.test_db_backend import db_backend


class RiftDemonTokenBattleSettlementTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        root = Path(self.temp_dir.name)
        self.game_db, self.player_db = root / "game.db", root / "player.db"
        self.rift = {"name": "boss", "rank": 4}
        with db_backend.transaction(self.game_db) as conn:
            conn.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,stone INTEGER,exp INTEGER,hp INTEGER,mp INTEGER)")
            conn.execute("CREATE TABLE user_cd(user_id TEXT PRIMARY KEY,type INTEGER,create_time TEXT,scheduled_time TEXT)")
            conn.execute("CREATE TABLE rift_entries(user_id TEXT PRIMARY KEY,rift_data TEXT,status TEXT)")
            conn.execute("CREATE TABLE back(user_id TEXT,goods_id INTEGER,goods_name TEXT,goods_type TEXT,goods_num INTEGER,create_time TEXT,update_time TEXT,bind_num INTEGER,UNIQUE(user_id,goods_id))")
            conn.execute("INSERT INTO user_xiuxian VALUES('u',1000,500,100,80)")
            conn.execute("INSERT INTO user_cd VALUES('u',3,'now','30')")
            conn.execute("INSERT INTO rift_entries VALUES('u',%s,'active')", (json.dumps(self.rift),))
            conn.execute("INSERT INTO back VALUES('u',20018,'token','item',1,'','',1)")
        with db_backend.transaction(self.player_db) as conn:
            conn.execute('CREATE TABLE rift(user_id TEXT PRIMARY KEY,"explore_count" INTEGER)')
            conn.execute("INSERT INTO rift VALUES('u',3)")
        self.service = RiftDemonTokenBattleSettlementService(self.game_db, self.player_db)
        self.user = {"stone": 1000, "exp": 500, "hp": 100, "mp": 80}
        self.win = {"delta": {"stone": 250, "exp": 75, "hp": -30, "mp": -20}, "statistics": {"rift_combat": 1}, "message": "fixed win"}

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_win_commits_all_battle_effects(self):
        result = self.service.settle("boss-op", "u", 20018, self.rift, self.user, 3, self.win, 1000)
        self.assertEqual((result.status, result.explore_count), ("applied", 4))
        with db_backend.connection(self.game_db) as conn:
            self.assertEqual(tuple(conn.execute("SELECT stone,exp,hp,mp FROM user_xiuxian").fetchone()), (1250, 575, 70, 60))
            self.assertEqual(
                tuple(
                    conn.execute(
                        "SELECT goods_num,bind_num FROM back WHERE goods_id=20018"
                    ).fetchone()
                ),
                (0, 0),
            )
            self.assertEqual(conn.execute("SELECT status FROM rift_entries").fetchone()[0], "settled")
        with db_backend.connection(self.player_db) as conn:
            self.assertEqual(conn.execute('SELECT "explore_count" FROM rift').fetchone()[0], 4)
            self.assertEqual(conn.execute('SELECT "rift_combat" FROM statistics').fetchone()[0], 1)

    def test_loss_replay_is_idempotent(self):
        loss = {"delta": {"hp": -99, "mp": -79}, "statistics": {"rift_combat": 1}, "message": "fixed loss"}
        first = self.service.settle("loss", "u", 20018, self.rift, self.user, 3, loss, 1000)
        replay = self.service.settle("loss", "u", 20018, self.rift, self.user, 3, loss, 1000)
        self.assertEqual((first.status, replay.status), ("applied", "duplicate"))
        with db_backend.connection(self.game_db) as conn:
            self.assertEqual(tuple(conn.execute("SELECT stone,exp,hp,mp FROM user_xiuxian").fetchone()), (1000, 500, 1, 1))

    def test_missing_token_is_rejected_without_state_changes(self):
        with db_backend.transaction(self.game_db) as conn:
            conn.execute(
                "UPDATE back SET goods_num=0 WHERE user_id='u' AND goods_id=20018"
            )
        result = self.service.settle(
            "missing", "u", 20018, self.rift, self.user, 3, self.win, 1000
        )
        self.assertEqual("item_missing", result.status)
        with db_backend.connection(self.game_db) as conn:
            self.assertEqual(
                (1000, 500, 100, 80),
                tuple(
                    conn.execute(
                        "SELECT stone,exp,hp,mp FROM user_xiuxian WHERE user_id='u'"
                    ).fetchone()
                ),
            )
            self.assertEqual(
                "active",
                conn.execute(
                    "SELECT status FROM rift_entries WHERE user_id='u'"
                ).fetchone()[0],
            )
        with db_backend.connection(self.player_db) as conn:
            self.assertEqual(
                3,
                conn.execute(
                    'SELECT "explore_count" FROM rift WHERE user_id=\'u\''
                ).fetchone()[0],
            )

    def test_statistics_failure_rolls_back_everything(self):
        with db_backend.transaction(self.player_db) as conn:
            conn.execute('CREATE TABLE statistics(user_id TEXT PRIMARY KEY,"rift_combat" INTEGER)')
            conn.execute("CREATE TRIGGER fail_stats BEFORE INSERT ON statistics BEGIN SELECT RAISE(ABORT,'fail'); END")
        with self.assertRaises(Exception):
            self.service.settle("boss-op", "u", 20018, self.rift, self.user, 3, self.win, 1000)
        with db_backend.connection(self.game_db) as conn:
            self.assertEqual(
                (1, 1),
                tuple(
                    conn.execute(
                        "SELECT goods_num,bind_num FROM back WHERE goods_id=20018"
                    ).fetchone()
                ),
            )
            self.assertEqual(conn.execute("SELECT status FROM rift_entries").fetchone()[0], "active")
            self.assertEqual(tuple(conn.execute("SELECT stone,exp,hp,mp FROM user_xiuxian").fetchone()), (1000, 500, 100, 80))
        with db_backend.connection(self.player_db) as conn:
            self.assertEqual(conn.execute('SELECT "explore_count" FROM rift').fetchone()[0], 3)


if __name__ == "__main__":
    unittest.main()
