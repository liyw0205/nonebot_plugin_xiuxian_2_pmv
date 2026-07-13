from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_dufang.bet_service import DufangBetService
from tests.test_db_backend import db_backend


class DufangBetServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        root = Path(self.temp.name)
        self.game = root / "game.db"
        self.player = root / "player.db"
        with db_backend.transaction(self.game) as conn:
            conn.execute("CREATE TABLE user_xiuxian (user_id TEXT PRIMARY KEY, stone INTEGER)")
            conn.execute("INSERT INTO user_xiuxian VALUES (%s,%s)", ("user", 1000))
        self.service = DufangBetService(self.game, self.player)

    def tearDown(self) -> None:
        self.temp.cleanup()

    def state(self):
        with db_backend.connection(self.game) as conn:
            stone = int(conn.execute("SELECT stone FROM user_xiuxian WHERE user_id=%s", ("user",)).fetchone()[0])
            bets = int(conn.execute("SELECT COUNT(*) FROM dufang_bets").fetchone()[0]) if conn.table_exists("dufang_bets") else 0
        with db_backend.connection(self.player) as conn:
            row = conn.execute("SELECT count,total_cost FROM unseal_data WHERE user_id=%s", ("user",)).fetchone() if conn.table_exists("unseal_data") else None
        return stone, bets, None if row is None else (int(row[0]), int(row[1]))

    def test_place_and_duplicate(self) -> None:
        first = self.service.place("bet", "user", 300, "now")
        duplicate = self.service.place("bet", "user", 300, "now")
        self.assertEqual((first.status, duplicate.status), ("applied", "duplicate"))
        self.assertEqual(self.state(), (700, 1, (1, 300)))

    def test_insufficient_and_conflict_change_nothing(self) -> None:
        self.assertEqual(self.service.place("large", "user", 1200, "now").status, "stone_insufficient")
        self.service.place("same", "user", 200, "now")
        self.assertEqual(self.service.place("same", "user", 201, "now").status, "state_changed")
        self.assertEqual(self.state(), (800, 1, (1, 200)))

    def test_operation_failure_rolls_back(self) -> None:
        with db_backend.transaction(self.game) as conn:
            conn.execute("CREATE TABLE dufang_bet_operations (operation_id TEXT PRIMARY KEY,payload TEXT,cost INTEGER,wallet_stone INTEGER,bet_id TEXT,created_at TIMESTAMP)")
            conn.execute("CREATE TRIGGER fail_bet BEFORE INSERT ON dufang_bet_operations BEGIN SELECT RAISE(ABORT,'failed'); END")
        with self.assertRaises(db_backend.IntegrityError):
            self.service.place("rollback", "user", 300, "now")
        self.assertEqual(self.state(), (1000, 0, None))


if __name__ == "__main__":
    unittest.main()
