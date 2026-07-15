from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_arena.transaction_service import ArenaBattleSettlementService
from tests.test_db_backend import db_backend


class ArenaBattleSettlementServiceTests(unittest.TestCase):
    arena = {"score": 1490, "total_wins": 2, "total_losses": 1, "win_streak": 2, "max_win_streak": 2, "rank": "青铜"}
    opponent = {"score": 1600, "total_wins": 4, "total_losses": 2, "win_streak": 1, "max_win_streak": 3, "rank": "白银"}

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        root = Path(self.temp.name)
        self.game, self.player = root / "game.db", root / "player.db"
        with db_backend.transaction(self.game) as conn:
            conn.execute("CREATE TABLE user_xiuxian (user_id TEXT PRIMARY KEY,hp INTEGER,mp INTEGER)")
            conn.execute("INSERT INTO user_xiuxian VALUES (%s,%s,%s)", ("user", 50, 40))
            conn.execute("INSERT INTO user_xiuxian VALUES (%s,%s,%s)", ("other", 60, 45))
        with db_backend.transaction(self.player) as conn:
            conn.execute("CREATE TABLE arena (user_id TEXT PRIMARY KEY,score INTEGER,total_wins INTEGER,total_losses INTEGER,win_streak INTEGER,max_win_streak INTEGER,rank TEXT)")
            conn.execute("INSERT INTO arena VALUES (%s,%s,%s,%s,%s,%s,%s)", ("user", *self.arena.values()))
            conn.execute("INSERT INTO arena VALUES (%s,%s,%s,%s,%s,%s,%s)", ("other", *self.opponent.values()))
        self.service = ArenaBattleSettlementService(self.game, self.player)

    def tearDown(self):
        self.temp.cleanup()

    def settle(self, operation="battle", **overrides):
        values = dict(challenger=self.arena, opponent=self.opponent, challenger_player={"hp": 50, "mp": 40}, opponent_player={"hp": 60, "mp": 45}, outcome="win")
        values.update(overrides)
        return self.service.settle(operation, "user", "other", values["outcome"], values["challenger"], values["opponent"], values["challenger_player"], values["opponent_player"], 11, 12, 21, 22, 20, 10, 10)

    def state(self):
        with db_backend.connection(self.player) as conn:
            arena = conn.execute("SELECT score,total_wins,total_losses,win_streak,max_win_streak,rank FROM arena WHERE user_id=%s", ("user",)).fetchone()
            opponent = conn.execute("SELECT score,total_wins,total_losses,win_streak,max_win_streak,rank FROM arena WHERE user_id=%s", ("other",)).fetchone()
        with db_backend.connection(self.game) as conn:
            players = conn.execute("SELECT user_id,hp,mp FROM user_xiuxian ORDER BY user_id").fetchall()
        return tuple(arena), tuple(opponent), [tuple(row) for row in players]

    def test_win_commits_both_records_and_vitals(self):
        result = self.settle()
        self.assertEqual((result.status, result.challenger_score, result.challenger_rank, result.opponent_score), ("applied", 1510, "白银", 1590))
        self.assertEqual(self.state(), ((1510, 3, 1, 3, 3, "白银"), (1590, 4, 3, 0, 3, "白银"), [("other", 21, 22), ("user", 11, 12)]))

    def test_duplicate_and_changed_snapshot_are_idempotent(self):
        first, duplicate = self.settle("same"), self.settle("same")
        changed = self.settle("same", outcome="loss")
        self.assertEqual((first.status, duplicate.status, changed.status), ("applied", "duplicate", "state_changed"))

    def test_stale_snapshot_and_operation_failure_roll_back(self):
        stale = {**self.arena, "score": 1491}
        self.assertEqual(self.settle("stale", challenger=stale).status, "state_changed")
        with db_backend.transaction(self.game) as conn:
            conn.execute("CREATE TABLE arena_battle_settlement_operations (operation_id TEXT PRIMARY KEY,payload TEXT,challenger_score INTEGER,challenger_rank TEXT,opponent_score INTEGER,created_at TIMESTAMP)")
            conn.execute("CREATE TRIGGER fail_battle BEFORE INSERT ON arena_battle_settlement_operations BEGIN SELECT RAISE(ABORT,'failed'); END")
        with self.assertRaises(db_backend.IntegrityError):
            self.settle("rollback")
        self.assertEqual(self.state(), ((1490, 2, 1, 2, 2, "青铜"), (1600, 4, 2, 1, 3, "白银"), [("other", 60, 45), ("user", 50, 40)]))


if __name__ == "__main__":
    unittest.main()
