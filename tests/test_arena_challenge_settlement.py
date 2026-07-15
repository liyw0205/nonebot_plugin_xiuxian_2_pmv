from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_arena.transaction_service import (
    ArenaChallengeSettlementService,
)
from tests.test_db_backend import db_backend


class ArenaChallengeSettlementTests(unittest.TestCase):
    challenger_arena = {
        "score": 1490,
        "total_wins": 2,
        "total_losses": 1,
        "win_streak": 2,
        "max_win_streak": 2,
        "rank": "青铜",
        "daily_challenges_used": 2,
        "daily_extra_challenges": 1,
        "last_challenge_time": "old",
    }
    opponent_arena = {
        "score": 1600,
        "total_wins": 4,
        "total_losses": 2,
        "win_streak": 1,
        "max_win_streak": 3,
        "rank": "白银",
    }
    challenger_player = {"hp": 50, "mp": 40, "user_stamina": 8}
    opponent_player = {"hp": 60, "mp": 45}

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        root = Path(self.temp.name)
        self.game = root / "game.db"
        self.player = root / "player.db"
        with db_backend.transaction(self.game) as conn:
            conn.execute(
                "CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,hp INTEGER,"
                "mp INTEGER,user_stamina INTEGER)"
            )
            conn.execute("INSERT INTO user_xiuxian VALUES('user',50,40,8)")
            conn.execute("INSERT INTO user_xiuxian VALUES('other',60,45,9)")
        with db_backend.transaction(self.player) as conn:
            conn.execute(
                "CREATE TABLE arena(user_id TEXT PRIMARY KEY,score INTEGER,total_wins INTEGER,"
                "total_losses INTEGER,win_streak INTEGER,max_win_streak INTEGER,rank TEXT,"
                "daily_challenges_used INTEGER,daily_extra_challenges INTEGER,"
                "last_challenge_time TEXT)"
            )
            conn.execute(
                "INSERT INTO arena VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                ("user", *self.challenger_arena.values()),
            )
            conn.execute(
                "INSERT INTO arena VALUES(%s,%s,%s,%s,%s,%s,%s,0,0,'')",
                ("other", *self.opponent_arena.values()),
            )
        self.service = ArenaChallengeSettlementService(self.game, self.player)

    def tearDown(self):
        self.temp.cleanup()

    def settle(self, operation="challenge", **overrides):
        values = {
            "opponent_id": "other",
            "outcome": "win",
            "cap": 11,
            "stamina_cost": 3,
            "challenged_at": "new",
            "challenger_arena": self.challenger_arena,
            "opponent_arena": self.opponent_arena,
            "challenger_player": self.challenger_player,
            "opponent_player": self.opponent_player,
            "final_challenger": (11, 12),
            "final_opponent": (21, 22),
        }
        values.update(overrides)
        return self.service.settle(
            operation,
            "user",
            values["opponent_id"],
            values["outcome"],
            values["cap"],
            values["stamina_cost"],
            values["challenged_at"],
            values["challenger_arena"],
            values["opponent_arena"],
            values["challenger_player"],
            values["opponent_player"],
            *values["final_challenger"],
            *values["final_opponent"],
            20,
            10,
            10,
        )

    def state(self):
        with db_backend.connection(self.player) as conn:
            arena = conn.execute(
                "SELECT score,total_wins,total_losses,win_streak,max_win_streak,rank,"
                "daily_challenges_used,daily_extra_challenges,last_challenge_time "
                "FROM arena WHERE user_id='user'"
            ).fetchone()
            opponent = conn.execute(
                "SELECT score,total_wins,total_losses,win_streak,max_win_streak,rank "
                "FROM arena WHERE user_id='other'"
            ).fetchone()
        with db_backend.connection(self.game) as conn:
            players = conn.execute(
                "SELECT user_id,hp,mp,user_stamina FROM user_xiuxian ORDER BY user_id"
            ).fetchall()
        return tuple(arena), tuple(opponent), [tuple(row) for row in players]

    def pristine_state(self):
        return (
            tuple(self.challenger_arena.values()),
            tuple(self.opponent_arena.values()),
            [("other", 60, 45, 9), ("user", 50, 40, 8)],
        )

    def test_win_commits_cost_vitals_and_both_arena_records(self):
        result = self.settle()

        self.assertEqual("applied", result.status)
        self.assertEqual((1510, "白银", 1590), (
            result.challenger_score,
            result.challenger_rank,
            result.opponent_score,
        ))
        self.assertEqual((3, 8, 5), (result.used, result.remaining, result.stamina))
        self.assertEqual(
            (
                (1510, 3, 1, 3, 3, "白银", 3, 1, "new"),
                (1590, 4, 3, 0, 3, "白银"),
                [("other", 21, 22, 9), ("user", 11, 12, 5)],
            ),
            self.state(),
        )

    def test_duplicate_restores_result_and_payload_conflict_is_rejected(self):
        first = self.settle("same")
        duplicate = self.settle("same")
        conflict = self.settle("same", outcome="loss")
        recovered = self.service.get_result("same", "user")

        self.assertEqual(
            ("applied", "duplicate", "operation_conflict", "duplicate"),
            (first.status, duplicate.status, conflict.status, recovered.status),
        )
        self.assertEqual(first.challenger_score, recovered.challenger_score)
        self.assertEqual(
            "operation_conflict", self.service.get_result("same", "another").status
        )

    def test_limit_stamina_and_stale_snapshots_change_nothing(self):
        self.assertEqual("limit_reached", self.settle("limit", cap=2).status)
        self.assertEqual(
            "stamina_insufficient",
            self.settle("stamina", stamina_cost=9).status,
        )
        stale = {**self.challenger_arena, "score": 1491}
        self.assertEqual(
            "state_changed",
            self.settle("stale", challenger_arena=stale).status,
        )
        stale_opponent = {**self.opponent_arena, "score": 1601}
        self.assertEqual(
            "state_changed",
            self.settle("stale-opponent", opponent_arena=stale_opponent).status,
        )
        self.assertEqual(self.pristine_state(), self.state())

    def test_no_match_still_commits_cost_and_consolation_score(self):
        result = self.settle(
            "no-match",
            opponent_id=None,
            outcome="no_match",
            opponent_arena=None,
            opponent_player=None,
            final_challenger=(50, 40),
            final_opponent=(None, None),
        )

        self.assertEqual(("applied", 1500, "白银", None), (
            result.status,
            result.challenger_score,
            result.challenger_rank,
            result.opponent_score,
        ))
        state = self.state()
        self.assertEqual((1500, 2, 2, 0, 2, "白银", 3, 1, "new"), state[0])
        self.assertEqual(tuple(self.opponent_arena.values()), state[1])
        self.assertEqual(("user", 50, 40, 5), state[2][1])

    def test_operation_insert_failure_rolls_back_every_state_change(self):
        with db_backend.transaction(self.game) as conn:
            conn.execute(
                "CREATE TABLE arena_challenge_settlement_operations("
                "operation_id TEXT PRIMARY KEY,challenger_id TEXT,payload TEXT,"
                "result_json TEXT,created_at TIMESTAMP)"
            )
            conn.execute(
                "CREATE TRIGGER fail_challenge BEFORE INSERT ON "
                "arena_challenge_settlement_operations BEGIN SELECT RAISE(ABORT,'failed'); END"
            )

        with self.assertRaises(db_backend.IntegrityError):
            self.settle("rollback")

        self.assertEqual(self.pristine_state(), self.state())

    def test_production_handler_uses_only_end_to_end_operation(self):
        source_path = (
            Path(__file__).resolve().parents[1]
            / "nonebot_plugin_xiuxian_2"
            / "xiuxian"
            / "xiuxian_arena"
            / "__init__.py"
        )
        source = source_path.read_text(encoding="utf-8")
        start = source.index("@arena_challenge.handle")
        handler = source[start:source.index("@arena_view.handle", start)]
        self.assertIn("arena_challenge_settlement_service.settle(", handler)
        self.assertIn("arena_challenge_settlement_service.get_result(", handler)
        self.assertNotIn("arena_challenge_cost_service.consume(", handler)
        self.assertNotIn("arena_battle_settlement_service.settle(", handler)
        self.assertLess(handler.index(".settle("), handler.index("send_msg_handler("))

        helper = source[source.index("def _arena_fight"):start]
        self.assertIn("random.seed(operation_id)", helper)
        opponent = source[source.index("async def find_arena_opponent"):]
        self.assertIn("random.Random(operation_id).choice", opponent)


if __name__ == "__main__":
    unittest.main()
