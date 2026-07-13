from __future__ import annotations

import json
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from threading import RLock

from ..xiuxian_utils import db_backend


@dataclass(frozen=True)
class ArenaBattleSettlementResult:
    status: str
    challenger_score: int
    challenger_rank: str
    opponent_score: int | None

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


class ArenaBattleSettlementService:
    """Commit arena score, records and battle vitals as one operation."""

    _ARENA_FIELDS = ("score", "total_wins", "total_losses", "win_streak", "max_win_streak", "rank")

    def __init__(self, game_database: str | Path, player_database: str | Path, lock: RLock | None = None) -> None:
        self._game_database = Path(game_database)
        self._player_database = Path(player_database)
        self._lock = lock or RLock()

    @staticmethod
    def _arena_snapshot(value):
        data = dict(value)
        return {
            "score": int(data["score"]), "total_wins": int(data["total_wins"]),
            "total_losses": int(data["total_losses"]), "win_streak": int(data["win_streak"]),
            "max_win_streak": int(data["max_win_streak"]), "rank": str(data["rank"]),
        }

    @staticmethod
    def _player_snapshot(value):
        data = dict(value)
        return {"hp": int(data["hp"]), "mp": int(data["mp"])}

    def settle(
        self, operation_id, challenger_id, opponent_id, outcome,
        expected_challenger_arena, expected_opponent_arena,
        expected_challenger_player, expected_opponent_player,
        final_challenger_hp, final_challenger_mp, final_opponent_hp, final_opponent_mp,
        win_points, lose_points, no_match_points,
    ) -> ArenaBattleSettlementResult:
        operation_id, challenger_id = str(operation_id).strip(), str(challenger_id)
        opponent_id = "" if opponent_id is None else str(opponent_id)
        outcome = str(outcome)
        challenger = self._arena_snapshot(expected_challenger_arena)
        opponent = None if expected_opponent_arena is None else self._arena_snapshot(expected_opponent_arena)
        challenger_player = self._player_snapshot(expected_challenger_player)
        opponent_player = None if expected_opponent_player is None else self._player_snapshot(expected_opponent_player)
        final_challenger_hp, final_challenger_mp = max(1, int(final_challenger_hp)), max(1, int(final_challenger_mp))
        final_opponent_hp = None if final_opponent_hp is None else max(1, int(final_opponent_hp))
        final_opponent_mp = None if final_opponent_mp is None else max(1, int(final_opponent_mp))
        win_points, lose_points, no_match_points = map(int, (win_points, lose_points, no_match_points))
        if not operation_id or outcome not in {"win", "loss", "draw", "no_match"} or min(win_points, lose_points, no_match_points) < 0:
            raise ValueError("valid operation, outcome and score rules are required")
        if outcome != "no_match" and (not opponent_id or opponent is None or opponent_player is None):
            raise ValueError("battle opponent snapshots are required")
        payload = json.dumps([
            challenger_id, opponent_id, outcome, challenger, opponent, challenger_player, opponent_player,
            final_challenger_hp, final_challenger_mp, final_opponent_hp, final_opponent_mp,
            win_points, lose_points, no_match_points,
        ], ensure_ascii=True, sort_keys=True)

        def rank(score):
            return "王者" if score >= 3200 else "钻石" if score >= 2700 else "铂金" if score >= 2300 else "黄金" if score >= 1900 else "白银" if score >= 1500 else "青铜"

        def result(status, challenger_score=challenger["score"], challenger_rank=challenger["rank"], opponent_score=None):
            return ArenaBattleSettlementResult(status, int(challenger_score), str(challenger_rank), None if opponent_score is None else int(opponent_score))

        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            attached = False
            try:
                conn.execute("ATTACH DATABASE %s AS player_data", (str(self._player_database),))
                attached = True
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS arena_battle_settlement_operations ("
                    "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,challenger_score INTEGER NOT NULL,"
                    "challenger_rank TEXT NOT NULL,opponent_score INTEGER,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
                )
                previous = conn.execute(
                    "SELECT payload,challenger_score,challenger_rank,opponent_score FROM arena_battle_settlement_operations WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    return result("state_changed") if str(previous[0]) != payload else result("duplicate", previous[1], previous[2], previous[3])
                columns = {str(row[1]) for row in conn.execute("PRAGMA player_data.table_info(arena)").fetchall()}
                if not set(self._ARENA_FIELDS).issubset(columns):
                    conn.rollback(); return result("state_changed")

                ids = [challenger_id] + ([opponent_id] if opponent_id else [])
                arena_rows, player_rows = {}, {}
                for user_id in ids:
                    arena_rows[user_id] = conn.execute(
                        "SELECT score,total_wins,total_losses,win_streak,max_win_streak,rank FROM player_data.arena WHERE user_id=%s",
                        (user_id,),
                    ).fetchone()
                    player_rows[user_id] = conn.execute(
                        "SELECT COALESCE(hp,0),COALESCE(mp,0) FROM user_xiuxian WHERE user_id=%s", (user_id,)
                    ).fetchone()
                expected_arena = {challenger_id: challenger, **({opponent_id: opponent} if opponent_id else {})}
                expected_players = {challenger_id: challenger_player, **({opponent_id: opponent_player} if opponent_id else {})}
                for user_id in ids:
                    row, player_row = arena_rows[user_id], player_rows[user_id]
                    expected = expected_arena[user_id]
                    if row is None or player_row is None or (
                        int(row[0]), int(row[1]), int(row[2]), int(row[3]), int(row[4]), str(row[5])
                    ) != tuple(expected[field] for field in self._ARENA_FIELDS) or tuple(map(int, player_row)) != (
                        expected_players[user_id]["hp"], expected_players[user_id]["mp"]
                    ):
                        conn.rollback(); return result("state_changed")

                challenger_new = dict(challenger)
                opponent_new = None if opponent is None else dict(opponent)
                if outcome == "win":
                    challenger_new["score"] += win_points
                    challenger_new["total_wins"] += 1
                    challenger_new["win_streak"] += 1
                    challenger_new["max_win_streak"] = max(challenger_new["max_win_streak"], challenger_new["win_streak"])
                    opponent_new["score"] = max(0, opponent_new["score"] - lose_points)
                    opponent_new["total_losses"] += 1
                    opponent_new["win_streak"] = 0
                elif outcome in {"loss", "draw"}:
                    challenger_new["total_losses"] += 1
                    challenger_new["win_streak"] = 0
                else:
                    challenger_new["score"] += no_match_points
                    challenger_new["total_losses"] += 1
                    challenger_new["win_streak"] = 0
                challenger_new["rank"] = rank(challenger_new["score"])
                if opponent_new is not None:
                    opponent_new["rank"] = rank(opponent_new["score"])

                def update_arena(user_id, data):
                    return conn.execute(
                        "UPDATE player_data.arena SET score=%s,total_wins=%s,total_losses=%s,win_streak=%s,max_win_streak=%s,rank=%s WHERE user_id=%s",
                        tuple(data[field] for field in self._ARENA_FIELDS) + (user_id,),
                    ).rowcount

                if update_arena(challenger_id, challenger_new) != 1:
                    conn.rollback(); return result("state_changed")
                if opponent_new is not None and update_arena(opponent_id, opponent_new) != 1:
                    conn.rollback(); return result("state_changed")
                if conn.execute(
                    "UPDATE user_xiuxian SET hp=%s,mp=%s WHERE user_id=%s AND COALESCE(hp,0)=%s AND COALESCE(mp,0)=%s",
                    (final_challenger_hp, final_challenger_mp, challenger_id, challenger_player["hp"], challenger_player["mp"]),
                ).rowcount != 1:
                    conn.rollback(); return result("state_changed")
                if opponent_player is not None and conn.execute(
                    "UPDATE user_xiuxian SET hp=%s,mp=%s WHERE user_id=%s AND COALESCE(hp,0)=%s AND COALESCE(mp,0)=%s",
                    (final_opponent_hp, final_opponent_mp, opponent_id, opponent_player["hp"], opponent_player["mp"]),
                ).rowcount != 1:
                    conn.rollback(); return result("state_changed")
                opponent_score = opponent_new["score"] if opponent_new is not None else None
                conn.execute(
                    "INSERT INTO arena_battle_settlement_operations "
                    "(operation_id,payload,challenger_score,challenger_rank,opponent_score) VALUES (%s,%s,%s,%s,%s)",
                    (operation_id, payload, challenger_new["score"], challenger_new["rank"], opponent_score),
                )
                conn.commit()
                return result("applied", challenger_new["score"], challenger_new["rank"], opponent_score)
            except Exception:
                conn.rollback()
                raise
            finally:
                if attached:
                    conn.execute("DETACH DATABASE player_data")


__all__ = ["ArenaBattleSettlementResult", "ArenaBattleSettlementService"]
