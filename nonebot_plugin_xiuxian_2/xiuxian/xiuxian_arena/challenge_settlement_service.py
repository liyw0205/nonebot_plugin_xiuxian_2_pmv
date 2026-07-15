from __future__ import annotations

import json
from contextlib import closing
from dataclasses import asdict, dataclass
from pathlib import Path
from threading import RLock

from ..xiuxian_utils import db_backend


@dataclass(frozen=True)
class ArenaChallengeSettlementResult:
    status: str
    outcome: str = ""
    challenger_score: int = 0
    challenger_rank: str = ""
    opponent_score: int | None = None
    score_delta: int = 0
    used: int = 0
    remaining: int = 0
    stamina: int = 0
    challenged_at: str = ""

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


class ArenaChallengeSettlementService:
    """Commit arena challenge cost and battle outcome in one transaction."""

    _BATTLE_FIELDS = (
        "score",
        "total_wins",
        "total_losses",
        "win_streak",
        "max_win_streak",
        "rank",
    )
    _CHALLENGER_FIELDS = _BATTLE_FIELDS + (
        "daily_challenges_used",
        "daily_extra_challenges",
        "last_challenge_time",
    )

    def __init__(
        self,
        game_database: str | Path,
        player_database: str | Path,
        lock: RLock | None = None,
    ) -> None:
        self._game_database = Path(game_database)
        self._player_database = Path(player_database)
        self._lock = lock or RLock()

    @staticmethod
    def _ensure_schema(conn) -> None:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS arena_challenge_settlement_operations("
            "operation_id TEXT PRIMARY KEY,challenger_id TEXT NOT NULL,payload TEXT NOT NULL,"
            "result_json TEXT NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
        )

    @classmethod
    def _arena_snapshot(cls, value, challenger: bool) -> dict:
        data = dict(value)
        fields = cls._CHALLENGER_FIELDS if challenger else cls._BATTLE_FIELDS
        result = {}
        for field in fields:
            if field in {"rank", "last_challenge_time"}:
                result[field] = str(data.get(field) or "")
            else:
                result[field] = int(data.get(field, 0) or 0)
        return result

    @staticmethod
    def _player_snapshot(value, challenger: bool) -> dict:
        data = dict(value)
        result = {"hp": int(data["hp"]), "mp": int(data["mp"])}
        if challenger:
            result["user_stamina"] = int(data.get("user_stamina", 0) or 0)
        return result

    @staticmethod
    def _rank(score: int) -> str:
        if score >= 3200:
            return "王者"
        if score >= 2700:
            return "钻石"
        if score >= 2300:
            return "铂金"
        if score >= 1900:
            return "黄金"
        if score >= 1500:
            return "白银"
        return "青铜"

    @staticmethod
    def _encode_result(result: ArenaChallengeSettlementResult) -> str:
        return json.dumps(
            asdict(result), ensure_ascii=True, sort_keys=True, separators=(",", ":")
        )

    @staticmethod
    def _decode_result(status: str, raw: str) -> ArenaChallengeSettlementResult:
        data = json.loads(raw)
        data["status"] = status
        return ArenaChallengeSettlementResult(**data)

    def get_result(
        self, operation_id, challenger_id=None
    ) -> ArenaChallengeSettlementResult | None:
        operation_id = str(operation_id).strip()
        if not operation_id:
            raise ValueError("operation_id is required")
        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            self._ensure_schema(conn)
            conn.commit()
            row = conn.execute(
                "SELECT challenger_id,result_json FROM arena_challenge_settlement_operations "
                "WHERE operation_id=%s",
                (operation_id,),
            ).fetchone()
            if row is None:
                return None
            if challenger_id is not None and str(row[0]) != str(challenger_id):
                return ArenaChallengeSettlementResult("operation_conflict")
            return self._decode_result("duplicate", str(row[1]))

    @classmethod
    def _normalize_arena_row(cls, row, challenger: bool) -> tuple:
        fields = cls._CHALLENGER_FIELDS if challenger else cls._BATTLE_FIELDS
        values = []
        for index, field in enumerate(fields):
            value = row[index]
            if field in {"rank", "last_challenge_time"}:
                values.append(str(value or ""))
            else:
                values.append(int(value or 0))
        return tuple(values)

    def settle(
        self,
        operation_id,
        challenger_id,
        opponent_id,
        outcome,
        challenge_cap,
        stamina_cost,
        challenged_at,
        expected_challenger_arena,
        expected_opponent_arena,
        expected_challenger_player,
        expected_opponent_player,
        final_challenger_hp,
        final_challenger_mp,
        final_opponent_hp,
        final_opponent_mp,
        win_points,
        lose_points,
        no_match_points,
    ) -> ArenaChallengeSettlementResult:
        operation_id = str(operation_id).strip()
        challenger_id = str(challenger_id)
        opponent_id = "" if opponent_id is None else str(opponent_id)
        outcome = str(outcome)
        challenge_cap, stamina_cost = int(challenge_cap), int(stamina_cost)
        challenged_at = str(challenged_at)
        challenger = self._arena_snapshot(expected_challenger_arena, True)
        opponent = (
            None
            if expected_opponent_arena is None
            else self._arena_snapshot(expected_opponent_arena, False)
        )
        challenger_player = self._player_snapshot(expected_challenger_player, True)
        opponent_player = (
            None
            if expected_opponent_player is None
            else self._player_snapshot(expected_opponent_player, False)
        )
        final_challenger_hp = max(1, int(final_challenger_hp))
        final_challenger_mp = max(1, int(final_challenger_mp))
        final_opponent_hp = (
            None if final_opponent_hp is None else max(1, int(final_opponent_hp))
        )
        final_opponent_mp = (
            None if final_opponent_mp is None else max(1, int(final_opponent_mp))
        )
        win_points, lose_points, no_match_points = map(
            int, (win_points, lose_points, no_match_points)
        )
        if (
            not operation_id
            or not challenger_id
            or not challenged_at
            or outcome not in {"win", "loss", "draw", "no_match"}
            or min(
                challenge_cap,
                stamina_cost,
                win_points,
                lose_points,
                no_match_points,
            )
            < 0
        ):
            raise ValueError("valid arena challenge settlement is required")
        if outcome != "no_match" and (
            not opponent_id or opponent is None or opponent_player is None
        ):
            raise ValueError("battle opponent snapshots are required")
        if opponent_id and opponent_id == challenger_id:
            raise ValueError("challenger and opponent must differ")

        payload = json.dumps(
            [
                challenger_id,
                opponent_id,
                outcome,
                challenge_cap,
                stamina_cost,
                challenged_at,
                challenger,
                opponent,
                challenger_player,
                opponent_player,
                final_challenger_hp,
                final_challenger_mp,
                final_opponent_hp,
                final_opponent_mp,
                win_points,
                lose_points,
                no_match_points,
            ],
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
        )

        def current_result(status: str) -> ArenaChallengeSettlementResult:
            used = challenger["daily_challenges_used"]
            return ArenaChallengeSettlementResult(
                status=status,
                outcome=outcome,
                challenger_score=challenger["score"],
                challenger_rank=challenger["rank"],
                opponent_score=None if opponent is None else opponent["score"],
                score_delta=(
                    win_points
                    if outcome == "win"
                    else no_match_points if outcome == "no_match" else 0
                ),
                used=used,
                remaining=max(0, challenge_cap - used),
                stamina=challenger_player["user_stamina"],
                challenged_at=challenger["last_challenge_time"],
            )

        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            attached = False
            try:
                conn.execute(
                    "ATTACH DATABASE %s AS player_data", (str(self._player_database),)
                )
                attached = True
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_schema(conn)
                previous = conn.execute(
                    "SELECT payload,result_json FROM arena_challenge_settlement_operations "
                    "WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    if str(previous[0]) != payload:
                        return current_result("operation_conflict")
                    return self._decode_result("duplicate", str(previous[1]))

                columns = {
                    str(row[1])
                    for row in conn.execute(
                        "PRAGMA player_data.table_info(arena)"
                    ).fetchall()
                }
                if not set(self._CHALLENGER_FIELDS).issubset(columns):
                    conn.rollback()
                    return current_result("state_changed")

                challenger_arena_row = conn.execute(
                    "SELECT score,total_wins,total_losses,win_streak,max_win_streak,rank,"
                    "daily_challenges_used,daily_extra_challenges,COALESCE(last_challenge_time,'') "
                    "FROM player_data.arena WHERE user_id=%s",
                    (challenger_id,),
                ).fetchone()
                challenger_player_row = conn.execute(
                    "SELECT COALESCE(hp,0),COALESCE(mp,0),COALESCE(user_stamina,0) "
                    "FROM user_xiuxian WHERE user_id=%s",
                    (challenger_id,),
                ).fetchone()
                if (
                    challenger_arena_row is None
                    or challenger_player_row is None
                    or self._normalize_arena_row(challenger_arena_row, True)
                    != tuple(challenger[field] for field in self._CHALLENGER_FIELDS)
                    or tuple(map(int, challenger_player_row))
                    != (
                        challenger_player["hp"],
                        challenger_player["mp"],
                        challenger_player["user_stamina"],
                    )
                ):
                    conn.rollback()
                    return current_result("state_changed")

                if opponent is not None:
                    opponent_arena_row = conn.execute(
                        "SELECT score,total_wins,total_losses,win_streak,max_win_streak,rank "
                        "FROM player_data.arena WHERE user_id=%s",
                        (opponent_id,),
                    ).fetchone()
                    opponent_player_row = conn.execute(
                        "SELECT COALESCE(hp,0),COALESCE(mp,0) FROM user_xiuxian "
                        "WHERE user_id=%s",
                        (opponent_id,),
                    ).fetchone()
                    if (
                        opponent_arena_row is None
                        or opponent_player_row is None
                        or self._normalize_arena_row(opponent_arena_row, False)
                        != tuple(opponent[field] for field in self._BATTLE_FIELDS)
                        or tuple(map(int, opponent_player_row))
                        != (opponent_player["hp"], opponent_player["mp"])
                    ):
                        conn.rollback()
                        return current_result("state_changed")

                used = challenger["daily_challenges_used"]
                if used >= challenge_cap:
                    conn.rollback()
                    return current_result("limit_reached")
                if challenger_player["user_stamina"] < stamina_cost:
                    conn.rollback()
                    return current_result("stamina_insufficient")

                challenger_new = dict(challenger)
                opponent_new = None if opponent is None else dict(opponent)
                if outcome == "win":
                    challenger_new["score"] += win_points
                    challenger_new["total_wins"] += 1
                    challenger_new["win_streak"] += 1
                    challenger_new["max_win_streak"] = max(
                        challenger_new["max_win_streak"], challenger_new["win_streak"]
                    )
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
                challenger_new["rank"] = self._rank(challenger_new["score"])
                challenger_new["daily_challenges_used"] = used + 1
                challenger_new["last_challenge_time"] = challenged_at
                if opponent_new is not None:
                    opponent_new["rank"] = self._rank(opponent_new["score"])

                if conn.execute(
                    "UPDATE player_data.arena SET score=%s,total_wins=%s,total_losses=%s,"
                    "win_streak=%s,max_win_streak=%s,rank=%s,daily_challenges_used=%s,"
                    "daily_extra_challenges=%s,last_challenge_time=%s WHERE user_id=%s",
                    tuple(
                        challenger_new[field] for field in self._CHALLENGER_FIELDS
                    )
                    + (challenger_id,),
                ).rowcount != 1:
                    raise RuntimeError("challenger arena state changed")
                if opponent_new is not None and conn.execute(
                    "UPDATE player_data.arena SET score=%s,total_wins=%s,total_losses=%s,"
                    "win_streak=%s,max_win_streak=%s,rank=%s WHERE user_id=%s",
                    tuple(opponent_new[field] for field in self._BATTLE_FIELDS)
                    + (opponent_id,),
                ).rowcount != 1:
                    raise RuntimeError("opponent arena state changed")

                stamina = challenger_player["user_stamina"] - stamina_cost
                if conn.execute(
                    "UPDATE user_xiuxian SET hp=%s,mp=%s,user_stamina=%s WHERE user_id=%s "
                    "AND COALESCE(hp,0)=%s AND COALESCE(mp,0)=%s "
                    "AND COALESCE(user_stamina,0)=%s",
                    (
                        final_challenger_hp,
                        final_challenger_mp,
                        stamina,
                        challenger_id,
                        challenger_player["hp"],
                        challenger_player["mp"],
                        challenger_player["user_stamina"],
                    ),
                ).rowcount != 1:
                    raise RuntimeError("challenger player state changed")
                if opponent_player is not None and conn.execute(
                    "UPDATE user_xiuxian SET hp=%s,mp=%s WHERE user_id=%s "
                    "AND COALESCE(hp,0)=%s AND COALESCE(mp,0)=%s",
                    (
                        final_opponent_hp,
                        final_opponent_mp,
                        opponent_id,
                        opponent_player["hp"],
                        opponent_player["mp"],
                    ),
                ).rowcount != 1:
                    raise RuntimeError("opponent player state changed")

                result = ArenaChallengeSettlementResult(
                    status="applied",
                    outcome=outcome,
                    challenger_score=challenger_new["score"],
                    challenger_rank=challenger_new["rank"],
                    opponent_score=(
                        None if opponent_new is None else opponent_new["score"]
                    ),
                    score_delta=(
                        win_points
                        if outcome == "win"
                        else no_match_points if outcome == "no_match" else 0
                    ),
                    used=challenger_new["daily_challenges_used"],
                    remaining=max(
                        0, challenge_cap - challenger_new["daily_challenges_used"]
                    ),
                    stamina=stamina,
                    challenged_at=challenged_at,
                )
                conn.execute(
                    "INSERT INTO arena_challenge_settlement_operations("
                    "operation_id,challenger_id,payload,result_json) VALUES(%s,%s,%s,%s)",
                    (
                        operation_id,
                        challenger_id,
                        payload,
                        self._encode_result(result),
                    ),
                )
                conn.commit()
                return result
            except Exception:
                conn.rollback()
                raise
            finally:
                if attached:
                    conn.execute("DETACH DATABASE player_data")


__all__ = ["ArenaChallengeSettlementResult", "ArenaChallengeSettlementService"]
