from __future__ import annotations

import json
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from threading import RLock

from ..xiuxian_utils import db_backend
from .relation_transaction_utils import increment_stat


@dataclass(frozen=True)
class StoneTrainingResult:
    status: str
    exp_gain: int = 0
    stone_cost: int = 0
    power: int = 0

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


class StoneTrainingSettlementService:
    def __init__(self, game_database: str | Path, player_database: str | Path, lock: RLock | None = None) -> None:
        self._game_database = Path(game_database)
        self._player_database = Path(player_database)
        self._lock = lock or RLock()

    def settle(self, operation_id, user_id, *, requested_stone, expected_exp, expected_stone, exp_cap, power_multiplier) -> StoneTrainingResult:
        operation_id, user_id = str(operation_id).strip(), str(user_id)
        requested_stone, expected_exp, expected_stone, exp_cap = map(
            int, (requested_stone, expected_exp, expected_stone, exp_cap)
        )
        power_multiplier = float(power_multiplier)
        if not operation_id or requested_stone <= 0 or min(expected_exp, expected_stone, exp_cap) < 0 or power_multiplier < 0:
            raise ValueError("invalid stone training settlement arguments")
        possible_exp = requested_stone // 10
        exp_gain = min(possible_exp, max(0, exp_cap - expected_exp))
        stone_cost = exp_gain * 10 if possible_exp >= max(0, exp_cap - expected_exp) else requested_stone
        # Request identity only — exp/stone snapshots are concurrency checks.
        payload = json.dumps(
            [user_id, requested_stone, exp_cap, power_multiplier],
            separators=(",", ":"),
        )
        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            attached = False
            try:
                conn.execute("ATTACH DATABASE %s AS player_data", (str(self._player_database),))
                attached = True
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS stone_training_operations ("
                    "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,result_json TEXT NOT NULL,"
                    "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
                )
                previous = conn.execute(
                    "SELECT payload,result_json FROM stone_training_operations WHERE operation_id=%s", (operation_id,)
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    if str(previous[0]) != payload:
                        return StoneTrainingResult("operation_conflict")
                    saved = json.loads(str(previous[1]))
                    return StoneTrainingResult("duplicate", **saved)
                user = conn.execute(
                    "SELECT COALESCE(exp,0),COALESCE(stone,0) FROM user_xiuxian WHERE user_id=%s", (user_id,)
                ).fetchone()
                if user is None:
                    conn.rollback()
                    return StoneTrainingResult("user_missing")
                if (int(user[0]), int(user[1])) != (expected_exp, expected_stone):
                    conn.rollback()
                    return StoneTrainingResult("state_changed")
                if exp_gain <= 0:
                    conn.rollback()
                    return StoneTrainingResult("exp_capped")
                if expected_stone < stone_cost:
                    conn.rollback()
                    return StoneTrainingResult("stone_insufficient", exp_gain, stone_cost)
                changed = conn.execute(
                    "UPDATE user_xiuxian SET exp=exp+%s,stone=stone-%s,power=ROUND((exp+%s)*%s,0) "
                    "WHERE user_id=%s AND exp=%s AND stone=%s AND stone>=%s",
                    (exp_gain, stone_cost, exp_gain, power_multiplier, user_id, expected_exp, expected_stone, stone_cost),
                )
                if changed.rowcount != 1:
                    conn.rollback()
                    return StoneTrainingResult("state_changed")
                increment_stat(conn, user_id, "灵石修炼", stone_cost)
                increment_stat(conn, user_id, "灵石修炼修为", exp_gain)
                power = int(conn.execute("SELECT power FROM user_xiuxian WHERE user_id=%s", (user_id,)).fetchone()[0])
                saved = {"exp_gain": exp_gain, "stone_cost": stone_cost, "power": power}
                conn.execute(
                    "INSERT INTO stone_training_operations (operation_id,payload,result_json) VALUES (%s,%s,%s)",
                    (operation_id, payload, json.dumps(saved, separators=(",", ":"))),
                )
                conn.commit()
                return StoneTrainingResult("applied", **saved)
            except Exception:
                conn.rollback()
                raise
            finally:
                if attached:
                    try:
                        conn.execute("DETACH DATABASE player_data")
                    except Exception:
                        pass


__all__ = ["StoneTrainingResult", "StoneTrainingSettlementService"]
