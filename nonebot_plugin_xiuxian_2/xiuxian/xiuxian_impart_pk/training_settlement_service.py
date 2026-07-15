from __future__ import annotations

import json
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from threading import RLock

from ..xiuxian_utils import db_backend

SQLITE_MAX_INT = 2**63 - 1

from .settlement_state import increment_stat, load_daily_state


@dataclass(frozen=True)
class ImpartTrainingSettlementResult:
    status: str
    exp_day: int
    exp: int
    exp_used: int
    exp_count: int
    exp_load: int
    exp_gain: int

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


class ImpartTrainingSettlementService:
    """Settle one virtual-world cultivation action across all authoritative stores."""

    def __init__(self, game_database, impart_database, player_database, lock=None):
        self._game_database = Path(game_database)
        self._impart_database = Path(impart_database)
        self._player_database = Path(player_database)
        self._lock = lock or RLock()

    def get_daily_state(self, user_id, legacy_state=None) -> dict[str, int]:
        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            conn.execute("ATTACH DATABASE %s AS player_data", (str(self._player_database),))
            try:
                conn.execute("BEGIN IMMEDIATE")
                state = load_daily_state(conn, str(user_id), legacy_state)
                conn.commit()
                return state
            except Exception:
                conn.rollback()
                raise
            finally:
                conn.execute("DETACH DATABASE player_data")

    def reset_daily(self) -> None:
        with self._lock, closing(db_backend.connect(self._player_database)) as conn:
            conn.execute("DELETE FROM impart_pk_daily") if conn.table_exists("impart_pk_daily") else None
            conn.commit()

    def get_result(self, operation_id: str) -> ImpartTrainingSettlementResult | None:
        operation_id = str(operation_id).strip()
        if not operation_id:
            return None
        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            conn.execute(
                "CREATE TABLE IF NOT EXISTS impart_training_operations ("
                "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,result_json TEXT NOT NULL,"
                "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
            )
            previous = conn.execute(
                "SELECT payload,result_json FROM impart_training_operations WHERE operation_id=%s", (operation_id,),
            ).fetchone()
            if previous is None:
                return None
            return ImpartTrainingSettlementResult("duplicate", *json.loads(str(previous[1])))

    def settle(
        self, operation_id, user_id, *, expected_exp, expected_exp_day, expected_daily,
        exp_cost, exp_gain, exp_load_gain, power, legacy_state=None,
    ) -> ImpartTrainingSettlementResult:
        operation_id, user_id = str(operation_id).strip(), str(user_id)
        expected_daily = {key: int(expected_daily[key]) for key in ("exp_used", "exp_count", "exp_load", "exp_gain")}
        expected_exp, expected_exp_day, exp_cost, exp_gain, exp_load_gain, power = map(
            int, (expected_exp, expected_exp_day, exp_cost, exp_gain, exp_load_gain, power)
        )
        power = max(0, min(power, SQLITE_MAX_INT))
        expected_exp = max(0, min(expected_exp, SQLITE_MAX_INT))
        exp_gain = max(0, min(exp_gain, SQLITE_MAX_INT))
        if not operation_id or exp_cost <= 0 or exp_gain <= 0 or exp_load_gain < 0 or power < 0:
            raise ValueError("invalid impart training settlement")
        # Request identity only — exp/daily snapshots are concurrency checks; roll outcome in result_json.
        payload = json.dumps([user_id, exp_cost], ensure_ascii=True, separators=(",", ":"))
        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            attached_impart = attached_player = False
            try:
                conn.execute("ATTACH DATABASE %s AS impart_data", (str(self._impart_database),)); attached_impart = True
                conn.execute("ATTACH DATABASE %s AS player_data", (str(self._player_database),)); attached_player = True
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS impart_training_operations ("
                    "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,result_json TEXT NOT NULL,"
                    "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
                )
                previous = conn.execute(
                    "SELECT payload,result_json FROM impart_training_operations WHERE operation_id=%s", (operation_id,)
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    if str(previous[0]) != payload:
                        return ImpartTrainingSettlementResult("operation_conflict", 0, 0, 0, 0, 0, 0)
                    return ImpartTrainingSettlementResult("duplicate", *json.loads(str(previous[1])))

                user = conn.execute("SELECT exp FROM user_xiuxian WHERE user_id=%s", (user_id,)).fetchone()
                impart = conn.execute("SELECT exp_day FROM impart_data.xiuxian_impart WHERE user_id=%s", (user_id,)).fetchone()
                daily = load_daily_state(conn, user_id, legacy_state)
                if user is None or impart is None or int(user[0] or 0) != expected_exp or int(impart[0] or 0) != expected_exp_day:
                    conn.rollback(); return ImpartTrainingSettlementResult("state_changed", 0, 0, 0, 0, 0, 0)
                if any(daily[key] != value for key, value in expected_daily.items()):
                    conn.rollback(); return ImpartTrainingSettlementResult("state_changed", 0, 0, 0, 0, 0, 0)
                if expected_exp_day < exp_cost:
                    conn.rollback(); return ImpartTrainingSettlementResult("time_insufficient", 0, 0, 0, 0, 0, 0)

                new_exp_day = expected_exp_day - exp_cost
                new_exp = min(SQLITE_MAX_INT, expected_exp + exp_gain)
                new_used = expected_daily["exp_used"] + exp_cost
                new_count = expected_daily["exp_count"] + 1
                new_load = min(100, expected_daily["exp_load"] + exp_load_gain)
                new_gain = expected_daily["exp_gain"] + exp_gain
                conn.execute("UPDATE impart_data.xiuxian_impart SET exp_day=%s WHERE user_id=%s", (new_exp_day, user_id))
                changed = conn.execute(
                    "UPDATE user_xiuxian SET exp=%s,power=%s WHERE user_id=%s AND exp=%s",
                    (new_exp, power, user_id, expected_exp),
                )
                if changed.rowcount != 1:
                    conn.rollback(); return ImpartTrainingSettlementResult("state_changed", 0, 0, 0, 0, 0, 0)
                conn.execute(
                    "UPDATE player_data.impart_pk_daily SET exp_used=%s,exp_count=%s,exp_load=%s,exp_gain=%s WHERE user_id=%s",
                    (new_used, new_count, new_load, new_gain, user_id),
                )
                for key, amount in (
                    ("虚神界修炼", exp_cost), ("虚神界修炼次数", 1),
                    ("虚神界修炼修为", exp_gain), ("虚神界修炼承载", exp_load_gain),
                ):
                    increment_stat(conn, user_id, key, amount)
                saved = [new_exp_day, new_exp, new_used, new_count, new_load, new_gain]
                conn.execute(
                    "INSERT INTO impart_training_operations(operation_id,payload,result_json) VALUES(%s,%s,%s)",
                    (operation_id, payload, json.dumps(saved, separators=(",", ":"))),
                )
                conn.commit()
                return ImpartTrainingSettlementResult("applied", *saved)
            except Exception:
                conn.rollback()
                raise
            finally:
                if attached_player:
                    try: conn.execute("DETACH DATABASE player_data")
                    except Exception: pass
                if attached_impart:
                    try: conn.execute("DETACH DATABASE impart_data")
                    except Exception: pass


__all__ = ["ImpartTrainingSettlementResult", "ImpartTrainingSettlementService"]
