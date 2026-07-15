from __future__ import annotations

import json
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from threading import RLock

from ..xiuxian_utils import db_backend
from .settlement_state import increment_stat, load_daily_state


@dataclass(frozen=True)
class ImpartExploreSettlementResult:
    status: str
    exp_day: int
    impart_lv: int
    impart_num: int

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


class ImpartExploreSettlementService:
    def __init__(self, game_database, impart_database, player_database, lock=None):
        self._game_database = Path(game_database)
        self._impart_database = Path(impart_database)
        self._player_database = Path(player_database)
        self._lock = lock or RLock()

    def get_result(self, operation_id: str) -> ImpartExploreSettlementResult | None:
        operation_id = str(operation_id).strip()
        if not operation_id:
            return None
        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            conn.execute(
                "CREATE TABLE IF NOT EXISTS impart_explore_operations ("
                "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,result_json TEXT NOT NULL,"
                "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
            )
            previous = conn.execute(
                "SELECT payload,result_json FROM impart_explore_operations WHERE operation_id=%s", (operation_id,),
            ).fetchone()
            if previous is None:
                return None
            return ImpartExploreSettlementResult("duplicate", *json.loads(str(previous[1])))

    def settle(
        self, operation_id, user_id, *, event_type, expected_exp_day, expected_impart_lv,
        expected_impart_num, time_cost, new_impart_lv, legacy_state=None,
    ) -> ImpartExploreSettlementResult:
        operation_id, user_id, event_type = str(operation_id).strip(), str(user_id), str(event_type)
        expected_exp_day, expected_impart_lv, expected_impart_num, time_cost, new_impart_lv = map(
            int, (expected_exp_day, expected_impart_lv, expected_impart_num, time_cost, new_impart_lv)
        )
        if not operation_id or event_type not in {"stay", "fail", "down", "up", "down_rate", "up_rate"}:
            raise ValueError("invalid impart exploration settlement")
        if expected_impart_num <= 0 or time_cost < 0 or not 0 <= new_impart_lv <= 30:
            raise ValueError("invalid impart exploration values")
        # Request identity only — event_type/time/lv rolls live in result_json.
        payload = json.dumps([user_id], ensure_ascii=True, separators=(",", ":"))
        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            attached_impart = attached_player = False
            try:
                conn.execute("ATTACH DATABASE %s AS impart_data", (str(self._impart_database),)); attached_impart = True
                conn.execute("ATTACH DATABASE %s AS player_data", (str(self._player_database),)); attached_player = True
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS impart_explore_operations ("
                    "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,result_json TEXT NOT NULL,"
                    "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
                )
                previous = conn.execute(
                    "SELECT payload,result_json FROM impart_explore_operations WHERE operation_id=%s", (operation_id,)
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    if str(previous[0]) != payload:
                        return ImpartExploreSettlementResult("operation_conflict", 0, 0, 0)
                    return ImpartExploreSettlementResult("duplicate", *json.loads(str(previous[1])))

                impart = conn.execute(
                    "SELECT exp_day,impart_lv FROM impart_data.xiuxian_impart WHERE user_id=%s", (user_id,)
                ).fetchone()
                daily = load_daily_state(conn, user_id, legacy_state)
                if impart is None or (int(impart[0] or 0), int(impart[1] or 0)) != (expected_exp_day, expected_impart_lv):
                    conn.rollback(); return ImpartExploreSettlementResult("state_changed", 0, 0, 0)
                if daily["impart_num"] != expected_impart_num:
                    conn.rollback(); return ImpartExploreSettlementResult("state_changed", 0, 0, 0)
                if expected_exp_day < time_cost:
                    conn.rollback(); return ImpartExploreSettlementResult("time_insufficient", 0, 0, 0)

                new_exp_day = expected_exp_day - time_cost
                new_impart_num = expected_impart_num - 1
                conn.execute(
                    "UPDATE impart_data.xiuxian_impart SET exp_day=%s,impart_lv=%s WHERE user_id=%s",
                    (new_exp_day, new_impart_lv, user_id),
                )
                conn.execute(
                    "UPDATE player_data.impart_pk_daily SET impart_num=%s WHERE user_id=%s",
                    (new_impart_num, user_id),
                )
                increment_stat(conn, user_id, "虚神界探索次数", 1)
                if time_cost:
                    increment_stat(conn, user_id, "虚神界探索消耗时间", time_cost)
                if event_type in {"up", "up_rate"}:
                    increment_stat(conn, user_id, "虚神界探索上升", 1)
                elif event_type in {"down", "down_rate"}:
                    increment_stat(conn, user_id, "虚神界探索下降", 1)
                saved = [new_exp_day, new_impart_lv, new_impart_num]
                conn.execute(
                    "INSERT INTO impart_explore_operations(operation_id,payload,result_json) VALUES(%s,%s,%s)",
                    (operation_id, payload, json.dumps(saved, separators=(",", ":"))),
                )
                conn.commit()
                return ImpartExploreSettlementResult("applied", *saved)
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


__all__ = ["ImpartExploreSettlementResult", "ImpartExploreSettlementService"]
