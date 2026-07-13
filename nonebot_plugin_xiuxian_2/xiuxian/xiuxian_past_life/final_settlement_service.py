from __future__ import annotations

import json
from contextlib import closing
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from threading import RLock

from ..xiuxian_utils import db_backend


PAST_LIFE_FIELDS = (
    "state", "stage", "alloc", "accumulated", "talent", "total_score",
    "score_breakdown", "event_indices", "event_snapshots", "early_death_rolls",
    "history", "last_run_time", "total_runs", "best_ending", "best_score",
    "endings_log", "achievement_points",
)
JSON_FIELDS = {
    "alloc", "accumulated", "score_breakdown", "event_indices", "event_snapshots",
    "early_death_rolls", "history", "endings_log",
}


@dataclass(frozen=True)
class PastLifeFinalSettlementResult:
    status: str
    rewards: dict

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


def _canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _decode(field, value):
    if value is None:
        return None
    if field in JSON_FIELDS and isinstance(value, str):
        try:
            return json.loads(value)
        except (TypeError, ValueError):
            return value
    if field in {"state", "stage", "total_score", "total_runs", "best_score", "achievement_points"}:
        try:
            return int(value)
        except (TypeError, ValueError):
            return value
    return value


class PastLifeFinalSettlementService:
    def __init__(self, game_db, player_db, lock=None, max_goods_num=1000):
        self.game_db = Path(game_db)
        self.player_db = Path(player_db)
        self.lock = lock or RLock()
        self.max_goods_num = max(1, int(max_goods_num))

    def _ensure_schema(self, conn):
        conn.execute("CREATE TABLE IF NOT EXISTS player_data.past_life(user_id TEXT PRIMARY KEY)")
        columns = {
            str(row[1])
            for row in conn.execute("PRAGMA player_data.table_info(past_life)").fetchall()
        }
        for field in PAST_LIFE_FIELDS:
            if field not in columns:
                conn.execute(
                    f"ALTER TABLE player_data.past_life ADD COLUMN "
                    f"{db_backend.quote_ident(field)} TEXT DEFAULT NULL"
                )
        conn.execute(
            "CREATE TABLE IF NOT EXISTS past_life_final_operations("
            "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,result_json TEXT NOT NULL,"
            "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
        )

    def settle(
        self,
        operation_id,
        user_id,
        expected_state,
        final_state,
        ending_name,
        score,
        exp_reward,
        stone_reward,
        achievement_points,
        item_reward=None,
        completed_at=None,
    ) -> PastLifeFinalSettlementResult:
        operation_id = str(operation_id).strip()
        user_id = str(user_id)
        score = int(score)
        exp_reward = int(exp_reward)
        stone_reward = int(stone_reward)
        achievement_points = int(achievement_points)
        if not operation_id or min(score, exp_reward, stone_reward, achievement_points) < 0:
            raise ValueError("invalid past life final settlement")

        item = None
        if item_reward:
            item = {
                "id": int(item_reward["id"]),
                "name": str(item_reward["name"]),
                "type": str(item_reward["type"]),
                "num": max(0, int(item_reward.get("num", 1))),
            }
        completed_at = str(completed_at or datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        expected_snapshot = {field: expected_state.get(field) for field in PAST_LIFE_FIELDS}
        payload = _canonical({
            "user_id": user_id,
            "expected": expected_snapshot,
            "ending": str(ending_name),
            "score": score,
            "exp": exp_reward,
            "stone": stone_reward,
            "points": achievement_points,
            "item": item,
            "completed_at": completed_at,
        })

        with self.lock, closing(db_backend.connect(self.game_db)) as conn:
            try:
                conn.execute("ATTACH DATABASE %s AS player_data", (str(self.player_db),))
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_schema(conn)
                old = conn.execute(
                    "SELECT payload,result_json FROM past_life_final_operations WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if old:
                    conn.rollback()
                    if str(old[0]) != payload:
                        return PastLifeFinalSettlementResult("operation_conflict", {})
                    return PastLifeFinalSettlementResult("duplicate", json.loads(str(old[1])))

                user = conn.execute(
                    "SELECT COALESCE(exp,0),COALESCE(stone,0) FROM user_xiuxian WHERE user_id=%s",
                    (user_id,),
                ).fetchone()
                row = conn.execute(
                    "SELECT * FROM player_data.past_life WHERE user_id=%s", (user_id,)
                ).fetchone()
                if user is None or row is None:
                    conn.rollback()
                    return PastLifeFinalSettlementResult("user_missing", {})
                columns = [str(col[0]) for col in conn.execute(
                    "SELECT * FROM player_data.past_life WHERE user_id=%s", (user_id,)
                ).description]
                current = {columns[index]: value for index, value in enumerate(row)}
                for field in PAST_LIFE_FIELDS:
                    if _canonical(_decode(field, current.get(field))) != _canonical(expected_snapshot.get(field)):
                        conn.rollback()
                        return PastLifeFinalSettlementResult("state_changed", {})

                persisted = dict(final_state)
                previous_runs = int(expected_state.get("total_runs", 0) or 0)
                previous_best = int(expected_state.get("best_score", 0) or 0)
                previous_points = int(expected_state.get("achievement_points", 0) or 0)
                endings_log = list(expected_state.get("endings_log", []) or [])
                endings_log.append({
                    "run_number": previous_runs + 1,
                    "name": str(ending_name),
                    "score": score,
                    "time": completed_at,
                })
                persisted.update({
                    "state": 0,
                    "last_run_time": completed_at,
                    "total_runs": previous_runs + 1,
                    "best_score": max(previous_best, score),
                    "best_ending": str(ending_name) if score > previous_best else expected_state.get("best_ending", ""),
                    "endings_log": endings_log[-10:],
                    "achievement_points": previous_points + achievement_points,
                })

                conn.execute(
                    "UPDATE user_xiuxian SET exp=COALESCE(exp,0)+%s,stone=COALESCE(stone,0)+%s "
                    "WHERE user_id=%s",
                    (exp_reward, stone_reward, user_id),
                )
                if item and item["num"]:
                    bag = conn.execute(
                        "SELECT goods_num FROM back WHERE user_id=%s AND goods_id=%s",
                        (user_id, item["id"]),
                    ).fetchone()
                    if bag:
                        conn.execute(
                            "UPDATE back SET goods_name=%s,goods_type=%s,goods_num=MIN(COALESCE(goods_num,0)+%s,%s),"
                            "update_time=%s WHERE user_id=%s AND goods_id=%s",
                            (item["name"], item["type"], item["num"], self.max_goods_num,
                             completed_at, user_id, item["id"]),
                        )
                    else:
                        conn.execute(
                            "INSERT INTO back(user_id,goods_id,goods_name,goods_type,goods_num,create_time,update_time) "
                            "VALUES(%s,%s,%s,%s,%s,%s,%s)",
                            (user_id, item["id"], item["name"], item["type"],
                             min(item["num"], self.max_goods_num), completed_at, completed_at),
                        )

                assignments = ",".join(
                    f"{db_backend.quote_ident(field)}=%s" for field in PAST_LIFE_FIELDS
                )
                values = []
                for field in PAST_LIFE_FIELDS:
                    value = persisted.get(field)
                    values.append(_canonical(value) if field in JSON_FIELDS else value)
                conn.execute(
                    f"UPDATE player_data.past_life SET {assignments} WHERE user_id=%s",
                    (*values, user_id),
                )
                rewards = {
                    "exp": exp_reward, "stone": stone_reward, "points": achievement_points,
                    "item": item,
                }
                result_json = _canonical(rewards)
                conn.execute(
                    "INSERT INTO past_life_final_operations(operation_id,payload,result_json) VALUES(%s,%s,%s)",
                    (operation_id, payload, result_json),
                )
                conn.commit()
                return PastLifeFinalSettlementResult("applied", rewards)
            except Exception:
                conn.rollback()
                raise
            finally:
                try:
                    conn.execute("DETACH DATABASE player_data")
                except Exception:
                    pass
