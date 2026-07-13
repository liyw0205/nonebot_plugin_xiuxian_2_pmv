from __future__ import annotations

import json
from contextlib import closing
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from threading import RLock

from ..xiuxian_utils import db_backend


@dataclass(frozen=True)
class MapExploreSettlementResult:
    status: str
    stone: int
    rewards: tuple[tuple[int, int], ...]

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


class MapExploreSettlementService:
    """Atomically clear a completed exploration and grant its fixed rewards."""

    def __init__(self, game_database: str | Path, player_database: str | Path, lock: RLock | None = None) -> None:
        self._game_database = Path(game_database)
        self._player_database = Path(player_database)
        self._lock = lock or RLock()

    def settle(self, operation_id, user_id, expected_state, expected_daily, daily_limit, stone, items, max_goods_num):
        operation_id, user_id = str(operation_id).strip(), str(user_id)
        state = {key: str(value) for key, value in dict(expected_state).items()}
        daily = {key: str(value) for key, value in dict(expected_daily).items()}
        daily_limit, stone, max_goods_num = map(int, (daily_limit, stone, max_goods_num))
        rewards = tuple(
            (int(item["id"]), str(item["name"]), str(item["type"]), int(item["amount"]))
            for item in items if int(item["amount"]) > 0
        )
        if not operation_id or min(daily_limit, stone, max_goods_num) < 0 or state.get("running") != "1" or not daily.get("date"):
            raise ValueError("valid operation, explore state and daily state are required")
        payload = json.dumps([user_id, state, daily, daily_limit, stone, rewards, max_goods_num], ensure_ascii=True, sort_keys=True)

        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            attached = False
            try:
                conn.execute("ATTACH DATABASE %s AS player_data", (str(self._player_database),))
                attached = True
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS map_explore_settlement_operations ("
                    "operation_id TEXT PRIMARY KEY, payload TEXT NOT NULL, stone INTEGER NOT NULL, "
                    "rewards TEXT NOT NULL, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
                )
                old = conn.execute(
                    "SELECT payload,stone,rewards FROM map_explore_settlement_operations WHERE operation_id=%s", (operation_id,)
                ).fetchone()
                if old is not None:
                    conn.rollback()
                    if str(old[0]) != payload:
                        return MapExploreSettlementResult("state_changed", 0, ())
                    return MapExploreSettlementResult("duplicate", int(old[1]), tuple(tuple(map(int, value)) for value in json.loads(str(old[2]))))
                if conn.execute("SELECT 1 FROM user_xiuxian WHERE user_id=%s", (user_id,)).fetchone() is None:
                    conn.rollback()
                    return MapExploreSettlementResult("user_missing", 0, ())

                explore_columns = {str(row[1]) for row in conn.execute("PRAGMA player_data.table_info(map_explore_status)").fetchall()}
                daily_columns = {str(row[1]) for row in conn.execute("PRAGMA player_data.table_info(map_daily_limit)").fetchall()}
                if not set(state).issubset(explore_columns) or not {"date", "explore_count", "resource_total_count"}.issubset(daily_columns):
                    conn.rollback()
                    return MapExploreSettlementResult("state_changed", 0, ())
                status_row = conn.execute(
                    "SELECT " + ",".join(state) + " FROM player_data.map_explore_status WHERE user_id=%s", (user_id,)
                ).fetchone()
                if status_row is None or tuple(str(value) for value in status_row) != tuple(state.values()):
                    conn.rollback()
                    return MapExploreSettlementResult("state_changed", 0, ())
                daily_row = conn.execute(
                    "SELECT date,explore_count,resource_total_count FROM player_data.map_daily_limit WHERE user_id=%s", (user_id,)
                ).fetchone()
                if daily_row is None or tuple(str(value) for value in daily_row) != (
                    daily["date"], daily.get("explore_count", "0"), daily.get("resource_total_count", "0"),
                ):
                    conn.rollback()
                    return MapExploreSettlementResult("state_changed", 0, ())
                if int(daily_row[1] or 0) >= daily_limit:
                    conn.rollback()
                    return MapExploreSettlementResult("limit_reached", 0, ())

                totals: dict[int, int] = {}
                metadata: dict[int, tuple[str, str]] = {}
                for item_id, name, item_type, amount in rewards:
                    totals[item_id] = totals.get(item_id, 0) + amount
                    metadata[item_id] = (name, item_type)
                for item_id, amount in totals.items():
                    row = conn.execute("SELECT COALESCE(goods_num,0) FROM back WHERE user_id=%s AND goods_id=%s", (user_id, item_id)).fetchone()
                    if (int(row[0]) if row else 0) + amount > max_goods_num:
                        conn.rollback()
                        return MapExploreSettlementResult("inventory_full", 0, ())

                conn.execute(
                    "UPDATE player_data.map_daily_limit SET explore_count=%s,resource_total_count=%s WHERE user_id=%s",
                    (int(daily_row[1] or 0) + 1, int(daily_row[2] or 0) + 1, user_id),
                )
                clear_values = {
                    "running": 0, "node_type": "", "node_name": "", "start_time": "", "duration_min": 0,
                    "max_duration_min": 0, "interval_min": 0,
                }
                if "settlement" in explore_columns:
                    clear_values["settlement"] = ""
                clear_values = {key: value for key, value in clear_values.items() if key in explore_columns}
                conn.execute(
                    "UPDATE player_data.map_explore_status SET " + ",".join(f'\"{key}\"=%s' for key in clear_values) + " WHERE user_id=%s",
                    (*clear_values.values(), user_id),
                )
                if stone:
                    conn.execute("UPDATE user_xiuxian SET stone=stone+%s WHERE user_id=%s", (stone, user_id))
                now = datetime.now()
                for item_id, amount in totals.items():
                    name, item_type = metadata[item_id]
                    conn.execute(
                        "INSERT INTO back (user_id,goods_id,goods_name,goods_type,goods_num,create_time,update_time,bind_num) "
                        "VALUES (%s,%s,%s,%s,%s,%s,%s,%s) "
                        "ON CONFLICT(user_id,goods_id) DO UPDATE SET goods_name=EXCLUDED.goods_name,goods_type=EXCLUDED.goods_type,"
                        "goods_num=back.goods_num+EXCLUDED.goods_num,bind_num=COALESCE(back.bind_num,0)+EXCLUDED.bind_num,update_time=EXCLUDED.update_time",
                        (user_id, item_id, name, item_type, amount, now, now, amount),
                    )
                compact_rewards = tuple(sorted(totals.items()))
                conn.execute(
                    "INSERT INTO map_explore_settlement_operations (operation_id,payload,stone,rewards) VALUES (%s,%s,%s,%s)",
                    (operation_id, payload, stone, json.dumps(compact_rewards)),
                )
                conn.commit()
                return MapExploreSettlementResult("applied", stone, compact_rewards)
            except Exception:
                conn.rollback()
                raise
            finally:
                if attached:
                    conn.execute("DETACH DATABASE player_data")


__all__ = ["MapExploreSettlementResult", "MapExploreSettlementService"]
