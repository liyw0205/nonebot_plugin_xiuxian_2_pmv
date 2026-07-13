from __future__ import annotations

import json
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from threading import RLock

from ..xiuxian_utils import db_backend


@dataclass(frozen=True)
class BlessedSpotResult:
    status: str
    user_id: str
    stone_cost: int = 0
    name: str = ""
    previous_level: int = 0
    current_level: int = 0

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


class BlessedSpotService:
    MIX_DEFAULTS = {
        "收取时间": "",
        "收取等级": 0,
        "灵田数量": 1,
        "药材速度": 0,
        "灵田傀儡": 0,
        "丹药控火": 0,
        "丹药耐药性": 0,
        "炼丹记录": {},
        "炼丹经验": 0,
    }

    def __init__(self, game_database: str | Path, player_database: str | Path, lock: RLock | None = None) -> None:
        self._game_database = Path(game_database)
        self._player_database = Path(player_database)
        self._lock = lock or RLock()

    @staticmethod
    def _ensure_operation_table(conn) -> None:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS blessed_spot_operations ("
            "operation_id TEXT PRIMARY KEY,action TEXT NOT NULL,payload TEXT NOT NULL,"
            "result_json TEXT NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
        )

    @classmethod
    def _ensure_mix_table(cls, conn) -> None:
        conn.execute("CREATE TABLE IF NOT EXISTS player_data.mix_elixir_info (user_id TEXT PRIMARY KEY)")
        columns = {
            str(row[1])
            for row in conn.execute("PRAGMA player_data.table_info(mix_elixir_info)").fetchall()
        }
        for field in cls.MIX_DEFAULTS:
            if field not in columns:
                conn.execute(
                    f"ALTER TABLE player_data.mix_elixir_info ADD COLUMN {db_backend.quote_ident(field)} TEXT"
                )

    @staticmethod
    def _payload(values) -> str:
        return json.dumps(values, ensure_ascii=True, sort_keys=True, separators=(",", ":"))

    def open(self, operation_id, user_id, stone_cost, default_name, harvest_time) -> BlessedSpotResult:
        operation_id = str(operation_id).strip()
        user_id = str(user_id)
        stone_cost = int(stone_cost)
        default_name = str(default_name)
        harvest_time = str(harvest_time)
        if not operation_id or stone_cost <= 0 or not default_name or not harvest_time:
            raise ValueError("valid operation, cost, name and harvest time are required")
        payload = self._payload([user_id, stone_cost, default_name, harvest_time])

        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            conn.execute("ATTACH DATABASE %s AS player_data", (str(self._player_database),))
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_operation_table(conn)
                self._ensure_mix_table(conn)
                previous = conn.execute(
                    "SELECT payload,result_json FROM blessed_spot_operations WHERE operation_id=%s AND action=%s",
                    (operation_id, "open"),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    if str(previous[0]) != payload:
                        return BlessedSpotResult("state_changed", user_id)
                    saved = json.loads(str(previous[1]))
                    return BlessedSpotResult("duplicate", user_id, saved["stone_cost"], saved["name"])

                user = conn.execute(
                    "SELECT COALESCE(stone,0),COALESCE(blessed_spot_flag,0) FROM user_xiuxian WHERE user_id=%s",
                    (user_id,),
                ).fetchone()
                if user is None:
                    conn.rollback()
                    return BlessedSpotResult("user_missing", user_id)
                if int(user[1]) != 0:
                    conn.rollback()
                    return BlessedSpotResult("already_owned", user_id)
                if int(user[0]) < stone_cost:
                    conn.rollback()
                    return BlessedSpotResult("stone_insufficient", user_id, stone_cost)
                changed = conn.execute(
                    "UPDATE user_xiuxian SET stone=stone-%s,blessed_spot_flag=1,blessed_spot_name=%s "
                    "WHERE user_id=%s AND stone>=%s AND COALESCE(blessed_spot_flag,0)=0",
                    (stone_cost, default_name, user_id, stone_cost),
                )
                if changed.rowcount != 1:
                    conn.rollback()
                    return BlessedSpotResult("state_changed", user_id)

                values = dict(self.MIX_DEFAULTS)
                values["收取时间"] = harvest_time
                columns = ["user_id", *values]
                params = [user_id]
                for value in values.values():
                    params.append(json.dumps(value, ensure_ascii=False) if isinstance(value, (dict, list)) else str(value))
                quoted = ",".join(db_backend.quote_ident(column) for column in columns)
                placeholders = ",".join("%s" for _ in columns)
                updates = ",".join(
                    f"{db_backend.quote_ident(field)}=EXCLUDED.{db_backend.quote_ident(field)}" for field in values
                )
                conn.execute(
                    f"INSERT INTO player_data.mix_elixir_info ({quoted}) VALUES ({placeholders}) "
                    f"ON CONFLICT(user_id) DO UPDATE SET {updates}",
                    tuple(params),
                )
                result_json = json.dumps({"stone_cost": stone_cost, "name": default_name}, ensure_ascii=False)
                conn.execute(
                    "INSERT INTO blessed_spot_operations (operation_id,action,payload,result_json) VALUES (%s,%s,%s,%s)",
                    (operation_id, "open", payload, result_json),
                )
                conn.commit()
                return BlessedSpotResult("applied", user_id, stone_cost, default_name)
            except Exception:
                conn.rollback()
                raise
            finally:
                conn.execute("DETACH DATABASE player_data")


__all__ = ["BlessedSpotResult", "BlessedSpotService"]
