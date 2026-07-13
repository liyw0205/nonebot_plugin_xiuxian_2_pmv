from __future__ import annotations

import json
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from threading import RLock

from ..xiuxian_utils import db_backend


@dataclass(frozen=True)
class MixelixirRefineRewardResult:
    status: str
    reward_id: int = 0
    reward_name: str = ""
    reward_quantity: int = 0

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


class MixelixirRefineRewardService:
    """Atomically complete a refining task, grant its item and save claim state."""

    _MIX_FIELDS = ("丹药控火", "炼丹记录", "炼丹经验")

    def __init__(self, game_database: str | Path, player_database: str | Path, lock: RLock | None = None) -> None:
        self._game_database = Path(game_database)
        self._player_database = Path(player_database)
        self._lock = lock or RLock()

    @staticmethod
    def _state_value(field: str, value) -> str:
        if field == "炼丹记录":
            return json.dumps(value, ensure_ascii=False, sort_keys=True)
        return str(value)

    def latest_ready_task(self, user_id) -> str | None:
        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            row = conn.execute(
                "SELECT task_id FROM mixelixir_refine_tasks WHERE user_id=%s AND status=%s "
                "ORDER BY created_at DESC,task_id DESC LIMIT 1",
                (str(user_id), "ready"),
            ).fetchone()
        return None if row is None else str(row[0])

    def claim(self, operation_id, user_id, task_id, max_goods_num) -> MixelixirRefineRewardResult:
        operation_id, user_id, task_id = str(operation_id).strip(), str(user_id), str(task_id)
        max_goods_num = int(max_goods_num)
        if not operation_id or not task_id or max_goods_num <= 0:
            raise ValueError("valid operation, task and capacity are required")
        payload = json.dumps([user_id, task_id, max_goods_num], ensure_ascii=True)

        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            attached = False
            try:
                conn.execute("ATTACH DATABASE %s AS player_data", (str(self._player_database),))
                attached = True
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS mixelixir_refine_reward_operations ("
                    "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,task_id TEXT NOT NULL,reward_id INTEGER NOT NULL,"
                    "reward_name TEXT NOT NULL,reward_quantity INTEGER NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
                )
                previous = conn.execute(
                    "SELECT payload,reward_id,reward_name,reward_quantity FROM mixelixir_refine_reward_operations "
                    "WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    if str(previous[0]) != payload:
                        return MixelixirRefineRewardResult("state_changed")
                    return MixelixirRefineRewardResult("duplicate", int(previous[1]), str(previous[2]), int(previous[3]))

                if conn.execute("SELECT 1 FROM user_xiuxian WHERE user_id=%s", (user_id,)).fetchone() is None:
                    conn.rollback()
                    return MixelixirRefineRewardResult("user_missing")
                task = conn.execute(
                    "SELECT status,reward_id,reward_name,reward_quantity,expected_mix_state,updated_mix_state "
                    "FROM mixelixir_refine_tasks WHERE task_id=%s AND user_id=%s",
                    (task_id, user_id),
                ).fetchone()
                if task is None:
                    conn.rollback()
                    return MixelixirRefineRewardResult("task_missing")
                if str(task[0]) != "ready":
                    conn.rollback()
                    return MixelixirRefineRewardResult("state_changed")
                reward_id, reward_name, reward_quantity = int(task[1]), str(task[2]), int(task[3])
                expected, updated = json.loads(str(task[4])), json.loads(str(task[5]))

                table = conn.execute(
                    "SELECT 1 FROM player_data.sqlite_master WHERE type='table' AND name=%s", ("mix_elixir_info",)
                ).fetchone()
                if table is None:
                    conn.rollback()
                    return MixelixirRefineRewardResult("state_changed")
                columns = {
                    str(column[1])
                    for column in conn.execute("PRAGMA player_data.table_info(mix_elixir_info)").fetchall()
                }
                if not set(self._MIX_FIELDS).issubset(columns):
                    conn.rollback()
                    return MixelixirRefineRewardResult("state_changed")
                quoted = ",".join(db_backend.quote_ident(field) for field in self._MIX_FIELDS)
                current = conn.execute(
                    f"SELECT {quoted} FROM player_data.mix_elixir_info WHERE user_id=%s", (user_id,)
                ).fetchone()
                if current is None or tuple(str(value) for value in current) != tuple(
                    self._state_value(field, expected[field]) for field in self._MIX_FIELDS
                ):
                    conn.rollback()
                    return MixelixirRefineRewardResult("state_changed")

                inventory = conn.execute(
                    "SELECT COALESCE(goods_num,0) FROM back WHERE user_id=%s AND goods_id=%s", (user_id, reward_id)
                ).fetchone()
                if (int(inventory[0]) if inventory else 0) + reward_quantity > max_goods_num:
                    conn.rollback()
                    return MixelixirRefineRewardResult("inventory_full")

                back_columns = set(conn.column_names("back"))
                insert_columns = "user_id,goods_id,goods_name,goods_type,goods_num"
                insert_values = "%s,%s,%s,%s,%s"
                if "bind_num" in back_columns:
                    insert_columns += ",bind_num"
                    insert_values += ",0"
                conn.execute(
                    f"INSERT INTO back ({insert_columns}) VALUES ({insert_values}) ON CONFLICT(user_id,goods_id) "
                    "DO UPDATE SET goods_name=EXCLUDED.goods_name,goods_type=EXCLUDED.goods_type,"
                    "goods_num=back.goods_num+EXCLUDED.goods_num",
                    (user_id, reward_id, reward_name, "丹药", reward_quantity),
                )
                assignments = ",".join(f"{db_backend.quote_ident(field)}=%s" for field in self._MIX_FIELDS)
                changed = conn.execute(
                    f"UPDATE player_data.mix_elixir_info SET {assignments} WHERE user_id=%s",
                    tuple(self._state_value(field, updated[field]) for field in self._MIX_FIELDS) + (user_id,),
                )
                if changed.rowcount != 1:
                    conn.rollback()
                    return MixelixirRefineRewardResult("state_changed")

                conn.execute("CREATE TABLE IF NOT EXISTS player_data.statistics (user_id TEXT PRIMARY KEY)")
                statistics_columns = {
                    str(column[1])
                    for column in conn.execute("PRAGMA player_data.table_info(statistics)").fetchall()
                }
                if "炼丹次数" not in statistics_columns:
                    conn.execute(
                        f"ALTER TABLE player_data.statistics ADD COLUMN {db_backend.quote_ident('炼丹次数')} INTEGER DEFAULT NULL"
                    )
                conn.execute(
                    f"INSERT INTO player_data.statistics (user_id,{db_backend.quote_ident('炼丹次数')}) VALUES (%s,1) "
                    f"ON CONFLICT(user_id) DO UPDATE SET {db_backend.quote_ident('炼丹次数')}="
                    f"COALESCE(player_data.statistics.{db_backend.quote_ident('炼丹次数')},0)+1",
                    (user_id,),
                )
                changed = conn.execute(
                    "UPDATE mixelixir_refine_tasks SET status=%s,claimed_at=CURRENT_TIMESTAMP "
                    "WHERE task_id=%s AND user_id=%s AND status=%s",
                    ("claimed", task_id, user_id, "ready"),
                )
                if changed.rowcount != 1:
                    conn.rollback()
                    return MixelixirRefineRewardResult("state_changed")
                conn.execute(
                    "INSERT INTO mixelixir_refine_reward_operations "
                    "(operation_id,payload,task_id,reward_id,reward_name,reward_quantity) VALUES (%s,%s,%s,%s,%s,%s)",
                    (operation_id, payload, task_id, reward_id, reward_name, reward_quantity),
                )
                conn.commit()
                return MixelixirRefineRewardResult("applied", reward_id, reward_name, reward_quantity)
            except Exception:
                conn.rollback()
                raise
            finally:
                if attached:
                    conn.execute("DETACH DATABASE player_data")


__all__ = ["MixelixirRefineRewardResult", "MixelixirRefineRewardService"]
