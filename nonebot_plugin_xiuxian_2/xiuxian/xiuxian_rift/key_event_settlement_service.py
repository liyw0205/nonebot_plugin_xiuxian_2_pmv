from __future__ import annotations

import json
from contextlib import closing
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from threading import RLock

from ..xiuxian_utils import db_backend


def _ensure_player_field(conn, table, field, data_type="TEXT"):
    table_sql, field_sql = db_backend.quote_ident(table), db_backend.quote_ident(field)
    conn.execute(f"CREATE TABLE IF NOT EXISTS player_data.{table_sql}(user_id TEXT PRIMARY KEY)")
    columns = {str(row[1]) for row in conn.execute(f"PRAGMA player_data.table_info({table_sql})").fetchall()}
    if field not in columns:
        conn.execute(f"ALTER TABLE player_data.{table_sql} ADD COLUMN {field_sql} {data_type}")


def _set_player_field(conn, table, user_id, field, value, data_type="TEXT"):
    _ensure_player_field(conn, table, field, data_type)
    table_sql, field_sql = db_backend.quote_ident(table), db_backend.quote_ident(field)
    changed = conn.execute(f"UPDATE player_data.{table_sql} SET {field_sql}=%s WHERE user_id=%s", (value, user_id))
    if changed.rowcount == 0:
        conn.execute(f"INSERT INTO player_data.{table_sql}(user_id,{field_sql}) VALUES(%s,%s)", (user_id, value))


def _increment_stat(conn, user_id, key, amount):
    _ensure_player_field(conn, "statistics", key, "INTEGER")
    field_sql = db_backend.quote_ident(key)
    changed = conn.execute(f"UPDATE player_data.statistics SET {field_sql}=COALESCE({field_sql},0)+%s WHERE user_id=%s", (amount, user_id))
    if changed.rowcount == 0:
        conn.execute(f"INSERT INTO player_data.statistics(user_id,{field_sql}) VALUES(%s,%s)", (user_id, amount))


def _normalise_progress_reward(expected_count, progress_reward):
    expected_count = int(expected_count)
    if not 0 <= expected_count <= 9:
        raise ValueError("expected explore count must be between 0 and 9")
    if expected_count != 9:
        if progress_reward is not None:
            raise ValueError("progress reward must match the tenth completion")
        return None
    if not isinstance(progress_reward, dict):
        raise ValueError("progress reward must match the tenth completion")
    try:
        reward = (
            int(progress_reward["id"]),
            str(progress_reward["name"]).strip(),
            str(progress_reward["type"]).strip(),
            int(progress_reward.get("amount", 1)),
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise ValueError("progress reward must be complete") from exc
    if reward[0] <= 0 or not reward[1] or not reward[2] or reward[3] <= 0:
        raise ValueError("progress reward must be complete and positive")
    return reward


@dataclass(frozen=True)
class RiftKeyEventSettlementResult:
    status: str
    explore_count: int = 0
    message: str = ""

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


class RiftKeyEventSettlementService:
    """Consume a rift item and commit a pre-rolled event across both databases."""

    def __init__(
        self,
        game_database: str | Path,
        player_database: str | Path,
        lock: RLock | None = None,
        operation_table: str = "rift_key_event_operations",
    ) -> None:
        self._game_database = Path(game_database)
        self._player_database = Path(player_database)
        self._lock = lock or RLock()
        self._operation_table = operation_table

    def replay(self, operation_id) -> RiftKeyEventSettlementResult | None:
        operation_id = str(operation_id).strip()
        if not operation_id:
            return None
        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            table = db_backend.quote_ident(self._operation_table)
            if not conn.table_exists(self._operation_table):
                return None
            row = conn.execute(f"SELECT explore_count,message FROM {table} WHERE operation_id=%s", (operation_id,)).fetchone()
            if row is None:
                return None
            return RiftKeyEventSettlementResult("duplicate", int(row[0]), str(row[1]))

    def settle(
        self,
        operation_id,
        user_id,
        item_id,
        expected_rift,
        expected_user,
        expected_explore_count,
        outcome,
        max_goods_num,
    ) -> RiftKeyEventSettlementResult:
        operation_id, user_id, item_id = str(operation_id).strip(), str(user_id), int(item_id)
        expected_count, max_goods_num = int(expected_explore_count), int(max_goods_num)
        rift_snapshot = json.dumps(expected_rift, ensure_ascii=False, sort_keys=True)
        expected = tuple(int(expected_user.get(key, 0)) for key in ("stone", "exp", "hp", "mp"))
        delta = tuple(int(outcome.get("delta", {}).get(key, 0)) for key in ("stone", "exp", "hp", "mp"))
        rewards = tuple(
            (int(item["id"]), str(item["name"]), str(item["type"]), int(item.get("amount", 1)))
            for item in outcome.get("items", ())
            if int(item.get("amount", 1)) > 0
        )
        progress_reward = _normalise_progress_reward(
            expected_count, outcome.get("progress_reward")
        )
        if progress_reward is not None:
            rewards += (progress_reward,)
        statistics = tuple(sorted((str(key), int(value)) for key, value in outcome.get("statistics", {}).items() if int(value)))
        message = str(outcome.get("message", ""))
        if not operation_id or not user_id or item_id <= 0 or expected_count < 0 or max_goods_num < 0:
            raise ValueError("valid operation, user, item and snapshots are required")
        payload = json.dumps(
            [user_id, item_id, rift_snapshot, expected, expected_count, delta, rewards, statistics, message, max_goods_num],
            ensure_ascii=True,
            sort_keys=True,
        )

        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            attached = False
            try:
                conn.execute("ATTACH DATABASE %s AS player_data", (str(self._player_database),))
                attached = True
                conn.execute("BEGIN IMMEDIATE")
                table = db_backend.quote_ident(self._operation_table)
                conn.execute(
                    f"CREATE TABLE IF NOT EXISTS {table} ("
                    "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,explore_count INTEGER NOT NULL,"
                    "message TEXT NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
                )
                old = conn.execute(
                    f"SELECT payload,explore_count,message FROM {table} WHERE operation_id=%s", (operation_id,)
                ).fetchone()
                if old is not None:
                    conn.rollback()
                    if str(old[0]) == payload:
                        return RiftKeyEventSettlementResult("duplicate", int(old[1]), str(old[2]))
                    return RiftKeyEventSettlementResult("state_changed")

                entry = conn.execute("SELECT rift_data,status FROM rift_entries WHERE user_id=%s", (user_id,)).fetchone()
                cd = conn.execute("SELECT COALESCE(type,0) FROM user_cd WHERE user_id=%s", (user_id,)).fetchone()
                user = conn.execute(
                    "SELECT stone,exp,hp,mp FROM user_xiuxian WHERE user_id=%s", (user_id,)
                ).fetchone()
                if entry is None or str(entry[1]) != "active" or cd is None or int(cd[0]) != 3:
                    conn.rollback()
                    return RiftKeyEventSettlementResult("not_active")
                if json.loads(str(entry[0])) != json.loads(rift_snapshot) or user is None or tuple(map(int, user)) != expected:
                    conn.rollback()
                    return RiftKeyEventSettlementResult("state_changed")
                item = conn.execute(
                    "SELECT COALESCE(goods_num,0) FROM back WHERE user_id=%s AND goods_id=%s", (user_id, item_id)
                ).fetchone()
                if item is None or int(item[0]) < 1:
                    conn.rollback()
                    return RiftKeyEventSettlementResult("item_missing")

                rift_columns = {
                    str(row[1]) for row in conn.execute('PRAGMA player_data.table_info("rift")').fetchall()
                }
                count_row = conn.execute(
                    'SELECT "explore_count" FROM player_data."rift" WHERE user_id=%s', (user_id,)
                ).fetchone() if "explore_count" in rift_columns else None
                current_count = int(count_row[0] or 0) if count_row else 0
                if current_count != expected_count:
                    conn.rollback()
                    return RiftKeyEventSettlementResult("state_changed")
                if expected[0] + delta[0] < 0:
                    conn.rollback()
                    return RiftKeyEventSettlementResult("resource_missing")

                totals: dict[int, int] = {}
                metadata: dict[int, tuple[str, str]] = {}
                for reward_id, name, item_type, amount in rewards:
                    totals[reward_id] = totals.get(reward_id, 0) + amount
                    old_metadata = metadata.setdefault(reward_id, (name, item_type))
                    if old_metadata != (name, item_type):
                        raise ValueError("conflicting reward metadata")
                for reward_id, amount in totals.items():
                    row = conn.execute(
                        "SELECT COALESCE(goods_num,0) FROM back WHERE user_id=%s AND goods_id=%s", (user_id, reward_id)
                    ).fetchone()
                    current_amount = int(row[0]) if row else 0
                    consumed_amount = 1 if reward_id == item_id else 0
                    if current_amount - consumed_amount + amount > max_goods_num:
                        conn.rollback()
                        return RiftKeyEventSettlementResult("inventory_full")

                bind_update = ""
                if conn.column_exists("back", "bind_num"):
                    bind_update = (
                        ",bind_num=MIN("
                        "MAX(COALESCE(bind_num,0)-1,0),goods_num-1)"
                    )
                consumed = conn.execute(
                    "UPDATE back SET goods_num=goods_num-1" + bind_update + " "
                    "WHERE user_id=%s AND goods_id=%s "
                    "AND COALESCE(goods_num,0)>=1",
                    (user_id, item_id),
                )
                if consumed.rowcount != 1:
                    conn.rollback()
                    return RiftKeyEventSettlementResult("item_missing")
                final_exp = max(0, expected[1] + delta[1])
                final_hp = max(1, expected[2] + delta[2])
                final_mp = max(1, expected[3] + delta[3])
                conn.execute(
                    "UPDATE user_xiuxian SET stone=stone+%s,exp=%s,hp=%s,mp=%s WHERE user_id=%s",
                    (delta[0], final_exp, final_hp, final_mp, user_id),
                )
                now = datetime.now()
                for reward_id, amount in totals.items():
                    name, item_type = metadata[reward_id]
                    conn.execute(
                        "INSERT INTO back(user_id,goods_id,goods_name,goods_type,goods_num,create_time,update_time,bind_num) "
                        "VALUES(%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT(user_id,goods_id) DO UPDATE SET "
                        "goods_name=EXCLUDED.goods_name,goods_type=EXCLUDED.goods_type,goods_num=back.goods_num+EXCLUDED.goods_num,"
                        "bind_num=COALESCE(back.bind_num,0)+EXCLUDED.bind_num,update_time=EXCLUDED.update_time",
                        (user_id, reward_id, name, item_type, amount, now, now, amount),
                    )
                new_count = 0 if expected_count + 1 >= 10 else expected_count + 1
                _set_player_field(conn, "rift", user_id, "explore_count", new_count, "INTEGER")
                for key, amount in statistics:
                    _increment_stat(conn, user_id, key, amount)
                conn.execute("UPDATE rift_entries SET status='settled' WHERE user_id=%s", (user_id,))
                conn.execute(
                    "UPDATE user_cd SET type=0,create_time=0,scheduled_time=NULL WHERE user_id=%s AND type=3", (user_id,)
                )
                conn.execute(
                    f"INSERT INTO {table}(operation_id,payload,explore_count,message) VALUES(%s,%s,%s,%s)",
                    (operation_id, payload, new_count, message),
                )
                conn.commit()
                return RiftKeyEventSettlementResult("applied", new_count, message)
            except Exception:
                conn.rollback()
                raise
            finally:
                if attached:
                    conn.execute("DETACH DATABASE player_data")


__all__ = ["RiftKeyEventSettlementResult", "RiftKeyEventSettlementService"]
