from __future__ import annotations

import json
from contextlib import closing
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from threading import RLock

from ..xiuxian_utils import db_backend
from .key_event_settlement_service import (
    _ensure_player_field,
    _increment_stat,
    _normalise_progress_reward,
    _set_player_field,
)


@dataclass(frozen=True)
class RiftSettlementResult:
    status: str
    explore_count: int = 0
    message: str = ""

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


class RiftSettlementService:
    """Commit one pre-rolled ordinary rift event across both databases."""

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
            "CREATE TABLE IF NOT EXISTS rift_settlement_operations("
            "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,"
            "explore_count INTEGER NOT NULL,message TEXT NOT NULL DEFAULT '',"
            "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
        )
        if not conn.column_exists("rift_settlement_operations", "message"):
            conn.execute(
                "ALTER TABLE rift_settlement_operations "
                "ADD COLUMN message TEXT NOT NULL DEFAULT ''"
            )

    def replay(self, operation_id) -> RiftSettlementResult | None:
        operation_id = str(operation_id).strip()
        if not operation_id:
            return None
        with self._lock, closing(
            db_backend.connect(self._game_database)
        ) as conn:
            if not conn.table_exists("rift_settlement_operations"):
                return None
            columns = {
                str(row[1])
                for row in conn.execute(
                    "PRAGMA table_info(rift_settlement_operations)"
                ).fetchall()
            }
            message_sql = "message" if "message" in columns else "''"
            row = conn.execute(
                "SELECT explore_count," + message_sql + " "
                "FROM rift_settlement_operations WHERE operation_id=%s",
                (operation_id,),
            ).fetchone()
            if row is None:
                return None
            return RiftSettlementResult("duplicate", int(row[0]), str(row[1]))

    def settle(
        self,
        operation_id,
        user_id,
        expected_rift,
        expected_user,
        expected_explore_count,
        outcome,
        max_goods_num,
    ) -> RiftSettlementResult:
        operation_id = str(operation_id).strip()
        user_id = str(user_id).strip()
        expected_count = int(expected_explore_count)
        max_goods_num = int(max_goods_num)
        rift_snapshot = json.dumps(
            expected_rift, ensure_ascii=False, sort_keys=True
        )
        expected = tuple(
            int(expected_user.get(key, 0))
            for key in ("stone", "exp", "hp", "mp")
        )
        delta = tuple(
            int(outcome.get("delta", {}).get(key, 0))
            for key in ("stone", "exp", "hp", "mp")
        )
        rewards = tuple(
            (
                int(item["id"]),
                str(item["name"]),
                str(item["type"]),
                int(item.get("amount", 1)),
            )
            for item in outcome.get("items", ())
            if int(item.get("amount", 1)) > 0
        )
        progress_reward = _normalise_progress_reward(
            expected_count, outcome.get("progress_reward")
        )
        if progress_reward is not None:
            rewards += (progress_reward,)
        statistics = tuple(
            sorted(
                (str(key), int(value))
                for key, value in outcome.get("statistics", {}).items()
                if int(value)
            )
        )
        message = str(outcome.get("message", ""))
        if (
            not operation_id
            or not user_id
            or expected_count < 0
            or max_goods_num < 0
            or not message
        ):
            raise ValueError("valid operation, user, outcome and snapshots are required")
        payload = json.dumps(
            [
                user_id,
                rift_snapshot,
                expected,
                expected_count,
                delta,
                rewards,
                statistics,
                message,
                max_goods_num,
            ],
            ensure_ascii=True,
            sort_keys=True,
        )

        with self._lock, closing(
            db_backend.connect(self._game_database)
        ) as conn:
            attached = False
            try:
                conn.execute(
                    "ATTACH DATABASE %s AS player_data",
                    (str(self._player_database),),
                )
                attached = True
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_schema(conn)
                old = conn.execute(
                    "SELECT payload,explore_count,message "
                    "FROM rift_settlement_operations WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if old is not None:
                    conn.rollback()
                    if str(old[0]) == payload:
                        return RiftSettlementResult(
                            "duplicate", int(old[1]), str(old[2])
                        )
                    return RiftSettlementResult("state_changed")

                entry = conn.execute(
                    "SELECT rift_data,status FROM rift_entries WHERE user_id=%s",
                    (user_id,),
                ).fetchone()
                cd = conn.execute(
                    "SELECT COALESCE(type,0),create_time,scheduled_time "
                    "FROM user_cd WHERE user_id=%s",
                    (user_id,),
                ).fetchone()
                user = conn.execute(
                    "SELECT stone,exp,hp,mp FROM user_xiuxian WHERE user_id=%s",
                    (user_id,),
                ).fetchone()
                if (
                    entry is None
                    or str(entry[1]) != "active"
                    or cd is None
                    or int(cd[0]) != 3
                ):
                    conn.rollback()
                    return RiftSettlementResult("not_active")
                try:
                    expected_duration = int(expected_rift.get("time", 0))
                    scheduled_duration = int(cd[2])
                except (TypeError, ValueError):
                    conn.rollback()
                    return RiftSettlementResult("state_changed")
                elapsed_row = conn.execute(
                    "SELECT (julianday('now')-julianday(%s))*1440.0",
                    (cd[1],),
                ).fetchone()
                if (
                    expected_duration <= 0
                    or scheduled_duration != expected_duration
                    or elapsed_row is None
                    or elapsed_row[0] is None
                ):
                    conn.rollback()
                    return RiftSettlementResult("state_changed")
                if max(0, int(float(elapsed_row[0]))) < expected_duration:
                    conn.rollback()
                    return RiftSettlementResult("not_ready")
                if (
                    json.loads(str(entry[0])) != json.loads(rift_snapshot)
                    or user is None
                    or tuple(map(int, user)) != expected
                ):
                    conn.rollback()
                    return RiftSettlementResult("state_changed")

                _ensure_player_field(conn, "rift", "explore_count", "INTEGER")
                count_row = conn.execute(
                    'SELECT "explore_count" FROM player_data."rift" '
                    "WHERE user_id=%s",
                    (user_id,),
                ).fetchone()
                current_count = int(count_row[0] or 0) if count_row else 0
                if current_count != expected_count:
                    conn.rollback()
                    return RiftSettlementResult("state_changed")
                if expected[0] + delta[0] < 0:
                    conn.rollback()
                    return RiftSettlementResult("resource_missing")

                totals: dict[int, int] = {}
                metadata: dict[int, tuple[str, str]] = {}
                for reward_id, name, item_type, amount in rewards:
                    totals[reward_id] = totals.get(reward_id, 0) + amount
                    old_metadata = metadata.setdefault(
                        reward_id, (name, item_type)
                    )
                    if old_metadata != (name, item_type):
                        raise ValueError("conflicting reward metadata")
                for reward_id, amount in totals.items():
                    row = conn.execute(
                        "SELECT COALESCE(goods_num,0) FROM back "
                        "WHERE user_id=%s AND goods_id=%s",
                        (user_id, reward_id),
                    ).fetchone()
                    if (int(row[0]) if row else 0) + amount > max_goods_num:
                        conn.rollback()
                        return RiftSettlementResult("inventory_full")

                final_exp = max(0, expected[1] + delta[1])
                final_hp = max(1, expected[2] + delta[2])
                final_mp = max(1, expected[3] + delta[3])
                conn.execute(
                    "UPDATE user_xiuxian SET stone=stone+%s,exp=%s,hp=%s,mp=%s "
                    "WHERE user_id=%s",
                    (delta[0], final_exp, final_hp, final_mp, user_id),
                )
                now = datetime.now()
                for reward_id, amount in totals.items():
                    name, item_type = metadata[reward_id]
                    conn.execute(
                        "INSERT INTO back("
                        "user_id,goods_id,goods_name,goods_type,goods_num,"
                        "create_time,update_time,bind_num) "
                        "VALUES(%s,%s,%s,%s,%s,%s,%s,%s) "
                        "ON CONFLICT(user_id,goods_id) DO UPDATE SET "
                        "goods_name=EXCLUDED.goods_name,"
                        "goods_type=EXCLUDED.goods_type,"
                        "goods_num=back.goods_num+EXCLUDED.goods_num,"
                        "bind_num=COALESCE(back.bind_num,0)+EXCLUDED.bind_num,"
                        "update_time=EXCLUDED.update_time",
                        (
                            user_id,
                            reward_id,
                            name,
                            item_type,
                            amount,
                            now,
                            now,
                            amount,
                        ),
                    )

                new_count = (
                    0 if expected_count + 1 >= 10 else expected_count + 1
                )
                _set_player_field(
                    conn,
                    "rift",
                    user_id,
                    "explore_count",
                    new_count,
                    "INTEGER",
                )
                for key, amount in statistics:
                    _increment_stat(conn, user_id, key, amount)
                conn.execute(
                    "UPDATE rift_entries SET status='settled' WHERE user_id=%s",
                    (user_id,),
                )
                conn.execute(
                    "UPDATE user_cd SET type=0,create_time=0,scheduled_time=NULL "
                    "WHERE user_id=%s AND type=3",
                    (user_id,),
                )
                conn.execute(
                    "INSERT INTO rift_settlement_operations("
                    "operation_id,payload,explore_count,message) "
                    "VALUES(%s,%s,%s,%s)",
                    (operation_id, payload, new_count, message),
                )
                conn.commit()
                return RiftSettlementResult("applied", new_count, message)
            except Exception:
                conn.rollback()
                raise
            finally:
                if attached:
                    conn.execute("DETACH DATABASE player_data")


__all__ = ["RiftSettlementResult", "RiftSettlementService"]
