from __future__ import annotations

import json
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from threading import RLock
from typing import Callable

from ..xiuxian_utils import db_backend


@dataclass(frozen=True)
class AccessoryTransactionResult:
    status: str
    action: str
    user_id: str
    affected: int = 0
    stone_delta: int = 0
    accessory: dict | None = None

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


class AccessoryTransactionService:
    """Apply accessory and wash-stone changes in one attached DB transaction."""

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
            """
            CREATE TABLE IF NOT EXISTS accessory_transaction_operations (
                operation_id TEXT PRIMARY KEY,
                action TEXT NOT NULL,
                payload TEXT NOT NULL,
                result_json TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        conn.execute(
            "CREATE TABLE IF NOT EXISTS player_data.player_accessory "
            "(user_id TEXT PRIMARY KEY, equipped TEXT, bag TEXT)"
        )
        columns = {
            str(row[1])
            for row in conn.execute(
                "PRAGMA player_data.table_info(player_accessory)"
            ).fetchall()
        }
        if "equipped" not in columns:
            conn.execute("ALTER TABLE player_data.player_accessory ADD COLUMN equipped TEXT")
        if "bag" not in columns:
            conn.execute("ALTER TABLE player_data.player_accessory ADD COLUMN bag TEXT")

    @staticmethod
    def _json(value) -> str:
        return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))

    @staticmethod
    def _load_list(value) -> list:
        if not value:
            return []
        decoded = json.loads(value) if isinstance(value, str) else value
        return decoded if isinstance(decoded, list) else []

    @staticmethod
    def _load_dict(value) -> dict:
        if not value:
            return {}
        decoded = json.loads(value) if isinstance(value, str) else value
        return decoded if isinstance(decoded, dict) else {}

    @staticmethod
    def _find(equipped: dict, bag: list, uid: str):
        for index, item in enumerate(bag):
            if str(item.get("uid", "")) == uid:
                return "bag", index, item
        for slot, item in equipped.items():
            if item and str(item.get("uid", "")) == uid:
                return "equipped", slot, item
        return None, None, None

    @staticmethod
    def _result_from_json(status: str, payload: str):
        value = json.loads(payload)
        return AccessoryTransactionResult(
            status=status,
            action=str(value["action"]),
            user_id=str(value["user_id"]),
            affected=int(value.get("affected", 0)),
            stone_delta=int(value.get("stone_delta", 0)),
            accessory=value.get("accessory"),
        )

    def replay(self, operation_id, action) -> AccessoryTransactionResult | None:
        operation_id = str(operation_id).strip()
        if not operation_id:
            raise ValueError("operation_id must not be empty")
        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            if not conn.table_exists("accessory_transaction_operations"):
                return None
            row = conn.execute(
                "SELECT action, result_json FROM accessory_transaction_operations "
                "WHERE operation_id=%s",
                (operation_id,),
            ).fetchone()
            if row is None or str(row[0]) != str(action):
                return None
            return self._result_from_json("duplicate", str(row[1]))

    def _run(self, operation_id, action, payload, apply: Callable):
        operation_id = str(operation_id).strip()
        if not operation_id:
            raise ValueError("operation_id must not be empty")
        payload_json = self._json(payload)

        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            conn.execute("ATTACH DATABASE %s AS player_data", (str(self._player_database),))
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_schema(conn)
                previous = conn.execute(
                    "SELECT action, payload, result_json "
                    "FROM accessory_transaction_operations WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    if str(previous[0]) != action or str(previous[1]) != payload_json:
                        return AccessoryTransactionResult(
                            "state_changed", action, str(payload.get("user_id", ""))
                        )
                    return self._result_from_json("duplicate", str(previous[2]))

                result = apply(conn)
                if result.status != "applied":
                    conn.rollback()
                    return result
                result_json = self._json(
                    {
                        "action": result.action,
                        "user_id": result.user_id,
                        "affected": result.affected,
                        "stone_delta": result.stone_delta,
                        "accessory": result.accessory,
                    }
                )
                conn.execute(
                    "INSERT INTO accessory_transaction_operations "
                    "(operation_id, action, payload, result_json) VALUES (%s, %s, %s, %s)",
                    (operation_id, action, payload_json, result_json),
                )
                conn.commit()
                return result
            except Exception:
                conn.rollback()
                raise
            finally:
                conn.execute("DETACH DATABASE player_data")

    def _load_accessories(self, conn, user_id: str):
        row = conn.execute(
            "SELECT equipped, bag FROM player_data.player_accessory WHERE user_id=%s",
            (user_id,),
        ).fetchone()
        if row is None:
            return {}, []
        return self._load_dict(row[0]), self._load_list(row[1])

    @staticmethod
    def _save_accessories(conn, user_id: str, equipped: dict, bag: list) -> None:
        conn.execute(
            "INSERT INTO player_data.player_accessory (user_id, equipped, bag) "
            "VALUES (%s, %s, %s) ON CONFLICT (user_id) DO UPDATE SET "
            "equipped=EXCLUDED.equipped, bag=EXCLUDED.bag",
            (
                user_id,
                json.dumps(equipped, ensure_ascii=False),
                json.dumps(bag, ensure_ascii=False),
            ),
        )

    @staticmethod
    def _stone_quantity(conn, user_id: str, stone_id: int) -> int:
        row = conn.execute(
            "SELECT goods_num FROM back WHERE user_id=%s AND goods_id=%s",
            (user_id, stone_id),
        ).fetchone()
        return int(row[0] or 0) if row else 0

    @staticmethod
    def _add_stones(conn, user_id, stone_id, stone_name, amount, max_goods_num):
        current = AccessoryTransactionService._stone_quantity(conn, user_id, stone_id)
        if current + amount > max_goods_num:
            return False
        conn.execute(
            "INSERT INTO back (user_id, goods_id, goods_name, goods_type, goods_num, bind_num) "
            "VALUES (%s, %s, %s, %s, %s, %s) "
            "ON CONFLICT (user_id, goods_id) DO UPDATE SET "
            "goods_name=EXCLUDED.goods_name, goods_type=EXCLUDED.goods_type, "
            "goods_num=COALESCE(back.goods_num, 0)+EXCLUDED.goods_num, "
            "bind_num=MIN(COALESCE(back.bind_num, 0)+EXCLUDED.goods_num, "
            "COALESCE(back.goods_num, 0)+EXCLUDED.goods_num)",
            (user_id, stone_id, stone_name, "特殊道具", amount, amount),
        )
        return True

    def wash(
        self,
        operation_id,
        user_id,
        uid,
        expected_accessory,
        expected_stones,
        stone_id,
        stone_cost,
        reroll: Callable[[dict], dict],
    ) -> AccessoryTransactionResult:
        user_id = str(user_id)
        uid = str(uid)
        expected_stones = int(expected_stones)
        stone_id = int(stone_id)
        stone_cost = int(stone_cost)
        payload = {
            "user_id": user_id,
            "uid": uid,
            "expected_accessory": expected_accessory,
            "expected_stones": expected_stones,
            "stone_id": stone_id,
            "stone_cost": stone_cost,
        }

        def apply(conn):
            equipped, bag = self._load_accessories(conn, user_id)
            where, key, current = self._find(equipped, bag, uid)
            if current is None:
                return AccessoryTransactionResult("accessory_missing", "wash", user_id)
            if self._json(current) != self._json(expected_accessory):
                return AccessoryTransactionResult("state_changed", "wash", user_id)
            if self._stone_quantity(conn, user_id, stone_id) != expected_stones:
                return AccessoryTransactionResult("state_changed", "wash", user_id)
            consumed = conn.execute(
                "UPDATE back SET goods_num=goods_num-%s, "
                "bind_num=MIN(COALESCE(bind_num, 0), goods_num-%s) "
                "WHERE user_id=%s AND goods_id=%s AND goods_num>=%s",
                (stone_cost, stone_cost, user_id, stone_id, stone_cost),
            )
            if consumed.rowcount != 1:
                return AccessoryTransactionResult("item_insufficient", "wash", user_id)

            updated = reroll(dict(current))
            if where == "bag":
                bag[key] = updated
            else:
                equipped[key] = updated
            self._save_accessories(conn, user_id, equipped, bag)
            return AccessoryTransactionResult(
                "applied", "wash", user_id, 1, -stone_cost, updated
            )

        return self._run(operation_id, "wash", payload, apply)

    def decompose(
        self,
        operation_id,
        user_id,
        uid,
        expected_accessory,
        stone_id,
        stone_name,
        stone_gain,
        max_goods_num,
    ) -> AccessoryTransactionResult:
        user_id = str(user_id)
        uid = str(uid)
        stone_id = int(stone_id)
        stone_gain = int(stone_gain)
        max_goods_num = int(max_goods_num)
        payload = {
            "user_id": user_id,
            "uid": uid,
            "expected_accessory": expected_accessory,
            "stone_id": stone_id,
            "stone_gain": stone_gain,
            "max_goods_num": max_goods_num,
        }

        def apply(conn):
            equipped, bag = self._load_accessories(conn, user_id)
            where, key, current = self._find(equipped, bag, uid)
            if current is None or where != "bag":
                return AccessoryTransactionResult("accessory_missing", "decompose", user_id)
            if self._json(current) != self._json(expected_accessory):
                return AccessoryTransactionResult("state_changed", "decompose", user_id)
            if not self._add_stones(
                conn, user_id, stone_id, stone_name, stone_gain, max_goods_num
            ):
                return AccessoryTransactionResult("inventory_full", "decompose", user_id)
            del bag[key]
            self._save_accessories(conn, user_id, equipped, bag)
            return AccessoryTransactionResult(
                "applied", "decompose", user_id, 1, stone_gain, current
            )

        return self._run(operation_id, "decompose", payload, apply)

    def batch_decompose(
        self,
        operation_id,
        user_id,
        expected_bag,
        selected_uids,
        stone_id,
        stone_name,
        total_gain,
        max_goods_num,
    ) -> AccessoryTransactionResult:
        user_id = str(user_id)
        selected_uids = tuple(str(uid) for uid in selected_uids)
        stone_id = int(stone_id)
        total_gain = int(total_gain)
        max_goods_num = int(max_goods_num)
        payload = {
            "user_id": user_id,
            "expected_bag": expected_bag,
            "selected_uids": selected_uids,
            "stone_id": stone_id,
            "total_gain": total_gain,
            "max_goods_num": max_goods_num,
        }

        def apply(conn):
            equipped, bag = self._load_accessories(conn, user_id)
            if self._json(bag) != self._json(expected_bag):
                return AccessoryTransactionResult("state_changed", "batch_decompose", user_id)
            selected = set(selected_uids)
            hit = [item for item in bag if str(item.get("uid", "")) in selected]
            if len(hit) != len(selected):
                return AccessoryTransactionResult("accessory_missing", "batch_decompose", user_id)
            if not self._add_stones(
                conn, user_id, stone_id, stone_name, total_gain, max_goods_num
            ):
                return AccessoryTransactionResult("inventory_full", "batch_decompose", user_id)
            bag = [item for item in bag if str(item.get("uid", "")) not in selected]
            self._save_accessories(conn, user_id, equipped, bag)
            return AccessoryTransactionResult(
                "applied", "batch_decompose", user_id, len(hit), total_gain
            )

        return self._run(operation_id, "batch_decompose", payload, apply)


__all__ = ["AccessoryTransactionResult", "AccessoryTransactionService"]
