from __future__ import annotations

import json
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from threading import RLock
from typing import Callable

from ..xiuxian_utils import db_backend


@dataclass(frozen=True)
class AdminAccessoryAdjustmentResult:
    status: str
    action: str
    user_id: str
    requested_quantity: int = 0
    affected_quantity: int = 0
    accessories: tuple[dict, ...] = ()

    @property
    def succeeded(self) -> bool:
        return self.status in {"granted", "destroyed", "duplicate"}


class AdminAccessoryAdjustmentService:
    """Atomically adjust one player's accessory bag and admin audit."""

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
            "CREATE TABLE IF NOT EXISTS admin_accessory_operations("
            "operation_id TEXT PRIMARY KEY,action TEXT NOT NULL,payload TEXT NOT NULL,"
            "result_json TEXT NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
        )
        conn.execute(
            "CREATE TABLE IF NOT EXISTS economy_log("
            "id INTEGER PRIMARY KEY AUTOINCREMENT,user_id TEXT,sect_id INTEGER,"
            "source TEXT NOT NULL,action TEXT NOT NULL,stone_delta INTEGER NOT NULL DEFAULT 0,"
            "exp_delta INTEGER NOT NULL DEFAULT 0,sect_contribution_delta INTEGER NOT NULL DEFAULT 0,"
            "sect_scale_delta INTEGER NOT NULL DEFAULT 0,sect_materials_delta INTEGER NOT NULL DEFAULT 0,"
            "item_delta TEXT NOT NULL DEFAULT '[]',detail TEXT NOT NULL DEFAULT '{}',"
            "trace_id TEXT,created_at TEXT NOT NULL)"
        )
        conn.execute(
            "CREATE TABLE IF NOT EXISTS player_data.player_accessory("
            "user_id TEXT PRIMARY KEY,equipped TEXT,bag TEXT)"
        )
        columns = {
            str(row[1])
            for row in conn.execute(
                "PRAGMA player_data.table_info(player_accessory)"
            ).fetchall()
        }
        for field_name in ("equipped", "bag"):
            if field_name not in columns:
                conn.execute(
                    "ALTER TABLE player_data.player_accessory "
                    f"ADD COLUMN {field_name} TEXT"
                )

    @staticmethod
    def _json(value) -> str:
        return json.dumps(
            value, ensure_ascii=False, sort_keys=True, separators=(",", ":")
        )

    @staticmethod
    def _load_dict(value) -> dict:
        if not value:
            return {}
        decoded = json.loads(value) if isinstance(value, str) else value
        return decoded if isinstance(decoded, dict) else {}

    @staticmethod
    def _load_list(value) -> list:
        if not value:
            return []
        decoded = json.loads(value) if isinstance(value, str) else value
        return decoded if isinstance(decoded, list) else []

    @classmethod
    def _result_from_json(cls, status: str, value: str):
        result = json.loads(value)
        return AdminAccessoryAdjustmentResult(
            status,
            str(result["action"]),
            str(result["user_id"]),
            int(result["requested_quantity"]),
            int(result["affected_quantity"]),
            tuple(result.get("accessories", [])),
        )

    @staticmethod
    def _load_accessories(conn, user_id: str) -> tuple[dict, list]:
        row = conn.execute(
            "SELECT equipped,bag FROM player_data.player_accessory WHERE user_id=%s",
            (user_id,),
        ).fetchone()
        if row is None:
            return {}, []
        return (
            AdminAccessoryAdjustmentService._load_dict(row[0]),
            AdminAccessoryAdjustmentService._load_list(row[1]),
        )

    @staticmethod
    def _save_accessories(conn, user_id: str, equipped: dict, bag: list) -> None:
        conn.execute(
            "INSERT INTO player_data.player_accessory(user_id,equipped,bag) "
            "VALUES(%s,%s,%s) ON CONFLICT(user_id) DO UPDATE SET "
            "equipped=EXCLUDED.equipped,bag=EXCLUDED.bag",
            (
                user_id,
                json.dumps(equipped, ensure_ascii=False),
                json.dumps(bag, ensure_ascii=False),
            ),
        )

    @staticmethod
    def _owned_count(equipped: dict, bag: list) -> int:
        return len(bag) + sum(1 for item in equipped.values() if item)

    @staticmethod
    def _owned_uids(equipped: dict, bag: list) -> set[str] | None:
        accessories = list(bag) + [item for item in equipped.values() if item]
        if any(not isinstance(item, dict) for item in accessories):
            return None
        try:
            if any(
                int(item.get("item_id", 0)) <= 0
                or int(item.get("quality", 0)) not in {1, 2, 3, 4, 5}
                or not str(item.get("name", "")).strip()
                for item in accessories
            ):
                return None
        except (TypeError, ValueError):
            return None
        uids = [str(item.get("uid", "")).strip() for item in accessories]
        if any(not uid for uid in uids) or len(set(uids)) != len(uids):
            return None
        return set(uids)

    @staticmethod
    def _audit(
        conn,
        operation_id: str,
        action: str,
        operator_id: str,
        user_id: str,
        target_name: str,
        item_id: int,
        item_name: str,
        requested_quantity: int,
        accessories: list[dict],
    ) -> None:
        amount = len(accessories) if action == "grant" else -len(accessories)
        item_delta = json.dumps(
            [{"id": item_id, "name": item_name, "type": "饰品", "amount": amount}],
            ensure_ascii=False,
            separators=(",", ":"),
        )
        detail = json.dumps(
            {
                "operator_id": operator_id,
                "target_name": target_name,
                "requested_quantity": requested_quantity,
                "accessory_uids": [str(item.get("uid", "")) for item in accessories],
                "qualities": [int(item.get("quality", 1)) for item in accessories],
            },
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        conn.execute(
            "INSERT INTO economy_log(user_id,source,action,item_delta,detail,trace_id,created_at) "
            "VALUES(%s,'admin',%s,%s,%s,%s,CURRENT_TIMESTAMP)",
            (
                user_id,
                "admin_accessory_add" if action == "grant" else "admin_accessory_cost",
                item_delta,
                detail,
                operation_id,
            ),
        )

    def snapshot(self, user_id) -> tuple[dict, list]:
        user_id = str(user_id).strip()
        if not user_id:
            raise ValueError("user id is required")
        with self._lock, closing(db_backend.connect(self._player_database)) as conn:
            if not conn.table_exists("player_accessory"):
                return {}, []
            columns = set(conn.column_names("player_accessory"))
            if not {"equipped", "bag"}.issubset(columns):
                return {}, []
            row = conn.execute(
                "SELECT equipped,bag FROM player_accessory WHERE user_id=%s",
                (user_id,),
            ).fetchone()
            if row is None:
                return {}, []
            return self._load_dict(row[0]), self._load_list(row[1])

    def replay(
        self, operation_id, action
    ) -> AdminAccessoryAdjustmentResult | None:
        operation_id = str(operation_id).strip()
        action = str(action)
        if not operation_id:
            raise ValueError("operation id is required")
        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            if not conn.table_exists("admin_accessory_operations"):
                return None
            row = conn.execute(
                "SELECT action,result_json FROM admin_accessory_operations "
                "WHERE operation_id=%s",
                (operation_id,),
            ).fetchone()
            if row is None or str(row[0]) != action:
                return None
            return self._result_from_json("duplicate", str(row[1]))

    def _run(self, operation_id, action, payload, apply):
        operation_id = str(operation_id).strip()
        payload_json = self._json(payload)
        if not operation_id:
            raise ValueError("operation id is required")
        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            conn.execute("ATTACH DATABASE %s AS player_data", (str(self._player_database),))
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_schema(conn)
                previous = conn.execute(
                    "SELECT action,payload,result_json FROM admin_accessory_operations "
                    "WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    if str(previous[0]) != action or str(previous[1]) != payload_json:
                        return AdminAccessoryAdjustmentResult(
                            "operation_conflict", action, str(payload.get("user_id", ""))
                        )
                    return self._result_from_json("duplicate", str(previous[2]))

                result = apply(conn)
                if result.status not in {"granted", "destroyed"}:
                    conn.rollback()
                    return result
                result_json = self._json(
                    {
                        "action": result.action,
                        "user_id": result.user_id,
                        "requested_quantity": result.requested_quantity,
                        "affected_quantity": result.affected_quantity,
                        "accessories": result.accessories,
                    }
                )
                conn.execute(
                    "INSERT INTO admin_accessory_operations("
                    "operation_id,action,payload,result_json) VALUES(%s,%s,%s,%s)",
                    (operation_id, action, payload_json, result_json),
                )
                conn.commit()
                return result
            except Exception:
                conn.rollback()
                raise
            finally:
                conn.execute("DETACH DATABASE player_data")

    def grant(
        self,
        operation_id,
        operator_id,
        user_id,
        item_id,
        item_name,
        quality,
        quantity,
        expected_equipped,
        expected_bag,
        max_accessories,
        create_accessory: Callable[[], dict],
        *,
        target_name="",
    ) -> AdminAccessoryAdjustmentResult:
        operator_id = str(operator_id).strip()
        user_id = str(user_id).strip()
        item_id = int(item_id)
        item_name = str(item_name)
        quality = int(quality)
        quantity = int(quantity)
        max_accessories = int(max_accessories)
        target_name = str(target_name)
        if not operator_id or not user_id or item_id <= 0:
            raise ValueError("operator, user and item are required")
        if quality not in {1, 2, 3, 4, 5} or quantity <= 0 or max_accessories <= 0:
            raise ValueError("valid quality, quantity and inventory limit are required")
        expected_equipped = json.loads(self._json(expected_equipped))
        expected_bag = json.loads(self._json(expected_bag))
        payload = {
            "operator_id": operator_id,
            "user_id": user_id,
            "item_id": item_id,
            "item_name": item_name,
            "quality": quality,
            "quantity": quantity,
            "max_accessories": max_accessories,
            "target_name": target_name,
        }

        def apply(conn):
            if conn.execute(
                "SELECT 1 FROM user_xiuxian WHERE user_id=%s", (user_id,)
            ).fetchone() is None:
                return AdminAccessoryAdjustmentResult(
                    "user_missing", "grant", user_id, quantity
                )
            equipped, bag = self._load_accessories(conn, user_id)
            if (
                self._json(equipped) != self._json(expected_equipped)
                or self._json(bag) != self._json(expected_bag)
            ):
                return AdminAccessoryAdjustmentResult(
                    "state_changed", "grant", user_id, quantity
                )
            known_uids = self._owned_uids(equipped, bag)
            if known_uids is None:
                return AdminAccessoryAdjustmentResult(
                    "invalid_state", "grant", user_id, quantity
                )
            if self._owned_count(equipped, bag) + quantity > max_accessories:
                return AdminAccessoryAdjustmentResult(
                    "inventory_full", "grant", user_id, quantity
                )

            generated = []
            for _ in range(quantity):
                accessory = create_accessory()
                if not isinstance(accessory, dict):
                    return AdminAccessoryAdjustmentResult(
                        "invalid_plan", "grant", user_id, quantity
                    )
                uid = str(accessory.get("uid", "")).strip()
                try:
                    matches_plan = (
                        uid
                        and uid not in known_uids
                        and int(accessory.get("item_id", 0)) == item_id
                        and int(accessory.get("quality", 0)) == quality
                        and str(accessory.get("name", "")) == item_name
                    )
                except (TypeError, ValueError):
                    matches_plan = False
                if not matches_plan:
                    return AdminAccessoryAdjustmentResult(
                        "invalid_plan", "grant", user_id, quantity
                    )
                known_uids.add(uid)
                generated.append(json.loads(self._json(accessory)))

            bag.extend(generated)
            self._save_accessories(conn, user_id, equipped, bag)
            self._audit(
                conn,
                str(operation_id),
                "grant",
                operator_id,
                user_id,
                target_name,
                item_id,
                item_name,
                quantity,
                generated,
            )
            return AdminAccessoryAdjustmentResult(
                "granted", "grant", user_id, quantity, quantity, tuple(generated)
            )

        return self._run(operation_id, "grant", payload, apply)

    def destroy(
        self,
        operation_id,
        operator_id,
        user_id,
        item_id,
        item_name,
        quantity,
        expected_equipped,
        expected_bag,
        *,
        target_name="",
    ) -> AdminAccessoryAdjustmentResult:
        operator_id = str(operator_id).strip()
        user_id = str(user_id).strip()
        item_id = int(item_id)
        item_name = str(item_name)
        quantity = int(quantity)
        target_name = str(target_name)
        if not operator_id or not user_id or item_id <= 0 or quantity <= 0:
            raise ValueError("operator, user, item and positive quantity are required")
        expected_equipped = json.loads(self._json(expected_equipped))
        expected_bag = json.loads(self._json(expected_bag))
        payload = {
            "operator_id": operator_id,
            "user_id": user_id,
            "item_id": item_id,
            "item_name": item_name,
            "quantity": quantity,
            "target_name": target_name,
        }

        def apply(conn):
            if conn.execute(
                "SELECT 1 FROM user_xiuxian WHERE user_id=%s", (user_id,)
            ).fetchone() is None:
                return AdminAccessoryAdjustmentResult(
                    "user_missing", "destroy", user_id, quantity
                )
            equipped, bag = self._load_accessories(conn, user_id)
            if (
                self._json(equipped) != self._json(expected_equipped)
                or self._json(bag) != self._json(expected_bag)
            ):
                return AdminAccessoryAdjustmentResult(
                    "state_changed", "destroy", user_id, quantity
                )
            if self._owned_uids(equipped, bag) is None:
                return AdminAccessoryAdjustmentResult(
                    "invalid_state", "destroy", user_id, quantity
                )

            removed = []
            kept = []
            for accessory in bag:
                if (
                    len(removed) < quantity
                    and int(accessory.get("item_id", 0)) == item_id
                ):
                    removed.append(accessory)
                else:
                    kept.append(accessory)
            if not removed:
                return AdminAccessoryAdjustmentResult(
                    "item_missing", "destroy", user_id, quantity
                )

            self._save_accessories(conn, user_id, equipped, kept)
            self._audit(
                conn,
                str(operation_id),
                "destroy",
                operator_id,
                user_id,
                target_name,
                item_id,
                item_name,
                quantity,
                removed,
            )
            return AdminAccessoryAdjustmentResult(
                "destroyed",
                "destroy",
                user_id,
                quantity,
                len(removed),
                tuple(removed),
            )

        return self._run(operation_id, "destroy", payload, apply)


__all__ = ["AdminAccessoryAdjustmentResult", "AdminAccessoryAdjustmentService"]
