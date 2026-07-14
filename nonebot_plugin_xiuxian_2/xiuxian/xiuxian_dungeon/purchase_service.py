from __future__ import annotations

import json
from contextlib import closing
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from threading import RLock

from ..xiuxian_utils import db_backend
from ..xiuxian_utils.utils import number_to


@dataclass(frozen=True)
class DungeonPurchaseResult:
    status: str
    quantity: int = 0
    cost: int = 0
    stone: int = 0
    inventory: int = 0
    response: str = ""

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


class DungeonPurchaseService:
    """Exchange spirit stones for a dungeon-shop item atomically."""

    _REJECTION_RESPONSES = {
        "stone_insufficient": "灵石不足，无法兑换。",
        "inventory_full": "背包中该物品数量已达上限。",
        "state_changed": "兑换状态已变化，请稍后重试。",
        "user_missing": "未找到道友数据，兑换失败。",
    }

    def __init__(self, game_database: str | Path, lock: RLock | None = None) -> None:
        self._database = Path(game_database)
        self._lock = lock or RLock()

    @staticmethod
    def _payload(user_id: str, item_id: int, quantity: int, bind_flag: int) -> str:
        return json.dumps(
            [user_id, item_id, quantity, bind_flag],
            ensure_ascii=True,
            separators=(",", ":"),
        )

    @classmethod
    def _response(cls, status: str, item_name: str, quantity: int, cost: int) -> str:
        if status == "applied":
            return f"成功兑换{item_name}×{quantity}，消耗{number_to(cost)}灵石。"
        return cls._REJECTION_RESPONSES.get(status, "兑换失败。")

    @classmethod
    def _legacy_payload(cls, payload: str) -> tuple[str, str]:
        """Return the immutable request identity and legacy item name."""
        try:
            values = json.loads(payload)
        except (TypeError, ValueError):
            return str(payload), ""
        if not isinstance(values, list):
            return str(payload), ""
        if len(values) == 8:
            try:
                return (
                    cls._payload(
                        str(values[0]),
                        int(values[1]),
                        int(values[4]),
                        int(bool(values[7])),
                    ),
                    str(values[2]),
                )
            except (TypeError, ValueError):
                return str(payload), ""
        if len(values) == 5:
            try:
                return (
                    cls._payload(
                        str(values[0]),
                        int(values[1]),
                        int(values[2]),
                        int(bool(values[4])),
                    ),
                    "",
                )
            except (TypeError, ValueError):
                pass
        if len(values) == 4:
            try:
                return (
                    cls._payload(
                        str(values[0]),
                        int(values[1]),
                        int(values[2]),
                        int(bool(values[3])),
                    ),
                    "",
                )
            except (TypeError, ValueError):
                pass
        return str(payload), ""

    @classmethod
    def _ensure_schema(cls, conn) -> None:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS dungeon_purchase_operations("
            "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,"
            "result_status TEXT NOT NULL DEFAULT 'applied',"
            "quantity INTEGER NOT NULL DEFAULT 0,cost INTEGER NOT NULL DEFAULT 0,"
            "stone INTEGER NOT NULL DEFAULT 0,inventory INTEGER NOT NULL DEFAULT 0,"
            "response TEXT NOT NULL DEFAULT '',"
            "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
        )
        for name, definition in (
            ("result_status", "TEXT NOT NULL DEFAULT 'applied'"),
            ("response", "TEXT NOT NULL DEFAULT ''"),
        ):
            if not conn.column_exists("dungeon_purchase_operations", name):
                conn.execute(
                    f"ALTER TABLE dungeon_purchase_operations ADD COLUMN {name} {definition}"
                )

        rows = conn.execute(
            "SELECT operation_id,payload,result_status,quantity,cost,response "
            "FROM dungeon_purchase_operations"
        ).fetchall()
        for row in rows:
            payload, item_name = cls._legacy_payload(str(row[1]))
            result_status = str(row[2] or "applied")
            response = str(row[5] or "")
            if not response:
                response = cls._response(
                    result_status, item_name or "该物品", int(row[3]), int(row[4])
                )
            if payload != str(row[1]) or result_status != str(row[2]) or response != str(row[5]):
                conn.execute(
                    "UPDATE dungeon_purchase_operations "
                    "SET payload=%s,result_status=%s,response=%s WHERE operation_id=%s",
                    (payload, result_status, response, str(row[0])),
                )

    @staticmethod
    def _stored_result(row) -> DungeonPurchaseResult:
        result_status = str(row[1])
        return DungeonPurchaseResult(
            "duplicate" if result_status == "applied" else result_status,
            int(row[2]),
            int(row[3]),
            int(row[4]),
            int(row[5]),
            str(row[6]),
        )

    @staticmethod
    def _record(conn, operation_id: str, payload: str, result: DungeonPurchaseResult) -> None:
        conn.execute(
            "INSERT INTO dungeon_purchase_operations("
            "operation_id,payload,result_status,quantity,cost,stone,inventory,response) "
            "VALUES(%s,%s,%s,%s,%s,%s,%s,%s)",
            (
                operation_id,
                payload,
                result.status,
                result.quantity,
                result.cost,
                result.stone,
                result.inventory,
                result.response,
            ),
        )

    def operation_result(
        self,
        operation_id,
        user_id,
        item_id,
        quantity,
        bind_flag=1,
    ) -> DungeonPurchaseResult | None:
        """Replay a fixed purchase before consulting mutable shop metadata."""

        operation_id = str(operation_id).strip()
        user_id = str(user_id).strip()
        item_id, quantity, bind_flag = map(int, (item_id, quantity, bind_flag))
        if (
            not operation_id
            or not user_id
            or item_id <= 0
            or quantity <= 0
            or bind_flag not in {0, 1}
        ):
            raise ValueError("valid purchase identity is required")
        payload = self._payload(user_id, item_id, quantity, bind_flag)
        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_schema(conn)
                previous = conn.execute(
                    "SELECT payload,result_status,quantity,cost,stone,inventory,response "
                    "FROM dungeon_purchase_operations WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                conn.commit()
            except Exception:
                conn.rollback()
                raise
        if previous is None:
            return None
        if str(previous[0]) != payload:
            return DungeonPurchaseResult(
                "state_changed",
                quantity=quantity,
                response=self._REJECTION_RESPONSES["state_changed"],
            )
        return self._stored_result(previous)

    def purchase(
        self,
        operation_id,
        user_id,
        item_id,
        item_name,
        item_type,
        quantity,
        unit_cost,
        expected_stone,
        max_goods,
        bind_flag=1,
    ) -> DungeonPurchaseResult:
        operation_id = str(operation_id).strip()
        user_id = str(user_id).strip()
        item_id, quantity, unit_cost, expected_stone, max_goods = map(
            int, (item_id, quantity, unit_cost, expected_stone, max_goods)
        )
        item_name, item_type = str(item_name).strip(), str(item_type).strip()
        bind_flag = int(bind_flag)
        if (
            not operation_id
            or not user_id
            or not item_name
            or not item_type
            or item_id <= 0
            or quantity <= 0
            or unit_cost <= 0
            or expected_stone < 0
            or max_goods < 0
            or bind_flag not in {0, 1}
        ):
            raise ValueError("valid purchase is required")
        payload = self._payload(user_id, item_id, quantity, bind_flag)
        cost = quantity * unit_cost

        def rejected(status: str, stone=0, inventory=0) -> DungeonPurchaseResult:
            return DungeonPurchaseResult(
                status,
                quantity,
                cost,
                int(stone),
                int(inventory),
                self._response(status, item_name, quantity, cost),
            )

        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_schema(conn)
                previous = conn.execute(
                    "SELECT payload,result_status,quantity,cost,stone,inventory,response "
                    "FROM dungeon_purchase_operations WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.commit()
                    if str(previous[0]) != payload:
                        return rejected("state_changed")
                    return self._stored_result(previous)

                user = conn.execute(
                    "SELECT COALESCE(stone,0) FROM user_xiuxian WHERE user_id=%s",
                    (user_id,),
                ).fetchone()
                if user is None:
                    result = rejected("user_missing")
                    self._record(conn, operation_id, payload, result)
                    conn.commit()
                    return result
                stone = int(user[0])
                if stone != expected_stone:
                    result = rejected("state_changed", stone)
                    self._record(conn, operation_id, payload, result)
                    conn.commit()
                    return result
                if stone < cost:
                    result = rejected("stone_insufficient", stone)
                    self._record(conn, operation_id, payload, result)
                    conn.commit()
                    return result

                item = conn.execute(
                    "SELECT COALESCE(goods_num,0),COALESCE(bind_num,0) "
                    "FROM back WHERE user_id=%s AND goods_id=%s",
                    (user_id, item_id),
                ).fetchone()
                inventory, bound = (
                    (int(item[0]), int(item[1])) if item is not None else (0, 0)
                )
                if inventory < 0 or bound < 0 or bound > inventory:
                    result = rejected("state_changed", stone, inventory)
                    self._record(conn, operation_id, payload, result)
                    conn.commit()
                    return result
                if inventory + quantity > max_goods:
                    result = rejected("inventory_full", stone, inventory)
                    self._record(conn, operation_id, payload, result)
                    conn.commit()
                    return result

                new_stone = stone - cost
                new_inventory = inventory + quantity
                new_bound = bound + (quantity if bind_flag else 0)
                changed = conn.execute(
                    "UPDATE user_xiuxian SET stone=%s "
                    "WHERE user_id=%s AND COALESCE(stone,0)=%s",
                    (new_stone, user_id, expected_stone),
                )
                if changed.rowcount != 1:
                    result = rejected("state_changed", stone, inventory)
                    self._record(conn, operation_id, payload, result)
                    conn.commit()
                    return result

                now = datetime.now()
                conn.execute(
                    "INSERT INTO back("
                    "user_id,goods_id,goods_name,goods_type,goods_num,"
                    "create_time,update_time,bind_num) "
                    "VALUES(%s,%s,%s,%s,%s,%s,%s,%s) "
                    "ON CONFLICT(user_id,goods_id) DO UPDATE SET "
                    "goods_name=EXCLUDED.goods_name,goods_type=EXCLUDED.goods_type,"
                    "goods_num=back.goods_num+EXCLUDED.goods_num,"
                    "bind_num=COALESCE(back.bind_num,0)+EXCLUDED.bind_num,"
                    "update_time=EXCLUDED.update_time",
                    (
                        user_id,
                        item_id,
                        item_name,
                        item_type,
                        quantity,
                        now,
                        now,
                        quantity if bind_flag else 0,
                    ),
                )
                final_item = conn.execute(
                    "SELECT COALESCE(goods_num,0),COALESCE(bind_num,0) "
                    "FROM back WHERE user_id=%s AND goods_id=%s",
                    (user_id, item_id),
                ).fetchone()
                if final_item is None or (int(final_item[0]), int(final_item[1])) != (
                    new_inventory,
                    new_bound,
                ):
                    raise RuntimeError("dungeon purchase inventory invariant failed")

                result = DungeonPurchaseResult(
                    "applied",
                    quantity,
                    cost,
                    new_stone,
                    new_inventory,
                    self._response("applied", item_name, quantity, cost),
                )
                self._record(conn, operation_id, payload, result)
                conn.commit()
                return result
            except Exception:
                conn.rollback()
                raise


__all__ = ["DungeonPurchaseResult", "DungeonPurchaseService"]
