from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from ...infrastructure.database import DatabaseUnitOfWork
from ...infrastructure.database.attached_uow import AttachedDatabaseUnitOfWork
from .accessory_decompose_domain import AccessoryDecomposeChange


class AccessoryDecomposeSqlRepository:
    """Atomically exchange one bag accessory for wash stones."""

    def __init__(self, game_database: str | Path, player_database: str | Path) -> None:
        self.game_database = str(game_database)
        self.player_database = str(player_database)

    @staticmethod
    def _json(value: Any) -> str:
        return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))

    @staticmethod
    def _decode(value: Any, expected_type: type) -> Any:
        if not value:
            return expected_type()
        decoded = json.loads(value) if isinstance(value, str) else value
        return decoded if isinstance(decoded, expected_type) else expected_type()

    @classmethod
    def _find_bag(cls, bag: list[Any], uid: str) -> tuple[int | None, dict[str, Any] | None]:
        for index, item in enumerate(bag):
            if isinstance(item, dict) and str(item.get("uid", "")) == uid:
                return index, item
        return None, None

    @classmethod
    def _result(cls, status: str, result_json: str) -> AccessoryDecomposeChange:
        value = json.loads(result_json)
        return AccessoryDecomposeChange(
            status=status,
            action=str(value.get("action", "decompose")),
            user_id=str(value.get("user_id", "")),
            affected=int(value.get("affected", 0)),
            stone_delta=int(value.get("stone_delta", 0)),
            accessory=value.get("accessory"),
        )

    def replay(self, operation_id: str) -> AccessoryDecomposeChange | None:
        operation_id = str(operation_id).strip()
        if not operation_id:
            raise ValueError("operation_id must not be empty")
        with DatabaseUnitOfWork(self.game_database, read_only=True) as uow:
            row = uow.query_one(
                "SELECT action,result_json FROM accessory_transaction_operations "
                "WHERE operation_id=?",
                (operation_id,),
            )
            if row is None or str(row["action"]) != "decompose":
                return None
            return self._result("duplicate", str(row["result_json"]))

    @staticmethod
    def _stone_quantity(uow: AttachedDatabaseUnitOfWork, user_id: str, stone_id: int) -> int:
        row = uow.query_one(
            "SELECT goods_num FROM back WHERE user_id=? AND goods_id=?",
            (user_id, stone_id),
        )
        return int(row["goods_num"] or 0) if row else 0

    @classmethod
    def _add_stones(
        cls,
        uow: AttachedDatabaseUnitOfWork,
        user_id: str,
        stone_id: int,
        stone_name: str,
        amount: int,
        max_goods_num: int,
    ) -> bool:
        current = cls._stone_quantity(uow, user_id, stone_id)
        if current + amount > max_goods_num:
            return False
        uow.execute(
            "INSERT INTO back (user_id,goods_id,goods_name,goods_type,goods_num,bind_num) "
            "VALUES (?,?,?,?,?,?) ON CONFLICT(user_id,goods_id) DO UPDATE SET "
            "goods_name=excluded.goods_name,goods_type=excluded.goods_type, "
            "goods_num=COALESCE(back.goods_num,0)+excluded.goods_num, "
            "bind_num=MIN(COALESCE(back.bind_num,0)+excluded.goods_num, "
            "COALESCE(back.goods_num,0)+excluded.goods_num)",
            (user_id, stone_id, stone_name, "特殊道具", amount, amount),
        )
        return True

    def decompose(
        self,
        operation_id: str,
        user_id: str,
        uid: str,
        expected_accessory: dict[str, Any],
        stone_id: int,
        stone_name: str,
        stone_gain: int,
        max_goods_num: int,
    ) -> AccessoryDecomposeChange:
        operation_id = str(operation_id).strip()
        user_id = str(user_id)
        uid = str(uid)
        stone_id = int(stone_id)
        stone_gain = int(stone_gain)
        max_goods_num = int(max_goods_num)
        if not operation_id:
            raise ValueError("operation_id must not be empty")
        if stone_gain <= 0 or max_goods_num <= 0:
            raise ValueError("stone_gain and max_goods_num must be positive")

        payload = {
            "user_id": user_id,
            "uid": uid,
            "expected_accessory": expected_accessory,
            "stone_id": stone_id,
            "stone_gain": stone_gain,
            "max_goods_num": max_goods_num,
        }
        payload_json = self._json(payload)
        with AttachedDatabaseUnitOfWork(
            self.game_database,
            attachments={"player_data": self.player_database},
            immediate=True,
        ) as uow:
            previous = uow.query_one(
                "SELECT action,payload,result_json FROM accessory_transaction_operations "
                "WHERE operation_id=?",
                (operation_id,),
            )
            if previous is not None:
                if str(previous["action"]) != "decompose" or str(previous["payload"]) != payload_json:
                    return AccessoryDecomposeChange("state_changed", "decompose", user_id)
                return self._result("duplicate", str(previous["result_json"]))

            row = uow.query_one(
                "SELECT equipped,bag FROM player_data.player_accessory WHERE user_id=?",
                (user_id,),
            )
            if row is None:
                return AccessoryDecomposeChange("accessory_missing", "decompose", user_id)
            equipped = self._decode(row["equipped"], dict)
            bag = self._decode(row["bag"], list)
            index, current = self._find_bag(bag, uid)
            if index is None or current is None:
                return AccessoryDecomposeChange("accessory_missing", "decompose", user_id)
            if self._json(current) != self._json(expected_accessory):
                return AccessoryDecomposeChange("state_changed", "decompose", user_id)
            if not self._add_stones(
                uow, user_id, stone_id, stone_name, stone_gain, max_goods_num
            ):
                return AccessoryDecomposeChange("inventory_full", "decompose", user_id)

            del bag[index]
            uow.execute(
                "UPDATE player_data.player_accessory SET equipped=?,bag=? WHERE user_id=?",
                (json.dumps(equipped, ensure_ascii=False), json.dumps(bag, ensure_ascii=False), user_id),
            )
            result_json = self._json(
                {
                    "action": "decompose",
                    "user_id": user_id,
                    "affected": 1,
                    "stone_delta": stone_gain,
                    "accessory": current,
                    "details": None,
                }
            )
            uow.execute(
                "INSERT INTO accessory_transaction_operations "
                "(operation_id,action,payload,result_json) VALUES(?,?,?,?)",
                (operation_id, "decompose", payload_json, result_json),
            )
            return AccessoryDecomposeChange(
                "applied", "decompose", user_id, 1, stone_gain, current
            )


__all__ = ["AccessoryDecomposeSqlRepository"]
