from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from ...infrastructure.database import DatabaseUnitOfWork
from ...infrastructure.database.attached_uow import AttachedDatabaseUnitOfWork
from .accessory_affix_domain import AccessoryAffixChange


class AccessoryAffixSqlRepository:
    """Persist affix-lock changes with the game operation receipt atomically."""

    def __init__(self, game_database: str | Path, player_database: str | Path) -> None:
        self.game_database = str(game_database)
        self.player_database = str(player_database)

    @staticmethod
    def _json(value: Any) -> str:
        return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))

    @staticmethod
    def _result(action: str, user_id: str, status: str, result_json: str) -> AccessoryAffixChange:
        value = json.loads(result_json)
        return AccessoryAffixChange(
            status=status,
            action=str(value.get("action", action)),
            user_id=str(value.get("user_id", user_id)),
            accessory=value.get("accessory"),
        )

    @classmethod
    def _decode_dict(cls, value: Any) -> dict[str, Any]:
        if not value:
            return {}
        decoded = json.loads(value) if isinstance(value, str) else value
        return decoded if isinstance(decoded, dict) else {}

    @classmethod
    def _decode_list(cls, value: Any) -> list[Any]:
        if not value:
            return []
        decoded = json.loads(value) if isinstance(value, str) else value
        return decoded if isinstance(decoded, list) else []

    @classmethod
    def _find(cls, equipped: dict[str, Any], bag: list[Any], uid: str):
        for index, item in enumerate(bag):
            if isinstance(item, dict) and str(item.get("uid", "")) == uid:
                return "bag", index, item
        for slot, item in equipped.items():
            if isinstance(item, dict) and str(item.get("uid", "")) == uid:
                return "equipped", slot, item
        return None, None, None

    def replay(self, operation_id: str, action: str) -> AccessoryAffixChange | None:
        operation_id = str(operation_id).strip()
        if not operation_id:
            raise ValueError("operation_id must not be empty")
        with DatabaseUnitOfWork(self.game_database, read_only=True) as uow:
            row = uow.query_one(
                "SELECT action, result_json FROM accessory_transaction_operations "
                "WHERE operation_id=?",
                (operation_id,),
            )
            if row is None or str(row["action"]) != str(action):
                return None
            return self._result(str(action), "", "duplicate", str(row["result_json"]))

    def set_locks(
        self,
        operation_id: str,
        action: str,
        user_id: str,
        uid: str,
        expected_accessory: dict[str, Any],
        accessory: dict[str, Any],
    ) -> AccessoryAffixChange:
        operation_id = str(operation_id).strip()
        user_id = str(user_id)
        uid = str(uid)
        if not operation_id:
            raise ValueError("operation_id must not be empty")
        if action not in {"lock", "unlock"}:
            raise ValueError("action must be lock or unlock")

        locked_indexes = sorted(int(index) for index in accessory.get("locked_affixes", ()))
        payload = {
            "user_id": user_id,
            "uid": uid,
            "expected_accessory": expected_accessory,
            "locked_indexes": locked_indexes,
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
                if str(previous["action"]) != action or str(previous["payload"]) != payload_json:
                    return AccessoryAffixChange("state_changed", action, user_id)
                return self._result(action, user_id, "duplicate", str(previous["result_json"]))

            row = uow.query_one(
                "SELECT equipped,bag FROM player_data.player_accessory WHERE user_id=?",
                (user_id,),
            )
            if row is None:
                return AccessoryAffixChange("accessory_missing", action, user_id)
            equipped = self._decode_dict(row["equipped"])
            bag = self._decode_list(row["bag"])
            where, key, current = self._find(equipped, bag, uid)
            if current is None:
                return AccessoryAffixChange("accessory_missing", action, user_id)
            if self._json(current) != self._json(expected_accessory):
                return AccessoryAffixChange("state_changed", action, user_id)

            if where == "bag":
                bag[key] = accessory
            else:
                equipped[key] = accessory
            uow.execute(
                "UPDATE player_data.player_accessory SET equipped=?,bag=? WHERE user_id=?",
                (
                    json.dumps(equipped, ensure_ascii=False),
                    json.dumps(bag, ensure_ascii=False),
                    user_id,
                ),
            )
            result_json = self._json(
                {
                    "action": action,
                    "user_id": user_id,
                    "affected": 1,
                    "stone_delta": 0,
                    "accessory": accessory,
                    "details": None,
                }
            )
            uow.execute(
                "INSERT INTO accessory_transaction_operations "
                "(operation_id,action,payload,result_json) VALUES(?,?,?,?)",
                (operation_id, action, payload_json, result_json),
            )
            return AccessoryAffixChange("applied", action, user_id, accessory)


__all__ = ["AccessoryAffixSqlRepository"]
