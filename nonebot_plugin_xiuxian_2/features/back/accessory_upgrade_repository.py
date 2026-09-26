from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from ...infrastructure.database import DatabaseUnitOfWork
from ...infrastructure.database.attached_uow import AttachedDatabaseUnitOfWork
from .accessory_upgrade_domain import AccessoryUpgradeChange, upgrade_material_count


class AccessoryUpgradeSqlRepository:
    """Atomically consume bag materials and upgrade an equipped accessory."""

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

    @staticmethod
    def _signature(accessory: dict[str, Any]) -> tuple[int, str, str, int]:
        return (
            int(accessory.get("item_id", 0)),
            str(accessory.get("part", "")),
            str(accessory.get("set_type", "")),
            int(accessory.get("quality", 1)),
        )

    @classmethod
    def _result(cls, status: str, result_json: str) -> AccessoryUpgradeChange:
        value = json.loads(result_json)
        return AccessoryUpgradeChange(
            status=status,
            action=str(value.get("action", "upgrade")),
            user_id=str(value.get("user_id", "")),
            affected=int(value.get("affected", 0)),
            stone_delta=int(value.get("stone_delta", 0)),
            accessory=value.get("accessory"),
        )

    def replay(self, operation_id: str) -> AccessoryUpgradeChange | None:
        operation_id = str(operation_id).strip()
        if not operation_id:
            raise ValueError("operation_id must not be empty")
        with DatabaseUnitOfWork(self.game_database, read_only=True) as uow:
            row = uow.query_one(
                "SELECT action,result_json FROM accessory_transaction_operations "
                "WHERE operation_id=?",
                (operation_id,),
            )
            if row is None or str(row["action"]) != "upgrade":
                return None
            return self._result("duplicate", str(row["result_json"]))

    def upgrade(
        self,
        operation_id: str,
        user_id: str,
        part: str,
        expected_equipped: dict[str, Any],
        expected_bag: list[dict[str, Any]],
        material_uids: tuple[str, ...],
        upgraded_accessory: dict[str, Any],
    ) -> AccessoryUpgradeChange:
        operation_id = str(operation_id).strip()
        user_id = str(user_id)
        part = str(part)
        material_uids = tuple(str(uid).strip() for uid in material_uids)
        if not operation_id:
            raise ValueError("operation_id must not be empty")
        if not part or not material_uids or len(set(material_uids)) != len(material_uids):
            raise ValueError("part and unique material uids are required")

        payload = {
            "user_id": user_id,
            "part": part,
            "expected_equipped": expected_equipped,
            "expected_bag": expected_bag,
            "material_uids": material_uids,
            "upgraded_accessory": upgraded_accessory,
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
                if str(previous["action"]) != "upgrade" or str(previous["payload"]) != payload_json:
                    return AccessoryUpgradeChange("state_changed", "upgrade", user_id)
                return self._result("duplicate", str(previous["result_json"]))

            row = uow.query_one(
                "SELECT equipped,bag FROM player_data.player_accessory WHERE user_id=?",
                (user_id,),
            )
            if row is None:
                return AccessoryUpgradeChange("accessory_missing", "upgrade", user_id)
            equipped = self._decode(row["equipped"], dict)
            bag = self._decode(row["bag"], list)
            if self._json(equipped) != self._json(expected_equipped) or self._json(bag) != self._json(expected_bag):
                return AccessoryUpgradeChange("state_changed", "upgrade", user_id)

            current = equipped.get(part)
            if not isinstance(current, dict):
                return AccessoryUpgradeChange("accessory_missing", "upgrade", user_id)
            old_quality = int(current.get("quality", 1))
            if old_quality >= 5:
                return AccessoryUpgradeChange("max_quality", "upgrade", user_id)
            required = upgrade_material_count(old_quality)
            if len(material_uids) != required:
                return AccessoryUpgradeChange("material_mismatch", "upgrade", user_id)

            material_indexes = []
            current_signature = self._signature(current)
            for uid in material_uids:
                matches = [
                    (index, item)
                    for index, item in enumerate(bag)
                    if isinstance(item, dict) and str(item.get("uid", "")) == uid
                ]
                if len(matches) != 1:
                    return AccessoryUpgradeChange("material_missing", "upgrade", user_id)
                index, material = matches[0]
                if self._signature(material) != current_signature:
                    return AccessoryUpgradeChange("material_mismatch", "upgrade", user_id)
                material_indexes.append(index)

            updated = dict(upgraded_accessory)
            if (
                str(updated.get("uid", "")) != str(current.get("uid", ""))
                or int(updated.get("quality", 0)) != old_quality + 1
                or int(updated.get("wash_count", -1)) != 0
            ):
                return AccessoryUpgradeChange("invalid_plan", "upgrade", user_id)
            current_fixed = dict(current)
            updated_fixed = dict(updated)
            for field_name in ("quality", "wash_count", "affixes", "locked_affixes"):
                current_fixed.pop(field_name, None)
                updated_fixed.pop(field_name, None)
            if self._json(current_fixed) != self._json(updated_fixed):
                return AccessoryUpgradeChange("invalid_plan", "upgrade", user_id)

            for index in sorted(material_indexes, reverse=True):
                del bag[index]
            equipped[part] = updated
            uow.execute(
                "UPDATE player_data.player_accessory SET equipped=?,bag=? WHERE user_id=?",
                (json.dumps(equipped, ensure_ascii=False), json.dumps(bag, ensure_ascii=False), user_id),
            )
            result_json = self._json(
                {
                    "action": "upgrade",
                    "user_id": user_id,
                    "affected": required,
                    "stone_delta": 0,
                    "accessory": updated,
                    "details": None,
                }
            )
            uow.execute(
                "INSERT INTO accessory_transaction_operations "
                "(operation_id,action,payload,result_json) VALUES(?,?,?,?)",
                (operation_id, "upgrade", payload_json, result_json),
            )
            return AccessoryUpgradeChange("applied", "upgrade", user_id, required, 0, updated)


__all__ = ["AccessoryUpgradeSqlRepository"]
