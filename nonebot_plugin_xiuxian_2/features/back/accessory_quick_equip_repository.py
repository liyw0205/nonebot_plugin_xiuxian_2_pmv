from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from ...infrastructure.database import DatabaseUnitOfWork
from ...infrastructure.database.attached_uow import AttachedDatabaseUnitOfWork
from .accessory_preset_domain import SLOTS, normalize_preset
from .accessory_quick_equip_domain import (
    AccessoryQuickEquipChange,
    AccessoryQuickEquipPlan,
)


class AccessoryQuickEquipSqlRepository:
    """Atomically apply a preset equipment switch and clean missing UIDs."""

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
    def _column(preset_idx: int) -> str:
        preset_idx = int(preset_idx)
        if preset_idx not in {1, 2, 3}:
            raise ValueError("preset index must be 1, 2, or 3")
        return f"preset_{preset_idx}"

    @staticmethod
    def _normalize_equipped(value: Any) -> dict[str, Any]:
        source = value if isinstance(value, dict) else {}
        return {slot: source.get(slot) for slot in SLOTS}

    @classmethod
    def _result(cls, status: str, result_json: str) -> AccessoryQuickEquipChange:
        value = json.loads(result_json)
        return AccessoryQuickEquipChange(
            status=status,
            action=str(value.get("action", "quick_equip_preset")),
            user_id=str(value.get("user_id", "")),
            affected=int(value.get("affected", 0)),
            details=value.get("details"),
        )

    def replay(self, operation_id: str) -> AccessoryQuickEquipChange | None:
        operation_id = str(operation_id).strip()
        if not operation_id:
            raise ValueError("operation_id must not be empty")
        with DatabaseUnitOfWork(self.game_database, read_only=True) as uow:
            row = uow.query_one(
                "SELECT action,result_json FROM accessory_transaction_operations "
                "WHERE operation_id=?",
                (operation_id,),
            )
            if row is None or str(row["action"]) != "quick_equip_preset":
                return None
            return self._result("duplicate", str(row["result_json"]))

    def equip(
        self,
        operation_id: str,
        user_id: str,
        plan: AccessoryQuickEquipPlan,
    ) -> AccessoryQuickEquipChange:
        operation_id = str(operation_id).strip()
        user_id = str(user_id)
        column = self._column(plan.preset_idx)
        if not operation_id:
            raise ValueError("operation_id must not be empty")
        payload = {
            "user_id": user_id,
            "preset_idx": plan.preset_idx,
            "expected_equipped": plan.expected_equipped,
            "expected_bag": plan.expected_bag,
            "expected_preset": plan.expected_preset,
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
                if str(previous["action"]) != "quick_equip_preset" or str(previous["payload"]) != payload_json:
                    return AccessoryQuickEquipChange("state_changed", "quick_equip_preset", user_id)
                return self._result("duplicate", str(previous["result_json"]))

            columns = {
                str(item[1])
                for item in uow.execute("PRAGMA player_data.table_info(player_accessory)").fetchall()
            }
            if column not in columns:
                return AccessoryQuickEquipChange("schema_missing", "quick_equip_preset", user_id)
            row = uow.query_one(
                f"SELECT equipped,bag,{column} FROM player_data.player_accessory WHERE user_id=?",
                (user_id,),
            )
            if row is None:
                return AccessoryQuickEquipChange("state_changed", "quick_equip_preset", user_id)
            current_equipped = self._normalize_equipped(self._decode(row["equipped"], dict))
            current_bag = self._decode(row["bag"], list)
            current_preset = normalize_preset(self._decode(row[column], dict))
            if (
                self._json(current_equipped) != self._json(plan.expected_equipped)
                or self._json(current_bag) != self._json(plan.expected_bag)
                or self._json(current_preset) != self._json(plan.expected_preset)
            ):
                return AccessoryQuickEquipChange("state_changed", "quick_equip_preset", user_id)

            if plan.details is None or self._json(normalize_preset(plan.preset)) != self._json(normalize_preset(plan.details.get("preset", {}))):
                return AccessoryQuickEquipChange("invalid_plan", "quick_equip_preset", user_id)
            uow.execute(
                f"UPDATE player_data.player_accessory SET equipped=?,bag=?,{column}=? WHERE user_id=?",
                (
                    json.dumps(plan.equipped, ensure_ascii=False),
                    json.dumps(plan.bag, ensure_ascii=False),
                    json.dumps(plan.preset, ensure_ascii=False),
                    user_id,
                ),
            )
            result_json = self._json(
                {
                    "action": "quick_equip_preset",
                    "user_id": user_id,
                    "affected": plan.affected,
                    "stone_delta": 0,
                    "accessory": None,
                    "details": plan.details,
                }
            )
            uow.execute(
                "INSERT INTO accessory_transaction_operations "
                "(operation_id,action,payload,result_json) VALUES(?,?,?,?)",
                (operation_id, "quick_equip_preset", payload_json, result_json),
            )
            return AccessoryQuickEquipChange(
                "applied", "quick_equip_preset", user_id, plan.affected, plan.details
            )


__all__ = ["AccessoryQuickEquipSqlRepository"]
