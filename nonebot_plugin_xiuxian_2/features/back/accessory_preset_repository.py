from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from ...infrastructure.database import DatabaseUnitOfWork
from ...infrastructure.database.attached_uow import AttachedDatabaseUnitOfWork
from .accessory_preset_domain import (
    AccessoryPresetChange,
    SLOTS,
    normalize_preset,
)


class AccessoryPresetSqlRepository:
    """Persist one accessory preset and its operation receipt atomically."""

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
    def _normalize_equipped(value: Any) -> dict[str, Any]:
        source = value if isinstance(value, dict) else {}
        return {slot: source.get(slot) for slot in SLOTS}

    @classmethod
    def _preset_from_equipped(cls, equipped: dict[str, Any]) -> dict[str, str | None]:
        normalized = cls._normalize_equipped(equipped)
        return {
            slot: (
                str(normalized[slot].get("uid"))
                if isinstance(normalized[slot], dict)
                and normalized[slot].get("uid") not in (None, "")
                else None
            )
            for slot in SLOTS
        }

    @staticmethod
    def _column(preset_idx: int) -> str:
        preset_idx = int(preset_idx)
        if preset_idx not in {1, 2, 3}:
            raise ValueError("preset index must be 1, 2, or 3")
        return f"preset_{preset_idx}"

    @classmethod
    def _result(cls, status: str, result_json: str) -> AccessoryPresetChange:
        value = json.loads(result_json)
        return AccessoryPresetChange(
            status=status,
            action=str(value.get("action", "save_preset")),
            user_id=str(value.get("user_id", "")),
            affected=int(value.get("affected", 0)),
            details=value.get("details"),
        )

    def replay(self, operation_id: str) -> AccessoryPresetChange | None:
        operation_id = str(operation_id).strip()
        if not operation_id:
            raise ValueError("operation_id must not be empty")
        with DatabaseUnitOfWork(self.game_database, read_only=True) as uow:
            row = uow.query_one(
                "SELECT action,result_json FROM accessory_transaction_operations "
                "WHERE operation_id=?",
                (operation_id,),
            )
            if row is None or str(row["action"]) != "save_preset":
                return None
            return self._result("duplicate", str(row["result_json"]))

    def save(
        self,
        operation_id: str,
        user_id: str,
        preset_idx: int,
        expected_equipped: dict[str, Any],
        expected_preset: dict[str, Any],
        preset: dict[str, str | None],
    ) -> AccessoryPresetChange:
        operation_id = str(operation_id).strip()
        user_id = str(user_id)
        preset_idx = int(preset_idx)
        column = self._column(preset_idx)
        expected_equipped = self._normalize_equipped(expected_equipped)
        expected_preset = normalize_preset(expected_preset)
        preset = normalize_preset(preset)
        if not operation_id:
            raise ValueError("operation_id must not be empty")
        payload = {
            "user_id": user_id,
            "preset_idx": preset_idx,
            "expected_equipped": expected_equipped,
            "expected_preset": expected_preset,
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
                if str(previous["action"]) != "save_preset" or str(previous["payload"]) != payload_json:
                    return AccessoryPresetChange("state_changed", "save_preset", user_id)
                return self._result("duplicate", str(previous["result_json"]))

            columns = {
                str(item[1])
                for item in uow.execute("PRAGMA player_data.table_info(player_accessory)").fetchall()
            }
            if column not in columns:
                return AccessoryPresetChange("schema_missing", "save_preset", user_id)

            row = uow.query_one(
                f"SELECT equipped,bag,{column} FROM player_data.player_accessory WHERE user_id=?",
                (user_id,),
            )
            if row is None:
                current_equipped = {}
                current_preset = normalize_preset({})
                current_bag: list[Any] = []
            else:
                current_equipped = self._decode(row["equipped"], dict)
                current_preset = normalize_preset(self._decode(row[column], dict))
                current_bag = self._decode(row["bag"], list)
            current_equipped = self._normalize_equipped(current_equipped)
            if (
                self._json(current_equipped) != self._json(expected_equipped)
                or self._json(current_preset) != self._json(expected_preset)
            ):
                return AccessoryPresetChange("state_changed", "save_preset", user_id)
            if self._json(self._preset_from_equipped(current_equipped)) != self._json(preset):
                return AccessoryPresetChange("invalid_plan", "save_preset", user_id)

            had_old = any(current_preset.values())
            if row is None:
                uow.execute(
                    f"INSERT INTO player_data.player_accessory(user_id,equipped,bag,{column}) "
                    "VALUES(?,?,?,?)",
                    (
                        user_id,
                        json.dumps(current_equipped, ensure_ascii=False),
                        json.dumps(current_bag, ensure_ascii=False),
                        json.dumps(preset, ensure_ascii=False),
                    ),
                )
            else:
                uow.execute(
                    f"UPDATE player_data.player_accessory SET {column}=? WHERE user_id=?",
                    (json.dumps(preset, ensure_ascii=False), user_id),
                )
            details = {
                "preset_idx": preset_idx,
                "preset": preset,
                "had_old": had_old,
            }
            result_json = self._json(
                {
                    "action": "save_preset",
                    "user_id": user_id,
                    "affected": 1,
                    "stone_delta": 0,
                    "accessory": None,
                    "details": details,
                }
            )
            uow.execute(
                "INSERT INTO accessory_transaction_operations "
                "(operation_id,action,payload,result_json) VALUES(?,?,?,?)",
                (operation_id, "save_preset", payload_json, result_json),
            )
            return AccessoryPresetChange("applied", "save_preset", user_id, 1, details=details)


__all__ = ["AccessoryPresetSqlRepository"]
