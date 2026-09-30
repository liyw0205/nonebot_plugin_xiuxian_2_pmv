from __future__ import annotations

import json
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from ...infrastructure.database import AttachedDatabaseUnitOfWork, DatabaseUnitOfWork


@dataclass(frozen=True)
class AdminAccessorySnapshot:
    status: str
    equipped: dict[str, Any] = field(default_factory=dict)
    bag: list[dict[str, Any]] = field(default_factory=list)


@dataclass(frozen=True)
class AdminAccessoryAdjustmentResult:
    status: str
    action: str
    user_id: str
    requested_quantity: int = 0
    affected_quantity: int = 0
    accessories: tuple[dict[str, Any], ...] = ()

    @property
    def succeeded(self) -> bool:
        return self.status in {"granted", "destroyed", "duplicate"}


class AdminAccessorySqlRepository:
    def __init__(self, game_database: str | Path, player_database: str | Path) -> None:
        self.game_database = Path(game_database)
        self.player_database = Path(player_database)

    @staticmethod
    def _columns(uow: DatabaseUnitOfWork, table: str, schema: str = "main") -> set[str]:
        rows = uow.query_all(f'PRAGMA "{schema}".table_info("{table}")')
        return {str(row["name"]).casefold() for row in rows}

    @classmethod
    def _schema_ready(cls, uow: DatabaseUnitOfWork) -> bool:
        return (
            {"user_id"}.issubset(cls._columns(uow, "user_xiuxian"))
            and {
                "operation_id", "action", "payload", "result_json"
            }.issubset(cls._columns(uow, "admin_accessory_operations"))
            and {
                "user_id", "source", "action", "item_delta", "detail", "trace_id", "created_at"
            }.issubset(cls._columns(uow, "economy_log"))
            and {"user_id", "equipped", "bag"}.issubset(
                cls._columns(uow, "player_accessory", "player_data")
            )
        )

    @staticmethod
    def _json(value: Any) -> str:
        return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))

    @staticmethod
    def _load_dict(value: Any) -> dict[str, Any]:
        if not value:
            return {}
        decoded = json.loads(value) if isinstance(value, str) else value
        return decoded if isinstance(decoded, dict) else {}

    @staticmethod
    def _load_list(value: Any) -> list[dict[str, Any]]:
        if not value:
            return []
        decoded = json.loads(value) if isinstance(value, str) else value
        return decoded if isinstance(decoded, list) else []

    @staticmethod
    def _result_from_json(status: str, value: str) -> AdminAccessoryAdjustmentResult:
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
    def _owned_count(equipped: dict[str, Any], bag: list[dict[str, Any]]) -> int:
        return len(bag) + sum(1 for item in equipped.values() if item)

    @staticmethod
    def _owned_uids(equipped: dict[str, Any], bag: list[dict[str, Any]]) -> set[str] | None:
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
    def _load_owned(uow: DatabaseUnitOfWork, user_id: str) -> tuple[dict, list] | None:
        rows = uow.query_all(
            "SELECT equipped,bag FROM player_data.player_accessory WHERE user_id=?",
            (user_id,),
        )
        if len(rows) > 1:
            return None
        if not rows:
            return {}, []
        return (
            AdminAccessorySqlRepository._load_dict(rows[0]["equipped"]),
            AdminAccessorySqlRepository._load_list(rows[0]["bag"]),
        )

    def snapshot(self, user_id: str) -> AdminAccessorySnapshot:
        user_id = str(user_id).strip()
        if not user_id:
            raise ValueError("user id is required")
        if not self.player_database.is_file():
            return AdminAccessorySnapshot("schema_missing")
        with DatabaseUnitOfWork(self.player_database, read_only=True) as uow:
            if not {"user_id", "equipped", "bag"}.issubset(
                self._columns(uow, "player_accessory")
            ):
                return AdminAccessorySnapshot("schema_missing")
            rows = uow.query_all(
                "SELECT equipped,bag FROM player_accessory WHERE user_id=?",
                (user_id,),
            )
            if len(rows) > 1:
                return AdminAccessorySnapshot("invalid_state")
            try:
                equipped, bag = (
                    self._load_dict(rows[0]["equipped"]),
                    self._load_list(rows[0]["bag"]),
                ) if rows else ({}, [])
            except (TypeError, ValueError, json.JSONDecodeError):
                return AdminAccessorySnapshot("invalid_state")
            return AdminAccessorySnapshot("ok", equipped, bag)

    @staticmethod
    def _save_accessories(
        uow: DatabaseUnitOfWork, user_id: str, equipped: dict, bag: list
    ) -> None:
        uow.execute(
            "INSERT INTO player_data.player_accessory(user_id,equipped,bag) "
            "VALUES(?,?,?) ON CONFLICT(user_id) DO UPDATE SET "
            "equipped=excluded.equipped,bag=excluded.bag",
            (
                user_id,
                json.dumps(equipped, ensure_ascii=False),
                json.dumps(bag, ensure_ascii=False),
            ),
        )

    @staticmethod
    def _audit(
        uow: DatabaseUnitOfWork,
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
        uow.execute(
            "INSERT INTO economy_log(user_id,source,action,item_delta,detail,trace_id,created_at) "
            "VALUES(?,'admin',?,?,?,?,CURRENT_TIMESTAMP)",
            (
                user_id,
                "admin_accessory_add" if action == "grant" else "admin_accessory_cost",
                item_delta,
                detail,
                operation_id,
            ),
        )

    @staticmethod
    def _validate_snapshot(
        current: tuple[dict, list] | None,
        expected_equipped: dict,
        expected_bag: list,
    ) -> bool:
        return current is not None and (
            AdminAccessorySqlRepository._json(current[0])
            == AdminAccessorySqlRepository._json(expected_equipped)
            and AdminAccessorySqlRepository._json(current[1])
            == AdminAccessorySqlRepository._json(expected_bag)
        )

    def _run(self, operation_id: str, action: str, payload: dict, apply: Callable) -> AdminAccessoryAdjustmentResult:
        payload_json = self._json(payload)
        if not self.game_database.is_file() or not self.player_database.is_file():
            return AdminAccessoryAdjustmentResult(
                "schema_missing", action, str(payload.get("user_id", ""))
            )

        with AttachedDatabaseUnitOfWork(
            self.game_database,
            attachments={"player_data": self.player_database},
            immediate=True,
        ) as uow:
            if not self._schema_ready(uow):
                return AdminAccessoryAdjustmentResult(
                    "schema_missing", action, str(payload.get("user_id", ""))
                )

            previous = uow.query_one(
                "SELECT action,payload,result_json FROM admin_accessory_operations "
                "WHERE operation_id=?",
                (operation_id,),
            )
            if previous is not None:
                if str(previous["action"]) != action or str(previous["payload"]) != payload_json:
                    return AdminAccessoryAdjustmentResult(
                        "operation_conflict", action, str(payload.get("user_id", ""))
                    )
                return self._result_from_json("duplicate", str(previous["result_json"]))

            result = apply(uow)
            if result.status not in {"granted", "destroyed"}:
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
            uow.execute(
                "INSERT INTO admin_accessory_operations("
                "operation_id,action,payload,result_json) VALUES(?,?,?,?)",
                (operation_id, action, payload_json, result_json),
            )
            return result

    def grant(
        self,
        operation_id: str,
        operator_id: str,
        user_id: str,
        item_id: int,
        item_name: str,
        quality: int,
        quantity: int,
        expected_equipped: dict,
        expected_bag: list,
        max_accessories: int,
        create_accessory: Callable[[], dict],
        *,
        target_name: str = "",
    ) -> AdminAccessoryAdjustmentResult:
        operation_id = str(operation_id).strip()
        operator_id = str(operator_id).strip()
        user_id = str(user_id).strip()
        item_id, quality, quantity, max_accessories = map(
            int, (item_id, quality, quantity, max_accessories)
        )
        item_name, target_name = str(item_name), str(target_name)
        if not operation_id or not operator_id or not user_id or item_id <= 0:
            raise ValueError("operation, operator, user and item are required")
        if quality not in {1, 2, 3, 4, 5} or quantity <= 0 or max_accessories <= 0:
            raise ValueError("valid quality, quantity and inventory limit are required")
        if not callable(create_accessory):
            raise ValueError("accessory factory is required")
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

        def apply(uow: DatabaseUnitOfWork) -> AdminAccessoryAdjustmentResult:
            if uow.query_one("SELECT 1 FROM user_xiuxian WHERE user_id=?", (user_id,)) is None:
                return AdminAccessoryAdjustmentResult("user_missing", "grant", user_id, quantity)
            current = self._load_owned(uow, user_id)
            if not self._validate_snapshot(current, expected_equipped, expected_bag):
                return AdminAccessoryAdjustmentResult("state_changed", "grant", user_id, quantity)
            equipped, bag = current
            known_uids = self._owned_uids(equipped, bag)
            if known_uids is None:
                return AdminAccessoryAdjustmentResult("invalid_state", "grant", user_id, quantity)
            if self._owned_count(equipped, bag) + quantity > max_accessories:
                return AdminAccessoryAdjustmentResult("inventory_full", "grant", user_id, quantity)

            generated = []
            for _ in range(quantity):
                accessory = create_accessory()
                if not isinstance(accessory, dict):
                    return AdminAccessoryAdjustmentResult("invalid_plan", "grant", user_id, quantity)
                uid = str(accessory.get("uid", "")).strip()
                try:
                    valid = (
                        uid
                        and uid not in known_uids
                        and int(accessory.get("item_id", 0)) == item_id
                        and int(accessory.get("quality", 0)) == quality
                        and str(accessory.get("name", "")) == item_name
                    )
                except (TypeError, ValueError):
                    valid = False
                if not valid:
                    return AdminAccessoryAdjustmentResult("invalid_plan", "grant", user_id, quantity)
                known_uids.add(uid)
                generated.append(json.loads(self._json(accessory)))

            bag.extend(generated)
            self._save_accessories(uow, user_id, equipped, bag)
            self._audit(
                uow, operation_id, "grant", operator_id, user_id, target_name,
                item_id, item_name, quantity, generated,
            )
            return AdminAccessoryAdjustmentResult(
                "granted", "grant", user_id, quantity, quantity, tuple(generated)
            )

        return self._run(operation_id, "grant", payload, apply)

    def destroy(
        self,
        operation_id: str,
        operator_id: str,
        user_id: str,
        item_id: int,
        item_name: str,
        quantity: int,
        expected_equipped: dict,
        expected_bag: list,
        *,
        target_name: str = "",
    ) -> AdminAccessoryAdjustmentResult:
        operation_id = str(operation_id).strip()
        operator_id, user_id = str(operator_id).strip(), str(user_id).strip()
        item_id, quantity = int(item_id), int(quantity)
        item_name, target_name = str(item_name), str(target_name)
        if not operation_id or not operator_id or not user_id or item_id <= 0 or quantity <= 0:
            raise ValueError("operation, operator, user, item and positive quantity are required")
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

        def apply(uow: DatabaseUnitOfWork) -> AdminAccessoryAdjustmentResult:
            if uow.query_one("SELECT 1 FROM user_xiuxian WHERE user_id=?", (user_id,)) is None:
                return AdminAccessoryAdjustmentResult("user_missing", "destroy", user_id, quantity)
            current = self._load_owned(uow, user_id)
            if not self._validate_snapshot(current, expected_equipped, expected_bag):
                return AdminAccessoryAdjustmentResult("state_changed", "destroy", user_id, quantity)
            equipped, bag = current
            if self._owned_uids(equipped, bag) is None:
                return AdminAccessoryAdjustmentResult("invalid_state", "destroy", user_id, quantity)

            removed, kept = [], []
            for accessory in bag:
                if len(removed) < quantity and int(accessory.get("item_id", 0)) == item_id:
                    removed.append(accessory)
                else:
                    kept.append(accessory)
            if not removed:
                return AdminAccessoryAdjustmentResult("item_missing", "destroy", user_id, quantity)

            self._save_accessories(uow, user_id, equipped, kept)
            self._audit(
                uow, operation_id, "destroy", operator_id, user_id, target_name,
                item_id, item_name, quantity, removed,
            )
            return AdminAccessoryAdjustmentResult(
                "destroyed", "destroy", user_id, quantity, len(removed), tuple(removed)
            )

        return self._run(operation_id, "destroy", payload, apply)


__all__ = [
    "AdminAccessoryAdjustmentResult",
    "AdminAccessorySnapshot",
    "AdminAccessorySqlRepository",
]
