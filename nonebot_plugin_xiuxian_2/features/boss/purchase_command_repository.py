from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any, Mapping

from ...infrastructure.database import DatabaseUnitOfWork, OperationLedger
from ...infrastructure.database.ledger import request_hash


class BossPurchaseCommandSchemaError(RuntimeError):
    """The read-side command contract is not available or is incomplete."""

    code = "schema_missing"

    def __init__(self, message: str) -> None:
        super().__init__(f"schema_missing: {message}")


class BossPurchaseCommandRepository:
    """Read BOSS purchase receipts and player snapshots without repairing storage."""

    _ACTION = "boss.purchase"
    _LEGACY_TABLE = "boss_purchase_operations"
    _LEGACY_COLUMNS = {
        "operation_id",
        "payload",
        "quantity",
        "cost",
        "integral",
        "purchased",
        "inventory",
    }
    _LEDGER_COLUMNS = {
        "operation_id",
        "action",
        "request_hash",
        "status",
        "result_json",
        "created_at",
        "updated_at",
    }
    _AUDIT_COLUMNS = {
        "audit_id",
        "operation_id",
        "action",
        "status",
        "before_json",
        "after_json",
        "consumed_json",
        "granted_json",
        "category",
        "occurred_at",
        "created_at",
    }
    _KNOWN_REJECTIONS = {
        "integral_insufficient",
        "limit_reached",
        "inventory_full",
        "state_changed",
        "user_missing",
        "rejected",
    }
    _HASH_RE = re.compile(r"^[0-9a-f]{64}$")

    def __init__(self, database: str | Path) -> None:
        self.database = Path(database)
        self.ledger = OperationLedger()

    @staticmethod
    def _columns(uow: DatabaseUnitOfWork, table: str) -> set[str]:
        return {
            str(row["name"]).casefold()
            for row in uow.query_all(f'PRAGMA table_info("{table}")')
        }

    @staticmethod
    def _table_exists(uow: DatabaseUnitOfWork, table: str) -> bool:
        return (
            uow.query_one(
                "SELECT 1 AS present FROM sqlite_master "
                "WHERE type='table' AND name=?",
                (table,),
            )
            is not None
        )

    @staticmethod
    def _number(value: Any, *, positive: bool = False) -> int:
        if isinstance(value, bool) or not isinstance(value, int):
            raise ValueError("receipt number is invalid")
        if value < (1 if positive else 0):
            raise ValueError("receipt number is invalid")
        return value

    @classmethod
    def _reject(cls, action: str, status: str) -> dict[str, Any]:
        return {"action": action, "status": status, "replayed": True}

    @classmethod
    def _display_action(cls, action: str) -> str:
        action = str(action).strip()
        if action in {"purchase", cls._ACTION}:
            return "purchase" if action == "purchase" else cls._ACTION
        raise ValueError("valid boss purchase action is required")

    @classmethod
    def _action_matches(cls, action: str) -> bool:
        return action in {"purchase", cls._ACTION}

    @classmethod
    def _legacy_payload(cls, encoded: Any) -> tuple[str, int, str, str, int, int, int, int]:
        try:
            payload = json.loads(encoded)
        except (TypeError, ValueError) as exc:
            raise ValueError("legacy purchase payload is invalid") from exc
        if not isinstance(payload, list) or len(payload) != 8:
            raise ValueError("legacy purchase payload is invalid")
        user_id, item_id, item_name, item_type, quantity, unit_cost, weekly_limit, max_goods_num = payload
        if not isinstance(user_id, str) or not user_id.strip():
            raise ValueError("legacy purchase user is invalid")
        if not isinstance(item_name, str) or not isinstance(item_type, str):
            raise ValueError("legacy purchase metadata is invalid")
        values = (item_id, quantity, unit_cost, weekly_limit, max_goods_num)
        if any(isinstance(value, bool) or not isinstance(value, int) or value < 0 for value in values):
            raise ValueError("legacy purchase quantity is invalid")
        if item_id <= 0 or quantity <= 0:
            raise ValueError("legacy purchase quantity is invalid")
        return (
            user_id,
            item_id,
            item_name,
            item_type,
            quantity,
            unit_cost,
            weekly_limit,
            max_goods_num,
        )

    @classmethod
    def _legacy_payload_hash(cls, payload: tuple[str, int, str, str, int, int, int, int]) -> str:
        user_id, item_id, _name, _type, quantity, unit_cost, weekly_limit, max_goods_num = payload
        return request_hash(
            {
                "user_id": user_id,
                "item_id": item_id,
                "quantity": quantity,
                "unit_cost": unit_cost,
                "weekly_limit": weekly_limit,
                "max_goods_num": max_goods_num,
            }
        )

    @classmethod
    def _ledger_payload_hash(
        cls,
        *,
        user_id: str,
        item_id: int | None,
        quantity: int | None,
        unit_cost: int | None,
        weekly_limit: int | None,
        max_goods_num: int | None,
    ) -> str | None:
        values = (item_id, quantity, unit_cost, weekly_limit, max_goods_num)
        if any(value is None for value in values):
            return None
        if any(isinstance(value, bool) or not isinstance(value, int) for value in values):
            raise ValueError("purchase identity is invalid")
        return request_hash(
            {
                "user_id": user_id,
                "item_id": item_id,
                "quantity": quantity,
                "unit_cost": unit_cost,
                "weekly_limit": weekly_limit,
                "max_goods_num": max_goods_num,
            }
        )

    @classmethod
    def _data_numbers(cls, data: Mapping[str, Any], *, success: bool) -> dict[str, int]:
        required = ("quantity", "cost", "integral", "purchased", "inventory")
        result: dict[str, int] = {}
        for field in required:
            result[field] = cls._number(data.get(field), positive=success and field == "quantity")
        if success and result["cost"] < 0:
            raise ValueError("successful purchase cost is invalid")
        if not success and (result["quantity"] != 0 or result["cost"] != 0):
            raise ValueError("rejected purchase result is invalid")
        return result

    @classmethod
    def _validate_ledger_result(
        cls,
        record: Any,
        *,
        operation_id: str,
        user_id: str,
        action: str,
        identity_hash: str | None,
        item_id: int | None,
        quantity: int | None,
        legacy_payload: tuple[str, int, str, str, int, int, int, int] | None,
    ) -> dict[str, Any] | None:
        if record is None:
            return cls._reject(action, "receipt_invalid")
        if identity_hash is not None and record.request_hash != identity_hash:
            return cls._reject(action, "operation_conflict")
        if not isinstance(record.request_hash, str) or not cls._HASH_RE.fullmatch(record.request_hash):
            return cls._reject(action, "receipt_invalid")
        if record.status == "started":
            return cls._reject(action, "operation_pending")
        if record.status == "needs_reconcile":
            return cls._reject(action, "operation_failed")
        if record.status not in {"applied", "rejected", "failed"}:
            return cls._reject(action, "receipt_invalid")
        try:
            outcome = record.outcome()
        except (TypeError, ValueError, KeyError, AttributeError):
            return cls._reject(action, "receipt_invalid")
        if outcome is None or outcome.operation_id != operation_id or outcome.action != cls._ACTION:
            return cls._reject(action, "receipt_invalid")
        if outcome.status != record.status:
            return cls._reject(action, "receipt_invalid")
        if record.status == "failed":
            if outcome.code != "internal_error" or outcome.data is not None:
                return cls._reject(action, "receipt_invalid")
            return None
        if not isinstance(outcome.data, dict):
            return cls._reject(action, "receipt_invalid")
        data = dict(outcome.data)
        if "user_id" in data and str(data["user_id"]) != user_id:
            return cls._reject(action, "receipt_invalid")
        request = data.get("request")
        if request is None:
            if legacy_payload is None:
                return cls._reject(action, "receipt_invalid")
            if record.request_hash != cls._legacy_payload_hash(legacy_payload):
                return cls._reject(action, "operation_conflict")
            if item_id is not None and item_id != legacy_payload[1]:
                return cls._reject(action, "operation_conflict")
            if quantity is not None and quantity != legacy_payload[4]:
                return cls._reject(action, "operation_conflict")
            if item_id is None or quantity is None:
                return cls._reject(action, "receipt_invalid")
            data.setdefault("item_id", legacy_payload[1])
            data.setdefault("item_name", legacy_payload[2])
            data.setdefault("item_type", legacy_payload[3])
        elif not isinstance(request, Mapping):
            return cls._reject(action, "receipt_invalid")
        else:
            try:
                request_data = dict(request)
                request_user = request_data.get("user_id")
                request_item = cls._number(request_data.get("item_id"), positive=True)
                request_quantity = cls._number(request_data.get("quantity"), positive=True)
                request_unit_cost = cls._number(request_data.get("unit_cost"))
                cls._number(request_data.get("weekly_limit"))
                cls._number(request_data.get("max_goods_num"))
                if not isinstance(request_user, str):
                    raise ValueError("purchase user is invalid")
                if request_user != user_id:
                    return cls._reject(action, "operation_conflict")
                requested_quantity = request_data.get("requested_quantity", request_quantity)
                requested_quantity = cls._number(requested_quantity, positive=True)
                if requested_quantity < request_quantity:
                    raise ValueError("requested quantity precedes applied quantity")
                if item_id is None or quantity is None:
                    raise ValueError("purchase identity is required")
                if item_id != request_item or quantity != requested_quantity:
                    return cls._reject(action, "operation_conflict")
                if record.request_hash != request_hash(request_data):
                    return cls._reject(action, "operation_conflict")
                if record.status == "applied" and int(data.get("quantity", 0) or 0) != request_quantity:
                    return cls._reject(action, "receipt_invalid")
                if record.status == "applied" and int(data.get("cost", -1) or 0) != request_quantity * request_unit_cost:
                    return cls._reject(action, "receipt_invalid")
                if "item_id" in data:
                    try:
                        if cls._number(data["item_id"], positive=True) != request_item:
                            return cls._reject(action, "receipt_invalid")
                    except (TypeError, ValueError):
                        return cls._reject(action, "receipt_invalid")
                data.setdefault("item_id", request_item)
                if (
                    not isinstance(data.get("item_name"), str)
                    or not data["item_name"].strip()
                    or not isinstance(data.get("item_type"), str)
                    or not data["item_type"].strip()
                ):
                    return cls._reject(action, "receipt_invalid")
            except (TypeError, ValueError, OverflowError):
                return cls._reject(action, "receipt_invalid")
        try:
            if record.status == "applied":
                if data.get("status") not in {"applied", "duplicate"}:
                    raise ValueError("successful purchase status is invalid")
                numbers = cls._data_numbers(data, success=True)
                data.update(numbers)
                return {
                    **data,
                    "action": action,
                    "status": "duplicate",
                    "replayed": True,
                }
            status = data.get("status")
            if (
                not isinstance(status, str)
                or not status
                or status in {"applied", "duplicate", "replayed"}
                or outcome.code != status
            ):
                raise ValueError("rejected purchase status is invalid")
            numbers = cls._data_numbers(data, success=False)
            data.update(numbers)
            return {**data, "action": action, "status": status, "replayed": True}
        except (TypeError, ValueError, OverflowError):
            return cls._reject(action, "receipt_invalid")

    @classmethod
    def _legacy_result(
        cls,
        row: Mapping[str, Any],
        *,
        operation_id: str,
        user_id: str,
        action: str,
        identity_hash: str | None,
        item_id: int | None = None,
        quantity: int | None = None,
    ) -> dict[str, Any]:
        payload = cls._legacy_payload(row["payload"])
        payload_user, item_id, item_name, item_type, quantity, unit_cost, weekly_limit, max_goods_num = payload
        if payload_user != user_id:
            return cls._reject(action, "operation_conflict")
        if item_id is not None and item_id != payload[1]:
            return cls._reject(action, "operation_conflict")
        if quantity is not None and quantity != payload[4]:
            return cls._reject(action, "operation_conflict")
        if identity_hash is not None and identity_hash != cls._legacy_payload_hash(payload):
            return cls._reject(action, "operation_conflict")
        status = row.get("status")
        if not isinstance(status, str) or not status.strip():
            return cls._reject(action, "receipt_invalid")
        status = status.strip()
        try:
            values = {
                "quantity": cls._number(row["quantity"]),
                "cost": cls._number(row["cost"]),
                "integral": cls._number(row["integral"]),
                "purchased": cls._number(row["purchased"]),
                "inventory": cls._number(row["inventory"]),
            }
            if status in {"applied", "duplicate"}:
                if values["quantity"] != quantity or values["quantity"] <= 0:
                    raise ValueError("legacy purchase quantity mismatch")
                if values["cost"] != quantity * unit_cost:
                    raise ValueError("legacy purchase cost mismatch")
                result_status = "duplicate"
            elif status in cls._KNOWN_REJECTIONS:
                if values["quantity"] != 0 or values["cost"] != 0:
                    raise ValueError("legacy rejected purchase result is invalid")
                result_status = status
            else:
                raise ValueError("legacy purchase status is invalid")
        except (TypeError, ValueError, OverflowError):
            return cls._reject(action, "receipt_invalid")
        return {
            **values,
            "item_id": item_id,
            "item_name": item_name,
            "item_type": item_type,
            "operation_id": operation_id,
            "action": action,
            "status": result_status,
            "replayed": True,
        }

    def receipt(
        self,
        operation_id: str,
        user_id: str,
        action: str = "purchase",
        *,
        item_id: int | None = None,
        quantity: int | None = None,
        unit_cost: int | None = None,
        weekly_limit: int | None = None,
        max_goods_num: int | None = None,
    ) -> dict[str, Any] | None:
        operation_id, user_id = str(operation_id).strip(), str(user_id).strip()
        if not operation_id or not user_id or not self._action_matches(action):
            raise ValueError("valid boss purchase identity is required")
        display_action = self._display_action(action)
        identity_hash = self._ledger_payload_hash(
            user_id=user_id,
            item_id=item_id,
            quantity=quantity,
            unit_cost=unit_cost,
            weekly_limit=weekly_limit,
            max_goods_num=max_goods_num,
        )
        if not self.database.is_file():
            raise BossPurchaseCommandSchemaError("boss purchase database missing")
        with DatabaseUnitOfWork(self.database, read_only=True) as uow:
            has_ledger = self._table_exists(uow, "operation_ledger")
            has_legacy = self._table_exists(uow, self._LEGACY_TABLE)
            if has_ledger and not self._LEDGER_COLUMNS <= self._columns(uow, "operation_ledger"):
                raise BossPurchaseCommandSchemaError("boss purchase ledger schema missing")
            if has_legacy and not self._LEGACY_COLUMNS <= self._columns(uow, self._LEGACY_TABLE):
                raise BossPurchaseCommandSchemaError("boss purchase receipt schema missing")
            if not has_ledger and not has_legacy:
                raise BossPurchaseCommandSchemaError("boss purchase receipt schema missing")

            legacy_row = None
            legacy_payload = None
            legacy_payload_invalid = False
            if has_legacy:
                legacy_row = uow.query_one(
                    f'SELECT * FROM "{self._LEGACY_TABLE}" WHERE operation_id=?',
                    (operation_id,),
                )
                if legacy_row is not None:
                    try:
                        legacy_payload = self._legacy_payload(legacy_row["payload"])
                    except (KeyError, TypeError, ValueError):
                        legacy_payload_invalid = True
                    if legacy_payload_invalid:
                        pass
                    elif legacy_payload[0] != user_id:
                        return self._reject(display_action, "operation_conflict")
                    elif item_id is not None and item_id != legacy_payload[1]:
                        return self._reject(display_action, "operation_conflict")
                    elif not has_ledger and quantity is not None and quantity != legacy_payload[4]:
                        return self._reject(display_action, "operation_conflict")

            if has_ledger:
                rows = uow.query_all(
                    "SELECT operation_id,action,request_hash,status,result_json,created_at,updated_at "
                    "FROM operation_ledger WHERE operation_id=?",
                    (operation_id,),
                )
                if rows:
                    if len(rows) != 1 or rows[0]["action"] != self._ACTION:
                        return self._reject(display_action, "operation_conflict")
                    record = self.ledger.get(uow, operation_id, self._ACTION)
                    ledger_result = self._validate_ledger_result(
                        record,
                        operation_id=operation_id,
                        user_id=user_id,
                        action=display_action,
                        identity_hash=identity_hash,
                        item_id=item_id,
                        quantity=quantity,
                        legacy_payload=legacy_payload,
                    )
                    if ledger_result is not None:
                        return ledger_result
                    # A retryable failure still carries the original request
                    # identity; use it when checking a conflicting old row.
                    identity_hash = identity_hash or record.request_hash
                    # A failed record is retryable only when no committed legacy
                    # receipt exists for the same operation.
                    if not has_legacy:
                        return None
                elif not legacy_row:
                    if not self._table_exists(uow, "operation_audit"):
                        raise BossPurchaseCommandSchemaError("boss purchase audit schema missing")
                    if not self._AUDIT_COLUMNS <= self._columns(uow, "operation_audit"):
                        raise BossPurchaseCommandSchemaError("boss purchase audit schema missing")

            if has_legacy:
                legacy_columns = self._columns(uow, self._LEGACY_TABLE)
                legacy_row = uow.query_one(
                    f'SELECT * FROM "{self._LEGACY_TABLE}" WHERE operation_id=?',
                    (operation_id,),
                )
                if legacy_row is not None:
                    if legacy_payload_invalid:
                        return self._reject(display_action, "receipt_invalid")
                    if "status" not in legacy_columns:
                        return self._reject(display_action, "receipt_invalid")
                    return self._legacy_result(
                        legacy_row,
                        operation_id=operation_id,
                        user_id=user_id,
                        action=display_action,
                        identity_hash=identity_hash,
                        item_id=item_id,
                        quantity=quantity,
                    )
            if has_ledger:
                return None
            raise BossPurchaseCommandSchemaError("boss purchase ledger schema missing")

    def profile(self, user_id: str) -> dict[str, Any] | None:
        user_id = str(user_id).strip()
        if not user_id:
            raise ValueError("user_id is required")
        if not self.database.is_file():
            raise BossPurchaseCommandSchemaError("boss purchase database missing")
        required = {"user_id"}
        with DatabaseUnitOfWork(self.database, read_only=True) as uow:
            if not self._table_exists(uow, "user_xiuxian"):
                raise BossPurchaseCommandSchemaError("boss purchase player schema missing")
            columns = self._columns(uow, "user_xiuxian")
            if not required <= columns:
                raise BossPurchaseCommandSchemaError("boss purchase player schema missing")
            row = uow.query_one(
                'SELECT * FROM "user_xiuxian" WHERE user_id=? ORDER BY rowid ASC LIMIT 1',
                (user_id,),
            )
            if row is None:
                return None
            result: dict[str, Any] = {"user_id": user_id}
            values = {str(key).casefold(): value for key, value in row.items()}
            for field in ("stone", "integral", "boss_integral", "boss_stone", "boss_battle_count"):
                if field in columns:
                    value = values.get(field)
                    result[field] = int(value or 0)
            return result


__all__ = ["BossPurchaseCommandRepository", "BossPurchaseCommandSchemaError"]
