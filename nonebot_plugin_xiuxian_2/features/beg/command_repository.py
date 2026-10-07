from __future__ import annotations

import json
from contextlib import contextmanager
from pathlib import Path

from ...infrastructure.database import DatabaseUnitOfWork, OperationLedger
from ...infrastructure.database.ledger import request_hash


class BegCommandSchemaError(RuntimeError):
    code = "schema_missing"


class BegCommandRepository:
    """Read command receipts and player snapshots without preparing storage."""

    _LEGACY = {
        "daily_settle": ("beg_daily_reward_operations", {"stone_reward", "stone"}),
        "novice_claim": ("novice_gift_claim_operations", {"stone"}),
    }
    _LEDGER_COLUMNS = {
        "operation_id", "action", "request_hash", "status", "result_json", "created_at", "updated_at",
    }
    _AUDIT_COLUMNS = {
        "audit_id", "operation_id", "action", "status", "before_json", "after_json",
        "consumed_json", "granted_json", "category", "occurred_at", "created_at",
    }

    def __init__(self, database: str | Path) -> None:
        self.database = Path(database)
        self.ledger = OperationLedger()

    @contextmanager
    def _read(self):
        if not self.database.is_file():
            raise BegCommandSchemaError("beg command database missing")
        with DatabaseUnitOfWork(self.database, read_only=True) as uow:
            yield uow

    @staticmethod
    def _columns(uow, table):
        return {str(row["name"]) for row in uow.query_all(f'PRAGMA table_info("{table}")')}

    @staticmethod
    def _reject(action, status):
        return {"action": action, "status": status, "replayed": True}

    @staticmethod
    def _valid_amounts(data, fields):
        return all(type(data.get(field)) is int and data[field] >= 0 for field in fields)

    def _ledger_result(self, record, *, operation_id, user_id, action):
        if record.request_hash != request_hash({"user_id": user_id}):
            return self._reject(action, "operation_conflict")
        if record.status == "started":
            return self._reject(action, "operation_pending")
        if record.status == "needs_reconcile":
            return self._reject(action, "operation_failed")
        if record.status not in {"applied", "rejected", "failed"}:
            return self._reject(action, "receipt_invalid")
        try:
            outcome = record.outcome()
        except (ValueError, TypeError, KeyError, AttributeError):
            return self._reject(action, "receipt_invalid")
        if (
            outcome is None or outcome.operation_id != operation_id
            or outcome.action != f"beg.{action}" or outcome.status != record.status
        ):
            return self._reject(action, "receipt_invalid")
        if record.status == "failed":
            # Beg writes assets and its ledger in one transaction; this failure
            # record is saved only after rollback, so the original writer may retry.
            if outcome.code != "internal_error" or outcome.data is not None:
                return self._reject(action, "receipt_invalid")
            return None
        if not isinstance(outcome.data, dict):
            return self._reject(action, "receipt_invalid")
        data = outcome.data
        if "user_id" in data and str(data["user_id"]) != user_id:
            return self._reject(action, "receipt_invalid")
        if record.status == "applied":
            if data.get("status") not in {"applied", "duplicate"} or not self._valid_amounts(data, self._LEGACY[action][1]):
                return self._reject(action, "receipt_invalid")
            return {**data, "action": action, "status": "duplicate", "replayed": True}
        status = data.get("status")
        if not isinstance(status, str) or not status or status in {"applied", "duplicate", "replayed"} or outcome.code != status:
            return self._reject(action, "receipt_invalid")
        return {**data, "action": action, "status": status, "replayed": True}

    def receipt(self, *, operation_id: str, user_id: str, action: str) -> dict | None:
        operation_id, user_id, action = str(operation_id).strip(), str(user_id).strip(), str(action)
        if not operation_id or not user_id or action not in self._LEGACY:
            raise ValueError("valid beg command identity is required")
        with self._read() as uow:
            ledger_columns = self._columns(uow, "operation_ledger")
            if ledger_columns:
                if not self._LEDGER_COLUMNS.issubset(ledger_columns):
                    raise BegCommandSchemaError("beg command ledger schema missing")
                actions = uow.query_all("SELECT action FROM operation_ledger WHERE operation_id=?", (operation_id,))
                if actions:
                    if len(actions) != 1 or str(actions[0]["action"]) != f"beg.{action}":
                        return self._reject(action, "operation_conflict")
                    record = self.ledger.get(uow, operation_id, f"beg.{action}")
                    result = self._ledger_result(record, operation_id=operation_id, user_id=user_id, action=action)
                    if result is not None:
                        return result

            previous = []
            target_ready = False
            for kind, (table, fields) in self._LEGACY.items():
                columns = self._columns(uow, table)
                if not columns:
                    continue
                if not fields.union({"operation_id", "payload"}).issubset(columns):
                    raise BegCommandSchemaError(f"beg command receipt schema missing: {table}")
                if kind == action:
                    target_ready = True
                row = uow.query_one(f'SELECT * FROM "{table}" WHERE operation_id=?', (operation_id,))
                if row is not None:
                    previous.append((kind, row))
            if previous:
                if len(previous) != 1 or previous[0][0] != action:
                    return self._reject(action, "operation_conflict")
                row = previous[0][1]
                try:
                    payload = json.loads(row["payload"])
                except (ValueError, TypeError):
                    return self._reject(action, "receipt_invalid")
                if not isinstance(payload, list) or len(payload) != 1 or not isinstance(payload[0], str):
                    return self._reject(action, "receipt_invalid")
                if payload != [user_id]:
                    return self._reject(action, "operation_conflict")
                fields = self._LEGACY[action][1]
                if not self._valid_amounts(row, fields):
                    return self._reject(action, "receipt_invalid")
                return {"action": action, "status": "duplicate", "replayed": True, **{field: row[field] for field in fields}}
            if not ledger_columns or not target_ready:
                raise BegCommandSchemaError("beg command startup schema missing")
            if not self._AUDIT_COLUMNS.issubset(self._columns(uow, "operation_audit")):
                raise BegCommandSchemaError("beg command audit schema missing")
            return None

    def profile(self, user_id: str) -> dict | None:
        user_id = str(user_id).strip()
        if not user_id:
            raise ValueError("user_id is required")
        required = {"user_id", "create_time", "stone", "sect_id", "root_type", "level", "is_beg", "is_novice"}
        with self._read() as uow:
            if not required.issubset(self._columns(uow, "user_xiuxian")):
                raise BegCommandSchemaError("beg command player schema missing")
            row = uow.query_one(
                "SELECT user_id,create_time,COALESCE(stone,0) AS stone,sect_id,root_type,level,"
                "COALESCE(is_beg,0) AS is_beg,COALESCE(is_novice,0) AS is_novice "
                "FROM user_xiuxian WHERE user_id=?", (user_id,),
            )
            return dict(row) if row is not None else None


__all__ = ["BegCommandRepository", "BegCommandSchemaError"]
