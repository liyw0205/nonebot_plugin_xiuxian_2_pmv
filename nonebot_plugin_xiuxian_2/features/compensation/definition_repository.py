from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Mapping

from ...core.errors import OperationConflictError
from ...core.result import OperationOutcome
from ...infrastructure.database import DatabaseUnitOfWork, OperationLedger
from ...infrastructure.database.ledger import request_hash


@dataclass(frozen=True)
class CompensationDefinitionMutation:
    status: str
    operation_id: str
    action: str
    record_id: str = ""
    version: int = 0
    removed_definitions: int = 0
    removed_claims: int = 0
    record: dict[str, Any] | None = None
    replayed: bool = False

    @property
    def succeeded(self) -> bool:
        return self.status in {
            "created", "updated", "deleted", "cleared", "missing", "duplicate"
        }


class CompensationDefinitionSqlRepository:
    ACTION_UPSERT = "compensation.definition_upsert_v2"
    ACTION_DELETE = "compensation.definition_delete_v2"
    ACTION_CLEAR = "compensation.definition_clear_v2"

    def __init__(
        self,
        database: str | Path,
        *,
        ledger: OperationLedger | None = None,
    ) -> None:
        self.database = Path(database)
        self.ledger = ledger or OperationLedger()

    @staticmethod
    def _schema_ready(uow: DatabaseUnitOfWork) -> bool:
        required = {
            "compensation_definition_revisions",
            "compensation_definitions",
            "compensation_definition_operations",
            "compensation_legacy_migrations",
            "reward_claims",
            "reward_claim_counters",
            "operation_ledger",
            "operation_audit",
        }
        existing = {
            str(row["name"])
            for row in uow.query_all(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )
        }
        if not required.issubset(existing):
            return False
        if uow.query_one(
            "SELECT 1 AS found FROM compensation_legacy_migrations "
            "WHERE migration_key=?",
            ("legacy-compensation-json-v1",),
        ) is None:
            return False
        return "result_json" in {
            str(row["name"]) for row in uow.query_all("PRAGMA table_info(compensation_definition_operations)")
        }

    @staticmethod
    def _canonical_record(record: Mapping[str, Any]) -> tuple[dict[str, Any], str]:
        normalized = {
            str(key): value
            for key, value in dict(record).items()
            if str(key) != "_definition_version"
        }
        payload = json.dumps(
            normalized,
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
        )
        return normalized, payload

    @staticmethod
    def _catalog_version(rows) -> str:
        payload = json.dumps(
            [(str(row[0]), int(row[1])) for row in rows],
            ensure_ascii=True,
            separators=(",", ":"),
        )
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()

    @staticmethod
    def _definition(row) -> dict[str, Any] | None:
        if row is None:
            return None
        record = json.loads(str(row["record_json"]))
        record["_definition_version"] = int(row["version"])
        return record

    @staticmethod
    def _legacy_operation(uow: DatabaseUnitOfWork, operation_id: str):
        return uow.query_one(
            "SELECT operation_id,payload,action,record_id,version,outcome,"
            "removed_definitions,removed_claims,result_json "
            "FROM compensation_definition_operations WHERE operation_id=?",
            (operation_id,),
        )

    @staticmethod
    def _legacy_result(row, action: str):
        record = None
        if str(row["result_json"] or "").strip():
            decoded = json.loads(str(row["result_json"]))
            record = decoded if isinstance(decoded, dict) and decoded else None
        return CompensationDefinitionMutation(
            status=str(row["outcome"]),
            operation_id=str(row["operation_id"]),
            action=action,
            record_id=str(row["record_id"] or ""),
            version=int(row["version"] or 0),
            removed_definitions=int(row["removed_definitions"] or 0),
            removed_claims=int(row["removed_claims"] or 0),
            record=record,
            replayed=True,
        )

    @staticmethod
    def _outcome_result(outcome: OperationOutcome | None, action: str):
        if outcome is None:
            return CompensationDefinitionMutation("in_progress", "", action)
        data = dict(outcome.data or {})
        return CompensationDefinitionMutation(
            status=str(data.get("status", outcome.code or outcome.status)),
            operation_id=str(outcome.operation_id),
            action=action,
            record_id=str(data.get("record_id", "")),
            version=int(data.get("version", 0)),
            removed_definitions=int(data.get("removed_definitions", 0)),
            removed_claims=int(data.get("removed_claims", 0)),
            record=data.get("record"),
            replayed=outcome.replayed,
        )

    @staticmethod
    def _result_data(result: CompensationDefinitionMutation) -> dict[str, Any]:
        return {
            "status": result.status,
            "record_id": result.record_id,
            "version": result.version,
            "removed_definitions": result.removed_definitions,
            "removed_claims": result.removed_claims,
            "record": result.record,
        }

    def _finish(
        self,
        uow: DatabaseUnitOfWork,
        result: CompensationDefinitionMutation,
        ledger_action: str,
    ) -> None:
        data = self._result_data(result)
        if result.succeeded:
            outcome = OperationOutcome.applied(
                result.operation_id,
                ledger_action,
                data=data,
                audit_category="compensation",
                after={
                    "removed_definitions": result.removed_definitions,
                    "removed_claims": result.removed_claims,
                },
            )
        else:
            outcome = OperationOutcome.rejected(
                result.operation_id,
                ledger_action,
                result.status,
                code=result.status,
                data=data,
                audit_category="compensation",
            )
        self.ledger.finish(uow, outcome)

    def _require_database(self) -> None:
        if not self.database.is_file():
            raise RuntimeError("compensation definition database is missing")

    def list_definitions(self) -> dict[str, dict[str, Any]]:
        self._require_database()
        with DatabaseUnitOfWork(self.database, read_only=True) as uow:
            if not self._schema_ready(uow):
                raise RuntimeError("compensation definition schema is missing")
            rows = uow.query_all(
                "SELECT record_id,version,record_json FROM compensation_definitions "
                "ORDER BY record_id"
            )
            return {
                str(row["record_id"]): self._definition(row)
                for row in rows
            }

    def get_definition(self, record_id: str) -> dict[str, Any] | None:
        self._require_database()
        with DatabaseUnitOfWork(self.database, read_only=True) as uow:
            if not self._schema_ready(uow):
                raise RuntimeError("compensation definition schema is missing")
            row = uow.query_one(
                "SELECT record_id,version,record_json FROM compensation_definitions "
                "WHERE record_id=?",
                (str(record_id).strip(),),
            )
            return self._definition(row)

    def claimed_data(self) -> dict[str, list[str]]:
        self._require_database()
        with DatabaseUnitOfWork(self.database, read_only=True) as uow:
            if not self._schema_ready(uow):
                raise RuntimeError("compensation definition schema is missing")
            rows = uow.query_all(
                "SELECT user_id,record_id FROM reward_claims "
                "WHERE reward_type='补偿' ORDER BY user_id,record_id"
            )
            result: dict[str, list[str]] = {}
            for row in rows:
                result.setdefault(str(row["user_id"]), []).append(
                    str(row["record_id"])
                )
            return result

    def catalog_version(self) -> str:
        self._require_database()
        with DatabaseUnitOfWork(self.database, read_only=True) as uow:
            if not self._schema_ready(uow):
                raise RuntimeError("compensation definition schema is missing")
            rows = uow.query_all(
                "SELECT record_id,version FROM compensation_definitions "
                "ORDER BY record_id"
            )
            return self._catalog_version(
                [(row["record_id"], row["version"]) for row in rows]
            )

    def replay_upsert(
        self, operation_id: str, request_identity: str
    ) -> CompensationDefinitionMutation | None:
        operation_id = str(operation_id).strip()
        request_identity = str(request_identity).strip()
        self._require_database()
        ledger_payload = {"request_identity": request_identity}
        with DatabaseUnitOfWork(self.database, read_only=True) as uow:
            if not self._schema_ready(uow):
                raise RuntimeError("compensation definition schema is missing")
            legacy = self._legacy_operation(uow, operation_id)
            if legacy is not None:
                expected = json.dumps(
                    ["upsert", request_identity],
                    ensure_ascii=True,
                    separators=(",", ":"),
                )
                if str(legacy["action"]) != "upsert" or str(legacy["payload"]) != expected:
                    return CompensationDefinitionMutation(
                        "operation_conflict", operation_id, "upsert"
                    )
                return self._legacy_result(legacy, "upsert")

            existing = self.ledger.get(uow, operation_id, self.ACTION_UPSERT)
            if existing is None:
                return None
            if existing.request_hash != request_hash(ledger_payload):
                return CompensationDefinitionMutation(
                    "operation_conflict", operation_id, "upsert"
                )
            return self._outcome_result(existing.outcome(), self.ACTION_UPSERT)

    def upsert(
        self,
        operation_id: str,
        request_identity: str,
        record_id: str,
        record: Mapping[str, Any],
        expected_version: int | None = None,
        *,
        occurred_at: str | None = None,
    ) -> CompensationDefinitionMutation:
        operation_id = str(operation_id).strip()
        request_identity = str(request_identity).strip()
        record_id = str(record_id).strip()
        expected_version = (
            None if expected_version in (None, "") else int(expected_version)
        )
        if not operation_id or not request_identity or not record_id:
            raise ValueError("operation_id, request_identity and record_id are required")
        if not isinstance(record, Mapping):
            raise ValueError("valid compensation definition is required")
        ledger_payload = {"request_identity": request_identity}
        legacy_payload = json.dumps(
            ["upsert", request_identity],
            ensure_ascii=True,
            separators=(",", ":"),
        )
        normalized, record_payload = self._canonical_record(record)
        occurred_at = str(occurred_at or datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        if not self.database.is_file():
            return CompensationDefinitionMutation(
                "schema_missing", operation_id, "upsert", record_id
            )

        try:
            with DatabaseUnitOfWork(self.database, immediate=True) as uow:
                if not self._schema_ready(uow):
                    return CompensationDefinitionMutation(
                        "schema_missing", operation_id, "upsert", record_id
                    )
                legacy = self._legacy_operation(uow, operation_id)
                if legacy is not None:
                    if str(legacy["action"]) != "upsert" or str(legacy["payload"]) != legacy_payload:
                        return CompensationDefinitionMutation(
                            "operation_conflict", operation_id, "upsert", record_id
                        )
                    return self._legacy_result(legacy, "upsert")
                try:
                    existing = self.ledger.begin(
                        uow, operation_id, self.ACTION_UPSERT, ledger_payload
                    )
                except OperationConflictError:
                    return CompensationDefinitionMutation(
                        "operation_conflict", operation_id, "upsert", record_id
                    )
                if existing is not None:
                    return self._outcome_result(
                        existing.outcome(), self.ACTION_UPSERT
                    )

                current = uow.query_one(
                    "SELECT version FROM compensation_definitions WHERE record_id=?",
                    (record_id,),
                )
                current_version = None if current is None else int(current["version"])
                if current_version is not None and expected_version is None:
                    result = CompensationDefinitionMutation(
                        "version_required", operation_id, "upsert", record_id
                    )
                    self._finish(uow, result, self.ACTION_UPSERT)
                    return result
                if current_version != expected_version and not (
                    current_version is None and expected_version in (None, 0)
                ):
                    result = CompensationDefinitionMutation(
                        "definition_changed",
                        operation_id,
                        "upsert",
                        record_id,
                        current_version or 0,
                    )
                    self._finish(uow, result, self.ACTION_UPSERT)
                    return result

                revision = uow.query_one(
                    "SELECT last_version FROM compensation_definition_revisions "
                    "WHERE record_id=?",
                    (record_id,),
                )
                next_version = 1 if revision is None else int(revision["last_version"]) + 1
                uow.execute(
                    "INSERT INTO compensation_definition_revisions(record_id,last_version) "
                    "VALUES(?,?) ON CONFLICT(record_id) DO UPDATE SET "
                    "last_version=excluded.last_version",
                    (record_id, next_version),
                )
                status = "created" if current is None else "updated"
                if current is None:
                    uow.execute(
                        "INSERT INTO compensation_definitions(record_id,version,record_json,created_at,updated_at) "
                        "VALUES(?,?,?,?,?)",
                        (record_id, next_version, record_payload, occurred_at, occurred_at),
                    )
                else:
                    uow.execute(
                        "UPDATE compensation_definitions SET version=?,record_json=?,updated_at=? "
                        "WHERE record_id=? AND version=?",
                        (next_version, record_payload, occurred_at, record_id, current_version),
                    )

                result_record = dict(normalized)
                result_record["_definition_version"] = next_version
                result = CompensationDefinitionMutation(
                    status,
                    operation_id,
                    "upsert",
                    record_id,
                    next_version,
                    record=result_record,
                )
                self._write_legacy_operation(
                    uow,
                    result,
                    legacy_payload,
                    occurred_at,
                )
                self._finish(uow, result, self.ACTION_UPSERT)
                return result
        except Exception as exc:
            self.ledger.record_failure(
                self.database, operation_id, self.ACTION_UPSERT, ledger_payload, str(exc)
            )
            raise

    def _write_legacy_operation(
        self,
        uow: DatabaseUnitOfWork,
        result: CompensationDefinitionMutation,
        payload: str,
        occurred_at: str,
    ) -> None:
        uow.execute(
            "INSERT INTO compensation_definition_operations("
            "operation_id,payload,action,record_id,version,outcome,removed_definitions,"
            "removed_claims,created_at,result_json) VALUES(?,?,?,?,?,?,?,?,?,?)",
            (
                result.operation_id,
                payload,
                result.action,
                result.record_id,
                result.version,
                result.status,
                result.removed_definitions,
                result.removed_claims,
                occurred_at,
                json.dumps(result.record or {}, ensure_ascii=True, sort_keys=True, separators=(",", ":")),
            ),
        )

    def delete(
        self,
        operation_id: str,
        record_id: str,
        expected_version: int | None,
        *,
        occurred_at: str | None = None,
    ) -> CompensationDefinitionMutation:
        operation_id = str(operation_id).strip()
        record_id = str(record_id).strip()
        expected_version = (
            None if expected_version in (None, "") else int(expected_version)
        )
        if not operation_id or not record_id:
            raise ValueError("operation_id and record_id are required")
        ledger_payload = {"record_id": record_id}
        legacy_payload = json.dumps(
            ["delete", record_id, expected_version],
            ensure_ascii=True,
            separators=(",", ":"),
        )
        occurred_at = str(occurred_at or datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        if not self.database.is_file():
            return CompensationDefinitionMutation(
                "schema_missing", operation_id, "delete", record_id
            )

        try:
            with DatabaseUnitOfWork(self.database, immediate=True) as uow:
                if not self._schema_ready(uow):
                    return CompensationDefinitionMutation(
                        "schema_missing", operation_id, "delete", record_id
                    )
                legacy = self._legacy_operation(uow, operation_id)
                if legacy is not None:
                    if str(legacy["action"]) != "delete" or str(legacy["record_id"]) != record_id:
                        return CompensationDefinitionMutation(
                            "operation_conflict", operation_id, "delete", record_id
                        )
                    return self._legacy_result(legacy, "delete")
                try:
                    existing = self.ledger.begin(
                        uow, operation_id, self.ACTION_DELETE, ledger_payload
                    )
                except OperationConflictError:
                    return CompensationDefinitionMutation(
                        "operation_conflict", operation_id, "delete", record_id
                    )
                if existing is not None:
                    return self._outcome_result(existing.outcome(), self.ACTION_DELETE)

                definition = uow.query_one(
                    "SELECT version FROM compensation_definitions WHERE record_id=?",
                    (record_id,),
                )
                current_version = None if definition is None else int(definition["version"])
                if current_version is not None and expected_version is None:
                    result = CompensationDefinitionMutation(
                        "version_required", operation_id, "delete", record_id
                    )
                    self._finish(uow, result, self.ACTION_DELETE)
                    return result
                if current_version != expected_version and not (
                    current_version is None and expected_version is None
                ):
                    result = CompensationDefinitionMutation(
                        "definition_changed",
                        operation_id,
                        "delete",
                        record_id,
                        current_version or 0,
                    )
                    self._finish(uow, result, self.ACTION_DELETE)
                    return result

                removed_claims = int(
                    uow.execute(
                        "SELECT COUNT(*) FROM reward_claims "
                        "WHERE reward_type='补偿' AND record_id=?",
                        (record_id,),
                    ).fetchone()[0]
                )
                uow.execute(
                    "DELETE FROM compensation_definitions WHERE record_id=?",
                    (record_id,),
                )
                uow.execute(
                    "DELETE FROM reward_claims WHERE reward_type='补偿' AND record_id=?",
                    (record_id,),
                )
                uow.execute(
                    "DELETE FROM reward_claim_counters WHERE reward_type='补偿' AND record_id=?",
                    (record_id,),
                )
                status = "deleted" if definition is not None else "missing"
                result = CompensationDefinitionMutation(
                    status,
                    operation_id,
                    "delete",
                    record_id,
                    current_version or 0,
                    int(definition is not None),
                    removed_claims,
                )
                self._write_legacy_operation(
                    uow,
                    result,
                    legacy_payload,
                    occurred_at,
                )
                self._finish(uow, result, self.ACTION_DELETE)
                return result
        except Exception as exc:
            self.ledger.record_failure(
                self.database, operation_id, self.ACTION_DELETE, ledger_payload, str(exc)
            )
            raise

    def clear(
        self,
        operation_id: str,
        expected_catalog_version: str,
        *,
        occurred_at: str | None = None,
    ) -> CompensationDefinitionMutation:
        operation_id = str(operation_id).strip()
        expected_catalog_version = str(expected_catalog_version).strip()
        if not operation_id or not expected_catalog_version:
            raise ValueError("operation_id and catalog version are required")
        ledger_payload = {"scope": "all"}
        legacy_payload = json.dumps(
            ["clear", expected_catalog_version],
            ensure_ascii=True,
            separators=(",", ":"),
        )
        occurred_at = str(occurred_at or datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        if not self.database.is_file():
            return CompensationDefinitionMutation("schema_missing", operation_id, "clear")

        try:
            with DatabaseUnitOfWork(self.database, immediate=True) as uow:
                if not self._schema_ready(uow):
                    return CompensationDefinitionMutation("schema_missing", operation_id, "clear")
                legacy = self._legacy_operation(uow, operation_id)
                if legacy is not None:
                    if str(legacy["action"]) != "clear":
                        return CompensationDefinitionMutation(
                            "operation_conflict", operation_id, "clear"
                        )
                    return self._legacy_result(legacy, "clear")
                try:
                    existing = self.ledger.begin(
                        uow, operation_id, self.ACTION_CLEAR, ledger_payload
                    )
                except OperationConflictError:
                    return CompensationDefinitionMutation(
                        "operation_conflict", operation_id, "clear"
                    )
                if existing is not None:
                    return self._outcome_result(existing.outcome(), self.ACTION_CLEAR)

                rows = uow.query_all(
                    "SELECT record_id,version FROM compensation_definitions "
                    "ORDER BY record_id"
                )
                actual_catalog_version = self._catalog_version(
                    [(row["record_id"], row["version"]) for row in rows]
                )
                if actual_catalog_version != expected_catalog_version:
                    result = CompensationDefinitionMutation(
                        "definition_changed", operation_id, "clear"
                    )
                    self._finish(uow, result, self.ACTION_CLEAR)
                    return result

                removed_claims = int(
                    uow.execute(
                        "SELECT COUNT(*) FROM reward_claims WHERE reward_type='补偿'"
                    ).fetchone()[0]
                )
                removed_definitions = len(rows)
                uow.execute("DELETE FROM compensation_definitions")
                uow.execute("DELETE FROM reward_claims WHERE reward_type='补偿'")
                uow.execute("DELETE FROM reward_claim_counters WHERE reward_type='补偿'")
                result = CompensationDefinitionMutation(
                    "cleared",
                    operation_id,
                    "clear",
                    removed_definitions=removed_definitions,
                    removed_claims=removed_claims,
                )
                self._write_legacy_operation(
                    uow,
                    result,
                    legacy_payload,
                    occurred_at,
                )
                self._finish(uow, result, self.ACTION_CLEAR)
                return result
        except Exception as exc:
            self.ledger.record_failure(
                self.database, operation_id, self.ACTION_CLEAR, ledger_payload, str(exc)
            )
            raise


__all__ = [
    "CompensationDefinitionMutation",
    "CompensationDefinitionSqlRepository",
]
