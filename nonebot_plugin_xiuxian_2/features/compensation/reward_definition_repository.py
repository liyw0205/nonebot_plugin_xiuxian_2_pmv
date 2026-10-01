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
class CompensationRewardDefinitionResult:
    status: str
    operation_id: str = ""
    reward_type: str = ""
    record_id: str = ""
    version: int = 0
    removed_definitions: int = 0
    removed_claims: int = 0
    removed_counters: int = 0
    record: dict[str, Any] | None = None
    replayed: bool = False

    @property
    def succeeded(self) -> bool:
        return self.status in {"created", "updated", "deleted", "cleared", "missing"}


class CompensationRewardDefinitionSqlRepository:
    ACTION_UPSERT = "compensation.reward_definition_upsert"
    ACTION_DELETE = "compensation.reward_definition_delete"
    ACTION_CLEAR = "compensation.reward_definition_clear"
    REWARD_TYPES = frozenset({"礼包", "兑换码"})

    def __init__(
        self,
        database: str | Path,
        *,
        ledger: OperationLedger | None = None,
    ) -> None:
        self.database = Path(database)
        self.ledger = ledger or OperationLedger()

    @classmethod
    def _reward_type(cls, reward_type: str) -> str:
        value = str(reward_type).strip()
        if value not in cls.REWARD_TYPES:
            raise ValueError("reward_type must be 礼包 or 兑换码")
        return value

    @staticmethod
    def _schema_ready(uow: DatabaseUnitOfWork) -> bool:
        required = {
            "compensation_reward_definitions",
            "compensation_reward_definition_revisions",
            "compensation_reward_catalog_migrations",
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
        return required.issubset(existing)

    @staticmethod
    def _record(row) -> dict[str, Any]:
        record = json.loads(str(row[2]))
        record["_definition_version"] = int(row[1])
        return record

    @staticmethod
    def _catalog_hash(rows) -> str:
        payload = json.dumps(
            [(str(row[0]), int(row[1])) for row in rows],
            ensure_ascii=True,
            separators=(",", ":"),
        )
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()

    @staticmethod
    def _mutation_from_outcome(outcome: OperationOutcome | None) -> CompensationRewardDefinitionResult:
        if outcome is None:
            return CompensationRewardDefinitionResult("in_progress")
        data = dict(outcome.data or {})
        return CompensationRewardDefinitionResult(
            status=str(data.get("status", outcome.code or outcome.status)),
            operation_id=str(outcome.operation_id),
            reward_type=str(data.get("reward_type", "")),
            record_id=str(data.get("record_id", "")),
            version=int(data.get("version", 0)),
            removed_definitions=int(data.get("removed_definitions", 0)),
            removed_claims=int(data.get("removed_claims", 0)),
            removed_counters=int(data.get("removed_counters", 0)),
            record=data.get("record"),
            replayed=outcome.replayed,
        )

    @staticmethod
    def _result_data(result: CompensationRewardDefinitionResult) -> dict[str, Any]:
        return {
            "status": result.status,
            "reward_type": result.reward_type,
            "record_id": result.record_id,
            "version": result.version,
            "removed_definitions": result.removed_definitions,
            "removed_claims": result.removed_claims,
            "removed_counters": result.removed_counters,
            "record": result.record,
        }

    def _finish(
        self,
        uow: DatabaseUnitOfWork,
        result: CompensationRewardDefinitionResult,
        action: str,
    ) -> None:
        data = self._result_data(result)
        if result.status in {"created", "updated", "deleted", "cleared", "missing"}:
            outcome = OperationOutcome.applied(
                result.operation_id,
                action,
                data=data,
                audit_category="compensation",
                after={key: data[key] for key in ("removed_definitions", "removed_claims", "removed_counters")},
            )
        else:
            outcome = OperationOutcome.rejected(
                result.operation_id,
                action,
                result.status,
                code=result.status,
                data=data,
                audit_category="compensation",
            )
        self.ledger.finish(uow, outcome)

    def list_definitions(self, reward_type: str) -> dict[str, dict[str, Any]]:
        reward_type = self._reward_type(reward_type)
        if not self.database.is_file():
            raise RuntimeError("compensation reward definition database is missing")
        with DatabaseUnitOfWork(self.database, read_only=True) as uow:
            if not self._schema_ready(uow):
                raise RuntimeError("compensation reward definition schema is missing")
            rows = uow.query_all(
                "SELECT record_id,version,record_json FROM compensation_reward_definitions "
                "WHERE reward_type=? ORDER BY record_id",
                (reward_type,),
            )
            return {str(row["record_id"]): self._record((row["record_id"], row["version"], row["record_json"])) for row in rows}

    def get_definition(
        self, reward_type: str, record_id: str
    ) -> dict[str, Any] | None:
        reward_type = self._reward_type(reward_type)
        record_id = str(record_id).strip()
        if not self.database.is_file():
            raise RuntimeError("compensation reward definition database is missing")
        with DatabaseUnitOfWork(self.database, read_only=True) as uow:
            if not self._schema_ready(uow):
                raise RuntimeError("compensation reward definition schema is missing")
            row = uow.query_one(
                "SELECT record_id,version,record_json "
                "FROM compensation_reward_definitions "
                "WHERE reward_type=? AND record_id=?",
                (reward_type, record_id),
            )
            return None if row is None else self._record(
                (row["record_id"], row["version"], row["record_json"])
            )

    def replay_upsert(
        self, operation_id: str, reward_type: str, request_identity: str
    ) -> CompensationRewardDefinitionResult | None:
        reward_type = self._reward_type(reward_type)
        payload = {
            "reward_type": reward_type,
            "request_identity": str(request_identity).strip(),
        }
        if not self.database.is_file():
            raise RuntimeError("compensation reward definition database is missing")
        with DatabaseUnitOfWork(self.database, read_only=True) as uow:
            if not self._schema_ready(uow):
                raise RuntimeError("compensation reward definition schema is missing")
            existing = self.ledger.get(uow, str(operation_id), self.ACTION_UPSERT)
            if existing is None:
                return None
            if existing.request_hash != request_hash(payload):
                return CompensationRewardDefinitionResult(
                    "operation_conflict", str(operation_id), reward_type
                )
            return self._mutation_from_outcome(existing.outcome())

    def upsert(
        self,
        operation_id: str,
        reward_type: str,
        record_id: str,
        request_identity: str,
        record: Mapping[str, Any],
        expected_version: int | None = None,
    ) -> CompensationRewardDefinitionResult:
        operation_id = str(operation_id).strip()
        reward_type = self._reward_type(reward_type)
        record_id = str(record_id).strip()
        request_identity = str(request_identity).strip()
        expected_version = (
            None if expected_version in (None, "") else int(expected_version)
        )
        if not operation_id or not record_id or not request_identity:
            raise ValueError("operation_id, record_id and request_identity are required")
        if not isinstance(record, Mapping):
            raise ValueError("valid reward definition is required")
        payload = {"reward_type": reward_type, "request_identity": request_identity}
        action = self.ACTION_UPSERT
        if not self.database.is_file():
            return CompensationRewardDefinitionResult("schema_missing", operation_id, reward_type, record_id)
        try:
            with DatabaseUnitOfWork(self.database, immediate=True) as uow:
                if not self._schema_ready(uow):
                    return CompensationRewardDefinitionResult("schema_missing", operation_id, reward_type, record_id)
                try:
                    existing = self.ledger.begin(uow, operation_id, action, payload)
                except OperationConflictError:
                    return CompensationRewardDefinitionResult("operation_conflict", operation_id, reward_type, record_id)
                if existing is not None:
                    return self._mutation_from_outcome(existing.outcome())

                current = uow.query_one(
                    "SELECT version FROM compensation_reward_definitions "
                    "WHERE reward_type=? AND record_id=?",
                    (reward_type, record_id),
                )
                current_version = None if current is None else int(current["version"])
                if current_version != expected_version:
                    result = CompensationRewardDefinitionResult(
                        "definition_changed", operation_id, reward_type, record_id,
                        current_version or 0,
                    )
                    self._finish(uow, result, action)
                    return result

                revision = uow.query_one(
                    "SELECT last_version FROM compensation_reward_definition_revisions "
                    "WHERE reward_type=? AND record_id=?",
                    (reward_type, record_id),
                )
                next_version = 1 if revision is None else int(revision["last_version"]) + 1
                uow.execute(
                    "INSERT INTO compensation_reward_definition_revisions("
                    "reward_type,record_id,last_version) VALUES(?,?,?) "
                    "ON CONFLICT(reward_type,record_id) DO UPDATE SET "
                    "last_version=excluded.last_version",
                    (reward_type, record_id, next_version),
                )
                normalized = {
                    str(key): value
                    for key, value in dict(record).items()
                    if str(key) != "_definition_version"
                }
                encoded = json.dumps(
                    normalized,
                    ensure_ascii=True,
                    sort_keys=True,
                    separators=(",", ":"),
                )
                now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                uow.execute(
                    "INSERT INTO compensation_reward_definitions("
                    "reward_type,record_id,version,record_json,created_at,updated_at) "
                    "VALUES(?,?,?,?,?,?) ON CONFLICT(reward_type,record_id) DO UPDATE SET "
                    "version=excluded.version,record_json=excluded.record_json,"
                    "updated_at=excluded.updated_at",
                    (reward_type, record_id, next_version, encoded, now, now),
                )
                normalized["_definition_version"] = next_version
                result = CompensationRewardDefinitionResult(
                    "created" if current is None else "updated",
                    operation_id,
                    reward_type,
                    record_id,
                    next_version,
                    record=normalized,
                )
                self._finish(uow, result, action)
                return result
        except Exception as exc:
            self.ledger.record_failure(self.database, operation_id, action, payload, str(exc))
            raise

    def delete(
        self, operation_id: str, reward_type: str, record_id: str
    ) -> CompensationRewardDefinitionResult:
        operation_id = str(operation_id).strip()
        reward_type = self._reward_type(reward_type)
        record_id = str(record_id).strip()
        if not operation_id or not record_id:
            raise ValueError("operation_id and record_id are required")
        action = self.ACTION_DELETE
        payload = {"reward_type": reward_type, "record_id": record_id}
        if not self.database.is_file():
            return CompensationRewardDefinitionResult("schema_missing", operation_id, reward_type, record_id)
        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            if not self._schema_ready(uow):
                return CompensationRewardDefinitionResult("schema_missing", operation_id, reward_type, record_id)
            try:
                existing = self.ledger.begin(uow, operation_id, action, payload)
            except OperationConflictError:
                return CompensationRewardDefinitionResult("operation_conflict", operation_id, reward_type, record_id)
            if existing is not None:
                return self._mutation_from_outcome(existing.outcome())
            definition = uow.query_one(
                "SELECT version FROM compensation_reward_definitions "
                "WHERE reward_type=? AND record_id=?",
                (reward_type, record_id),
            )
            removed_definitions = int(definition is not None)
            if definition is not None:
                uow.execute(
                    "DELETE FROM compensation_reward_definitions "
                    "WHERE reward_type=? AND record_id=?",
                    (reward_type, record_id),
                )
            deleted_claims = int(
                uow.execute(
                    "SELECT COUNT(*) FROM reward_claims "
                    "WHERE reward_type=? AND record_id=?",
                    (reward_type, record_id),
                ).fetchone()[0]
            )
            deleted_counters = int(
                uow.execute(
                    "SELECT COUNT(*) FROM reward_claim_counters "
                    "WHERE reward_type=? AND record_id=?",
                    (reward_type, record_id),
                ).fetchone()[0]
            )
            uow.execute(
                "DELETE FROM reward_claims WHERE reward_type=? AND record_id=?",
                (reward_type, record_id),
            )
            uow.execute(
                "DELETE FROM reward_claim_counters WHERE reward_type=? AND record_id=?",
                (reward_type, record_id),
            )
            result = CompensationRewardDefinitionResult(
                "deleted" if removed_definitions else "missing",
                operation_id,
                reward_type,
                record_id,
                int(definition["version"]) if definition else 0,
                removed_definitions,
                deleted_claims,
                deleted_counters,
            )
            self._finish(uow, result, action)
            return result

    def clear(
        self, operation_id: str, reward_type: str
    ) -> CompensationRewardDefinitionResult:
        operation_id = str(operation_id).strip()
        reward_type = self._reward_type(reward_type)
        if not operation_id:
            raise ValueError("operation_id is required")
        action = self.ACTION_CLEAR
        payload = {"reward_type": reward_type}
        if not self.database.is_file():
            return CompensationRewardDefinitionResult("schema_missing", operation_id, reward_type)
        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            if not self._schema_ready(uow):
                return CompensationRewardDefinitionResult("schema_missing", operation_id, reward_type)
            try:
                existing = self.ledger.begin(uow, operation_id, action, payload)
            except OperationConflictError:
                return CompensationRewardDefinitionResult("operation_conflict", operation_id, reward_type)
            if existing is not None:
                return self._mutation_from_outcome(existing.outcome())
            removed_definitions = int(
                uow.execute(
                    "SELECT COUNT(*) FROM compensation_reward_definitions "
                    "WHERE reward_type=?",
                    (reward_type,),
                ).fetchone()[0]
            )
            removed_claims = int(
                uow.execute(
                    "SELECT COUNT(*) FROM reward_claims WHERE reward_type=?",
                    (reward_type,),
                ).fetchone()[0]
            )
            removed_counters = int(
                uow.execute(
                    "SELECT COUNT(*) FROM reward_claim_counters WHERE reward_type=?",
                    (reward_type,),
                ).fetchone()[0]
            )
            uow.execute(
                "DELETE FROM compensation_reward_definitions WHERE reward_type=?",
                (reward_type,),
            )
            uow.execute("DELETE FROM reward_claims WHERE reward_type=?", (reward_type,))
            uow.execute(
                "DELETE FROM reward_claim_counters WHERE reward_type=?", (reward_type,)
            )
            result = CompensationRewardDefinitionResult(
                "cleared",
                operation_id,
                reward_type,
                removed_definitions=removed_definitions,
                removed_claims=removed_claims,
                removed_counters=removed_counters,
            )
            self._finish(uow, result, action)
            return result


__all__ = [
    "CompensationRewardDefinitionResult",
    "CompensationRewardDefinitionSqlRepository",
]
