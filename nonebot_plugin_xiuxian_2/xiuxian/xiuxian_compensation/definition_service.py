from __future__ import annotations

import hashlib
import json
from contextlib import closing
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from threading import RLock
from typing import Any, Mapping

from ..xiuxian_utils import db_backend


class CompensationDefinitionConflict(RuntimeError):
    pass


@dataclass(frozen=True)
class CompensationDefinition:
    record_id: str
    version: int
    record: dict[str, Any]


@dataclass(frozen=True)
class CompensationMutationResult:
    status: str
    operation_id: str
    action: str
    record_id: str = ""
    version: int = 0
    removed_definitions: int = 0
    removed_claims: int = 0

    @property
    def succeeded(self) -> bool:
        return self.status in {"deleted", "cleared", "missing", "duplicate"}


class CompensationDefinitionService:
    """Database source of truth for versioned compensation definitions."""

    _MIGRATION_KEY = "legacy-compensation-json-v1"

    def __init__(
        self,
        database: str | Path,
        legacy_definitions_path: str | Path | None = None,
        legacy_claims_path: str | Path | None = None,
        *,
        lock: RLock | None = None,
    ) -> None:
        self._database = Path(database)
        self._legacy_definitions_path = (
            None
            if legacy_definitions_path is None
            else Path(legacy_definitions_path)
        )
        self._legacy_claims_path = (
            None if legacy_claims_path is None else Path(legacy_claims_path)
        )
        self._lock = lock or RLock()

    @staticmethod
    def _now(value=None) -> str:
        if value is None:
            return datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        if isinstance(value, datetime):
            return value.strftime("%Y-%m-%d %H:%M:%S")
        return str(value).strip()

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
    def _load_json_dict(path: Path | None) -> dict:
        if path is None or not path.is_file():
            return {}
        try:
            with path.open("r", encoding="utf-8") as file:
                value = json.load(file)
        except (OSError, ValueError, TypeError):
            return {}
        return value if isinstance(value, dict) else {}

    @staticmethod
    def _ensure_schema(conn) -> None:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS compensation_definition_revisions("
            "record_id TEXT PRIMARY KEY,last_version INTEGER NOT NULL)"
        )
        conn.execute(
            "CREATE TABLE IF NOT EXISTS compensation_definitions("
            "record_id TEXT PRIMARY KEY,version INTEGER NOT NULL,record_json TEXT NOT NULL,"
            "created_at TEXT NOT NULL,updated_at TEXT NOT NULL)"
        )
        conn.execute(
            "CREATE TABLE IF NOT EXISTS compensation_definition_operations("
            "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,action TEXT NOT NULL,"
            "record_id TEXT NOT NULL DEFAULT '',version INTEGER NOT NULL DEFAULT 0,"
            "outcome TEXT NOT NULL,removed_definitions INTEGER NOT NULL DEFAULT 0,"
            "removed_claims INTEGER NOT NULL DEFAULT 0,created_at TEXT NOT NULL)"
        )
        conn.execute(
            "CREATE TABLE IF NOT EXISTS compensation_legacy_migrations("
            "migration_key TEXT PRIMARY KEY,definitions_payload TEXT NOT NULL,"
            "claims_payload TEXT NOT NULL,migrated_at TEXT NOT NULL)"
        )
        conn.execute(
            "CREATE TABLE IF NOT EXISTS reward_claims("
            "reward_type TEXT NOT NULL,record_id TEXT NOT NULL,user_id TEXT NOT NULL,"
            "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,"
            "PRIMARY KEY(reward_type,record_id,user_id))"
        )
        conn.execute(
            "CREATE TABLE IF NOT EXISTS reward_claim_counters("
            "reward_type TEXT NOT NULL,record_id TEXT NOT NULL,"
            "baseline_count INTEGER NOT NULL DEFAULT 0,"
            "PRIMARY KEY(reward_type,record_id))"
        )

    def _migrate_legacy(self, conn, migrated_at: str) -> None:
        migrated = conn.execute(
            "SELECT 1 FROM compensation_legacy_migrations WHERE migration_key=%s",
            (self._MIGRATION_KEY,),
        ).fetchone()
        if migrated is not None:
            return

        definitions = self._load_json_dict(self._legacy_definitions_path)
        claims = self._load_json_dict(self._legacy_claims_path)
        for record_id, record in definitions.items():
            record_id = str(record_id).strip()
            if not record_id or not isinstance(record, dict):
                continue
            _, payload = self._canonical_record(record)
            conn.execute(
                "INSERT INTO compensation_definition_revisions(record_id,last_version) "
                "VALUES(%s,1) ON CONFLICT(record_id) DO NOTHING",
                (record_id,),
            )
            conn.execute(
                "INSERT INTO compensation_definitions("
                "record_id,version,record_json,created_at,updated_at) "
                "VALUES(%s,1,%s,%s,%s) ON CONFLICT(record_id) DO NOTHING",
                (record_id, payload, migrated_at, migrated_at),
            )

        for user_id, record_ids in claims.items():
            if not isinstance(record_ids, (list, tuple, set)):
                continue
            user_id = str(user_id).strip()
            if not user_id:
                continue
            for record_id in dict.fromkeys(str(value).strip() for value in record_ids):
                if not record_id:
                    continue
                conn.execute(
                    "INSERT INTO reward_claims(reward_type,record_id,user_id,created_at) "
                    "VALUES('补偿',%s,%s,%s) "
                    "ON CONFLICT(reward_type,record_id,user_id) DO NOTHING",
                    (record_id, user_id, migrated_at),
                )

        conn.execute(
            "INSERT INTO compensation_legacy_migrations("
            "migration_key,definitions_payload,claims_payload,migrated_at) "
            "VALUES(%s,%s,%s,%s)",
            (
                self._MIGRATION_KEY,
                json.dumps(
                    definitions,
                    ensure_ascii=True,
                    sort_keys=True,
                    separators=(",", ":"),
                ),
                json.dumps(
                    claims,
                    ensure_ascii=True,
                    sort_keys=True,
                    separators=(",", ":"),
                ),
                migrated_at,
            ),
        )

    def _prepare(self, conn, occurred_at: str) -> None:
        self._ensure_schema(conn)
        self._migrate_legacy(conn, occurred_at)

    @staticmethod
    def _definition(row) -> CompensationDefinition | None:
        if row is None:
            return None
        record = json.loads(str(row[2]))
        record["_definition_version"] = int(row[1])
        return CompensationDefinition(str(row[0]), int(row[1]), record)

    @staticmethod
    def _catalog_version_rows(rows) -> str:
        payload = json.dumps(
            [(str(row[0]), int(row[1])) for row in rows],
            ensure_ascii=True,
            separators=(",", ":"),
        )
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()

    def get(self, record_id, *, occurred_at=None) -> CompensationDefinition | None:
        record_id = str(record_id).strip()
        occurred_at = self._now(occurred_at)
        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._prepare(conn, occurred_at)
                row = conn.execute(
                    "SELECT record_id,version,record_json FROM compensation_definitions "
                    "WHERE record_id=%s",
                    (record_id,),
                ).fetchone()
                result = self._definition(row)
                conn.commit()
                return result
            except Exception:
                conn.rollback()
                raise

    def list(self, *, occurred_at=None) -> dict[str, dict[str, Any]]:
        occurred_at = self._now(occurred_at)
        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._prepare(conn, occurred_at)
                rows = conn.execute(
                    "SELECT record_id,version,record_json FROM compensation_definitions "
                    "ORDER BY record_id"
                ).fetchall()
                result = {
                    definition.record_id: definition.record
                    for definition in (self._definition(row) for row in rows)
                }
                conn.commit()
                return result
            except Exception:
                conn.rollback()
                raise

    def claimed_data(self, *, occurred_at=None) -> dict[str, list[str]]:
        occurred_at = self._now(occurred_at)
        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._prepare(conn, occurred_at)
                rows = conn.execute(
                    "SELECT user_id,record_id FROM reward_claims "
                    "WHERE reward_type='补偿' ORDER BY user_id,record_id"
                ).fetchall()
                result: dict[str, list[str]] = {}
                for row in rows:
                    result.setdefault(str(row[0]), []).append(str(row[1]))
                conn.commit()
                return result
            except Exception:
                conn.rollback()
                raise

    def sync(
        self,
        definitions: Mapping[str, Mapping[str, Any]],
        *,
        occurred_at=None,
    ) -> dict[str, dict[str, Any]]:
        occurred_at = self._now(occurred_at)
        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._prepare(conn, occurred_at)
                for raw_record_id, raw_record in definitions.items():
                    record_id = str(raw_record_id).strip()
                    if not record_id or not isinstance(raw_record, Mapping):
                        raise ValueError("valid compensation definitions are required")
                    expected = raw_record.get("_definition_version")
                    expected_version = None if expected in (None, "") else int(expected)
                    _, payload = self._canonical_record(raw_record)
                    current = conn.execute(
                        "SELECT version,record_json FROM compensation_definitions "
                        "WHERE record_id=%s",
                        (record_id,),
                    ).fetchone()
                    if current is not None and str(current[1]) == payload:
                        continue
                    if current is not None and expected_version != int(current[0]):
                        raise CompensationDefinitionConflict(
                            f"compensation {record_id} definition changed"
                        )
                    if current is None and expected_version not in (None, 0):
                        raise CompensationDefinitionConflict(
                            f"compensation {record_id} definition was removed"
                        )

                    revision = conn.execute(
                        "SELECT last_version FROM compensation_definition_revisions "
                        "WHERE record_id=%s",
                        (record_id,),
                    ).fetchone()
                    next_version = 1 if revision is None else int(revision[0]) + 1
                    conn.execute(
                        "INSERT INTO compensation_definition_revisions(record_id,last_version) "
                        "VALUES(%s,%s) ON CONFLICT(record_id) DO UPDATE SET "
                        "last_version=EXCLUDED.last_version",
                        (record_id, next_version),
                    )
                    if current is None:
                        conn.execute(
                            "INSERT INTO compensation_definitions("
                            "record_id,version,record_json,created_at,updated_at) "
                            "VALUES(%s,%s,%s,%s,%s)",
                            (record_id, next_version, payload, occurred_at, occurred_at),
                        )
                    else:
                        conn.execute(
                            "UPDATE compensation_definitions SET version=%s,record_json=%s,"
                            "updated_at=%s WHERE record_id=%s AND version=%s",
                            (
                                next_version,
                                payload,
                                occurred_at,
                                record_id,
                                int(current[0]),
                            ),
                        )
                conn.commit()
                return self.list(occurred_at=occurred_at)
            except Exception:
                conn.rollback()
                raise

    @staticmethod
    def _operation_result(row, status="duplicate") -> CompensationMutationResult:
        return CompensationMutationResult(
            status,
            str(row[0]),
            str(row[2]),
            str(row[3]),
            int(row[4]),
            int(row[6]),
            int(row[7]),
        )

    @staticmethod
    def _operation(conn, operation_id: str):
        return conn.execute(
            "SELECT operation_id,payload,action,record_id,version,outcome,"
            "removed_definitions,removed_claims "
            "FROM compensation_definition_operations WHERE operation_id=%s",
            (operation_id,),
        ).fetchone()

    def catalog_version(self, *, occurred_at=None) -> str:
        occurred_at = self._now(occurred_at)
        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._prepare(conn, occurred_at)
                rows = conn.execute(
                    "SELECT record_id,version FROM compensation_definitions ORDER BY record_id"
                ).fetchall()
                result = self._catalog_version_rows(rows)
                conn.commit()
                return result
            except Exception:
                conn.rollback()
                raise

    def delete(
        self,
        operation_id,
        record_id,
        expected_version: int | None,
        *,
        occurred_at=None,
    ) -> CompensationMutationResult:
        operation_id = str(operation_id).strip()
        record_id = str(record_id).strip()
        expected_version = (
            None if expected_version in (None, "") else int(expected_version)
        )
        occurred_at = self._now(occurred_at)
        if not operation_id or not record_id:
            raise ValueError("operation and compensation id are required")
        payload = json.dumps(
            ["delete", record_id, expected_version],
            ensure_ascii=True,
            separators=(",", ":"),
        )

        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._prepare(conn, occurred_at)
                previous = self._operation(conn, operation_id)
                if previous is not None:
                    if str(previous[1]) != payload:
                        conn.commit()
                        return CompensationMutationResult(
                            "operation_conflict", operation_id, "delete", record_id
                        )
                    result = self._operation_result(previous)
                    conn.commit()
                    return result

                definition = conn.execute(
                    "SELECT version FROM compensation_definitions WHERE record_id=%s",
                    (record_id,),
                ).fetchone()
                if definition is not None and expected_version is None:
                    conn.commit()
                    return CompensationMutationResult(
                        "version_required", operation_id, "delete", record_id
                    )
                if definition is None and expected_version is not None:
                    conn.commit()
                    return CompensationMutationResult(
                        "definition_changed",
                        operation_id,
                        "delete",
                        record_id,
                        expected_version,
                    )
                if definition is not None and int(definition[0]) != expected_version:
                    conn.commit()
                    return CompensationMutationResult(
                        "definition_changed",
                        operation_id,
                        "delete",
                        record_id,
                        int(definition[0]),
                    )

                removed_definitions = int(definition is not None)
                claim_count = int(
                    conn.execute(
                        "SELECT COUNT(*) FROM reward_claims "
                        "WHERE reward_type='补偿' AND record_id=%s",
                        (record_id,),
                    ).fetchone()[0]
                )
                if definition is not None:
                    conn.execute(
                        "DELETE FROM compensation_definitions "
                        "WHERE record_id=%s AND version=%s",
                        (record_id, expected_version),
                    )
                conn.execute(
                    "DELETE FROM reward_claims WHERE reward_type='补偿' AND record_id=%s",
                    (record_id,),
                )
                conn.execute(
                    "DELETE FROM reward_claim_counters "
                    "WHERE reward_type='补偿' AND record_id=%s",
                    (record_id,),
                )
                outcome = "deleted" if removed_definitions else "missing"
                conn.execute(
                    "INSERT INTO compensation_definition_operations("
                    "operation_id,payload,action,record_id,version,outcome,"
                    "removed_definitions,removed_claims,created_at) "
                    "VALUES(%s,%s,'delete',%s,%s,%s,%s,%s,%s)",
                    (
                        operation_id,
                        payload,
                        record_id,
                        expected_version or 0,
                        outcome,
                        removed_definitions,
                        claim_count,
                        occurred_at,
                    ),
                )
                conn.commit()
                return CompensationMutationResult(
                    outcome,
                    operation_id,
                    "delete",
                    record_id,
                    expected_version or 0,
                    removed_definitions,
                    claim_count,
                )
            except Exception:
                conn.rollback()
                raise

    def clear(
        self,
        operation_id,
        expected_catalog_version,
        *,
        occurred_at=None,
    ) -> CompensationMutationResult:
        operation_id = str(operation_id).strip()
        expected_catalog_version = str(expected_catalog_version).strip()
        occurred_at = self._now(occurred_at)
        if not operation_id or not expected_catalog_version:
            raise ValueError("operation and catalog version are required")
        payload = json.dumps(
            ["clear", expected_catalog_version],
            ensure_ascii=True,
            separators=(",", ":"),
        )

        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._prepare(conn, occurred_at)
                previous = self._operation(conn, operation_id)
                if previous is not None:
                    if str(previous[1]) != payload:
                        conn.commit()
                        return CompensationMutationResult(
                            "operation_conflict", operation_id, "clear"
                        )
                    result = self._operation_result(previous)
                    conn.commit()
                    return result

                definitions = conn.execute(
                    "SELECT record_id,version FROM compensation_definitions ORDER BY record_id"
                ).fetchall()
                actual_catalog_version = self._catalog_version_rows(definitions)
                if actual_catalog_version != expected_catalog_version:
                    conn.commit()
                    return CompensationMutationResult(
                        "definition_changed", operation_id, "clear"
                    )
                claim_count = int(
                    conn.execute(
                        "SELECT COUNT(*) FROM reward_claims WHERE reward_type='补偿'"
                    ).fetchone()[0]
                )
                definition_count = len(definitions)
                conn.execute("DELETE FROM compensation_definitions")
                conn.execute("DELETE FROM reward_claims WHERE reward_type='补偿'")
                conn.execute("DELETE FROM reward_claim_counters WHERE reward_type='补偿'")
                conn.execute(
                    "INSERT INTO compensation_definition_operations("
                    "operation_id,payload,action,outcome,removed_definitions,"
                    "removed_claims,created_at) VALUES(%s,%s,'clear','cleared',%s,%s,%s)",
                    (
                        operation_id,
                        payload,
                        definition_count,
                        claim_count,
                        occurred_at,
                    ),
                )
                conn.commit()
                return CompensationMutationResult(
                    "cleared",
                    operation_id,
                    "clear",
                    removed_definitions=definition_count,
                    removed_claims=claim_count,
                )
            except Exception:
                conn.rollback()
                raise


__all__ = [
    "CompensationDefinition",
    "CompensationDefinitionConflict",
    "CompensationDefinitionService",
    "CompensationMutationResult",
]
