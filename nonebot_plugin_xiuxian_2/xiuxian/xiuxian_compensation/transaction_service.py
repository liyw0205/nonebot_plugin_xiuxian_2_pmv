from __future__ import annotations

import json
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from threading import RLock
import hashlib
from datetime import datetime
from typing import Any, Mapping
from ..xiuxian_utils import db_backend
from typing import Any, Iterable

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
    record: dict[str, Any] | None = None

    @property
    def succeeded(self) -> bool:
        return self.status in {
            "created",
            "updated",
            "deleted",
            "cleared",
            "missing",
            "duplicate",
        }

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
            "removed_claims INTEGER NOT NULL DEFAULT 0,created_at TEXT NOT NULL,"
            "result_json TEXT NOT NULL DEFAULT '{}')"
        )
        if "result_json" not in conn.column_names(
            "compensation_definition_operations"
        ):
            conn.execute(
                "ALTER TABLE compensation_definition_operations ADD COLUMN "
                "result_json TEXT NOT NULL DEFAULT '{}'"
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
        record = json.loads(str(row[8])) if str(row[8]).strip() else None
        if not isinstance(record, dict) or not record:
            record = None
        return CompensationMutationResult(
            status,
            str(row[0]),
            str(row[2]),
            str(row[3]),
            int(row[4]),
            int(row[6]),
            int(row[7]),
            record,
        )

    @staticmethod
    def _operation(conn, operation_id: str):
        return conn.execute(
            "SELECT operation_id,payload,action,record_id,version,outcome,"
            "removed_definitions,removed_claims,result_json "
            "FROM compensation_definition_operations WHERE operation_id=%s",
            (operation_id,),
        ).fetchone()

    @staticmethod
    def _upsert_payload(request_identity) -> str:
        return json.dumps(
            ["upsert", str(request_identity).strip()],
            ensure_ascii=True,
            separators=(",", ":"),
        )

    def replay_upsert(
        self,
        operation_id,
        request_identity,
        *,
        occurred_at=None,
    ) -> CompensationMutationResult | None:
        operation_id = str(operation_id).strip()
        request_identity = str(request_identity).strip()
        occurred_at = self._now(occurred_at)
        if not operation_id or not request_identity:
            raise ValueError("operation and request identity are required")
        payload = self._upsert_payload(request_identity)

        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._prepare(conn, occurred_at)
                previous = self._operation(conn, operation_id)
                if previous is None:
                    conn.commit()
                    return None
                if str(previous[1]) != payload:
                    conn.commit()
                    return CompensationMutationResult(
                        "operation_conflict", operation_id, "upsert"
                    )
                result = self._operation_result(previous)
                conn.commit()
                return result
            except Exception:
                conn.rollback()
                raise

    def upsert(
        self,
        operation_id,
        request_identity,
        record_id,
        record: Mapping[str, Any],
        expected_version: int | None = None,
        *,
        occurred_at=None,
    ) -> CompensationMutationResult:
        operation_id = str(operation_id).strip()
        request_identity = str(request_identity).strip()
        record_id = str(record_id).strip()
        expected_version = (
            None if expected_version in (None, "") else int(expected_version)
        )
        occurred_at = self._now(occurred_at)
        if not operation_id or not request_identity or not record_id:
            raise ValueError("operation, request identity and compensation id are required")
        if not isinstance(record, Mapping):
            raise ValueError("valid compensation definition is required")
        payload = self._upsert_payload(request_identity)
        normalized, record_payload = self._canonical_record(record)

        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._prepare(conn, occurred_at)
                previous = self._operation(conn, operation_id)
                if previous is not None:
                    if str(previous[1]) != payload:
                        conn.commit()
                        return CompensationMutationResult(
                            "operation_conflict", operation_id, "upsert", record_id
                        )
                    result = self._operation_result(previous)
                    conn.commit()
                    return result

                current = conn.execute(
                    "SELECT version FROM compensation_definitions WHERE record_id=%s",
                    (record_id,),
                ).fetchone()
                if current is not None and expected_version is None:
                    conn.commit()
                    return CompensationMutationResult(
                        "version_required", operation_id, "upsert", record_id
                    )
                if current is None and expected_version not in (None, 0):
                    conn.commit()
                    return CompensationMutationResult(
                        "definition_changed",
                        operation_id,
                        "upsert",
                        record_id,
                        expected_version,
                    )
                if current is not None and int(current[0]) != expected_version:
                    conn.commit()
                    return CompensationMutationResult(
                        "definition_changed",
                        operation_id,
                        "upsert",
                        record_id,
                        int(current[0]),
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
                outcome = "created" if current is None else "updated"
                if current is None:
                    conn.execute(
                        "INSERT INTO compensation_definitions("
                        "record_id,version,record_json,created_at,updated_at) "
                        "VALUES(%s,%s,%s,%s,%s)",
                        (
                            record_id,
                            next_version,
                            record_payload,
                            occurred_at,
                            occurred_at,
                        ),
                    )
                else:
                    conn.execute(
                        "UPDATE compensation_definitions SET version=%s,record_json=%s,"
                        "updated_at=%s WHERE record_id=%s AND version=%s",
                        (
                            next_version,
                            record_payload,
                            occurred_at,
                            record_id,
                            expected_version,
                        ),
                    )

                result_record = dict(normalized)
                result_record["_definition_version"] = next_version
                result_json = json.dumps(
                    result_record,
                    ensure_ascii=True,
                    sort_keys=True,
                    separators=(",", ":"),
                )
                conn.execute(
                    "INSERT INTO compensation_definition_operations("
                    "operation_id,payload,action,record_id,version,outcome,created_at,"
                    "result_json) VALUES(%s,%s,'upsert',%s,%s,%s,%s,%s)",
                    (
                        operation_id,
                        payload,
                        record_id,
                        next_version,
                        outcome,
                        occurred_at,
                        result_json,
                    ),
                )
                conn.commit()
                return CompensationMutationResult(
                    outcome,
                    operation_id,
                    "upsert",
                    record_id,
                    next_version,
                    record=result_record,
                )
            except Exception:
                conn.rollback()
                raise

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

@dataclass(frozen=True)
class RewardClaim:
    status: str
    reward_type: str
    record_id: str
    user_id: str
    used_count: int = 0

    @property
    def applied(self) -> bool:
        return self.status == "claimed"

class RewardClaimService:
    def __init__(
        self,
        database: str | Path,
        *,
        max_goods_num: int,
        lock: RLock | None = None,
    ) -> None:
        self._database = Path(database)
        self._max_goods_num = int(max_goods_num)
        self._lock = lock or RLock()

    @staticmethod
    def _ensure_claims(conn) -> None:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS reward_claims (
                reward_type TEXT NOT NULL,
                record_id TEXT NOT NULL,
                user_id TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (reward_type, record_id, user_id)
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS reward_claim_counters (
                reward_type TEXT NOT NULL,
                record_id TEXT NOT NULL,
                baseline_count INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY (reward_type, record_id)
            )
            """
        )

    @staticmethod
    def _inventory_type(goods_type: str) -> str:
        if goods_type in {"辅修功法", "神通", "功法", "身法", "瞳术"}:
            return "技能"
        if goods_type in {"法器", "防具"}:
            return "装备"
        return goods_type

    def has_claimed(self, reward_type, record_id, user_id) -> bool:
        with self._lock, closing(db_backend.connect(self._database)) as conn:
            self._ensure_claims(conn)
            row = conn.execute(
                "SELECT 1 FROM reward_claims "
                "WHERE reward_type=%s AND record_id=%s AND user_id=%s",
                (str(reward_type), str(record_id), str(user_id)),
            ).fetchone()
            conn.commit()
            return row is not None

    def claim(
        self,
        reward_type,
        record_id,
        user_id,
        reward_items: Iterable[dict[str, Any]],
        *,
        usage_limit: int = 0,
        legacy_used_count: int = 0,
        expected_definition_version: int | None = None,
    ) -> RewardClaim:
        reward_type = str(reward_type)
        record_id = str(record_id)
        user_id = str(user_id)
        normalized_items = list(reward_items)
        usage_limit = max(int(usage_limit or 0), 0)
        legacy_used_count = max(int(legacy_used_count or 0), 0)
        expected_definition_version = (
            None
            if expected_definition_version in (None, "")
            else int(expected_definition_version)
        )

        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_claims(conn)
                if expected_definition_version is not None:
                    if not conn.table_exists("compensation_definitions"):
                        conn.rollback()
                        return RewardClaim(
                            "record_missing", reward_type, record_id, user_id
                        )
                    definition = conn.execute(
                        "SELECT version FROM compensation_definitions WHERE record_id=%s",
                        (record_id,),
                    ).fetchone()
                    if definition is None:
                        conn.rollback()
                        return RewardClaim(
                            "record_missing", reward_type, record_id, user_id
                        )
                    if int(definition[0]) != expected_definition_version:
                        conn.rollback()
                        return RewardClaim(
                            "definition_changed", reward_type, record_id, user_id
                        )
                user = conn.execute(
                    "SELECT 1 FROM user_xiuxian WHERE user_id=%s", (user_id,)
                ).fetchone()
                if user is None:
                    conn.rollback()
                    return RewardClaim("user_missing", reward_type, record_id, user_id)
                previous = conn.execute(
                    "SELECT 1 FROM reward_claims "
                    "WHERE reward_type=%s AND record_id=%s AND user_id=%s",
                    (reward_type, record_id, user_id),
                ).fetchone()
                if previous:
                    used_count = self._used_count(conn, reward_type, record_id)
                    conn.rollback()
                    return RewardClaim(
                        "duplicate", reward_type, record_id, user_id, used_count
                    )

                if usage_limit:
                    conn.execute(
                        "INSERT INTO reward_claim_counters "
                        "(reward_type, record_id, baseline_count) VALUES (%s, %s, %s) "
                        "ON CONFLICT (reward_type, record_id) DO NOTHING",
                        (reward_type, record_id, legacy_used_count),
                    )
                    used_count = self._used_count(conn, reward_type, record_id)
                    if used_count >= usage_limit:
                        conn.rollback()
                        return RewardClaim(
                            "exhausted", reward_type, record_id, user_id, used_count
                        )

                now = datetime.now().isoformat(sep=" ", timespec="seconds")
                for item in normalized_items:
                    quantity = max(int(item["quantity"]), 0)
                    if quantity <= 0:
                        continue
                    if item["type"] == "stone":
                        updated = conn.execute(
                            "UPDATE user_xiuxian SET stone=stone+%s WHERE user_id=%s",
                            (quantity, user_id),
                        )
                        if updated.rowcount != 1:
                            raise db_backend.IntegrityError("reward user disappeared")
                        continue

                    goods_id = int(item["id"])
                    goods_type = self._inventory_type(str(item["type"]))
                    quantity = min(quantity, self._max_goods_num)
                    conn.execute(
                        """
                        INSERT INTO back (
                            user_id, goods_id, goods_name, goods_type, goods_num,
                            create_time, update_time, bind_num
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (user_id, goods_id) DO UPDATE SET
                            goods_name=EXCLUDED.goods_name,
                            goods_type=EXCLUDED.goods_type,
                            update_time=EXCLUDED.update_time,
                            goods_num=LEAST(COALESCE(back.goods_num, 0)+EXCLUDED.goods_num, %s),
                            bind_num=LEAST(
                                COALESCE(back.bind_num, 0)+EXCLUDED.goods_num,
                                LEAST(COALESCE(back.goods_num, 0)+EXCLUDED.goods_num, %s),
                                %s
                            )
                        """,
                        (
                            user_id,
                            goods_id,
                            str(item["name"]),
                            goods_type,
                            quantity,
                            now,
                            now,
                            quantity,
                            self._max_goods_num,
                            self._max_goods_num,
                            self._max_goods_num,
                        ),
                    )

                conn.execute(
                    "INSERT INTO reward_claims (reward_type, record_id, user_id) "
                    "VALUES (%s, %s, %s)",
                    (reward_type, record_id, user_id),
                )
                conn.commit()
                return RewardClaim(
                    "claimed",
                    reward_type,
                    record_id,
                    user_id,
                    self._used_count(conn, reward_type, record_id),
                )
            except Exception:
                conn.rollback()
                raise

    def delete_claims(self, reward_type, record_id: str | None = None) -> None:
        with self._lock, closing(db_backend.connect(self._database)) as conn:
            self._ensure_claims(conn)
            if record_id is None:
                conn.execute(
                    "DELETE FROM reward_claims WHERE reward_type=%s",
                    (str(reward_type),),
                )
                conn.execute(
                    "DELETE FROM reward_claim_counters WHERE reward_type=%s",
                    (str(reward_type),),
                )
            else:
                conn.execute(
                    "DELETE FROM reward_claims WHERE reward_type=%s AND record_id=%s",
                    (str(reward_type), str(record_id)),
                )
                conn.execute(
                    "DELETE FROM reward_claim_counters "
                    "WHERE reward_type=%s AND record_id=%s",
                    (str(reward_type), str(record_id)),
                )
            conn.commit()

    @staticmethod
    def _used_count(conn, reward_type: str, record_id: str) -> int:
        row = conn.execute(
            "SELECT COALESCE(c.baseline_count, 0) + COUNT(r.user_id) "
            "FROM reward_claim_counters c "
            "LEFT JOIN reward_claims r ON r.reward_type=c.reward_type "
            "AND r.record_id=c.record_id "
            "WHERE c.reward_type=%s AND c.record_id=%s "
            "GROUP BY c.baseline_count",
            (reward_type, record_id),
        ).fetchone()
        if row:
            return int(row[0] or 0)
        row = conn.execute(
            "SELECT COUNT(*) FROM reward_claims "
            "WHERE reward_type=%s AND record_id=%s",
            (reward_type, record_id),
        ).fetchone()
        return int(row[0] or 0)

    def get_used_count(self, reward_type, record_id, legacy_used_count: int = 0) -> int:
        reward_type = str(reward_type)
        record_id = str(record_id)
        with self._lock, closing(db_backend.connect(self._database)) as conn:
            self._ensure_claims(conn)
            conn.execute(
                "INSERT INTO reward_claim_counters "
                "(reward_type, record_id, baseline_count) VALUES (%s, %s, %s) "
                "ON CONFLICT (reward_type, record_id) DO NOTHING",
                (reward_type, record_id, max(int(legacy_used_count or 0), 0)),
            )
            used_count = self._used_count(conn, reward_type, record_id)
            conn.commit()
            return used_count

@dataclass(frozen=True)
class InvitationRewardClaimResult:
    status: str
    thresholds: tuple[int, ...] = ()
    invitation_count: int = 0

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}

class InvitationRewardClaimService:
    """Claim one or more invitation milestones in one game-db transaction."""

    def __init__(self, database: str | Path, lock: RLock | None = None) -> None:
        self._database = Path(database)
        self._lock = lock or RLock()

    @staticmethod
    def _ensure_tables(conn) -> None:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS invitation_reward_invites ("
            "inviter_id TEXT NOT NULL,invited_id TEXT NOT NULL,source TEXT NOT NULL,"
            "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,"
            "PRIMARY KEY(inviter_id,invited_id))"
        )
        conn.execute(
            "CREATE TABLE IF NOT EXISTS invitation_reward_claims ("
            "user_id TEXT NOT NULL,threshold INTEGER NOT NULL,source TEXT NOT NULL,"
            "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,"
            "PRIMARY KEY(user_id,threshold))"
        )
        conn.execute(
            "CREATE TABLE IF NOT EXISTS invitation_reward_operations ("
            "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,"
            "thresholds_json TEXT NOT NULL,invitation_count INTEGER NOT NULL,"
            "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
        )

    @staticmethod
    def _inventory_type(item_type: str) -> str:
        if item_type in {"辅修功法", "神通", "功法", "身法", "瞳术"}:
            return "技能"
        if item_type in {"法器", "防具"}:
            return "装备"
        return item_type

    @classmethod
    def _normalize_rewards(cls, rewards_by_threshold):
        normalized = {}
        for raw_threshold, rewards in rewards_by_threshold.items():
            threshold = int(raw_threshold)
            if threshold <= 0:
                raise ValueError("invitation threshold must be positive")
            rows = []
            for reward in rewards:
                quantity = int(reward["quantity"])
                if quantity <= 0:
                    raise ValueError("reward quantity must be positive")
                if str(reward["type"]) == "stone":
                    rows.append(("stone", "stone", "灵石", "stone", quantity))
                    continue
                rows.append(
                    (
                        "item",
                        int(reward["id"]),
                        str(reward["name"]),
                        cls._inventory_type(str(reward["type"])),
                        quantity,
                    )
                )
            normalized[threshold] = tuple(rows)
        return normalized

    def claimed_thresholds(self, user_id) -> set[int]:
        with self._lock, closing(db_backend.connect(self._database)) as conn:
            self._ensure_tables(conn)
            rows = conn.execute(
                "SELECT threshold FROM invitation_reward_claims WHERE user_id=%s",
                (str(user_id),),
            ).fetchall()
            conn.commit()
            return {int(row[0]) for row in rows}

    def get_result(self, operation_id: str) -> InvitationRewardClaimResult | None:
        operation_id = str(operation_id).strip()
        if not operation_id:
            return None
        with self._lock, closing(db_backend.connect(self._database)) as conn:
            self._ensure_tables(conn)
            previous = conn.execute(
                "SELECT payload,thresholds_json,invitation_count "
                "FROM invitation_reward_operations WHERE operation_id=%s",
                (operation_id,),
            ).fetchone()
            conn.commit()
            if previous is None:
                return None
            return InvitationRewardClaimResult(
                "duplicate",
                tuple(int(value) for value in json.loads(str(previous[1]))),
                int(previous[2]),
            )

    def claim(
        self,
        operation_id,
        user_id,
        invited_user_ids,
        rewards_by_threshold,
        requested_thresholds,
        legacy_claimed_thresholds,
        max_goods_num,
    ) -> InvitationRewardClaimResult:
        operation_id = str(operation_id).strip()
        user_id = str(user_id)
        max_goods_num = int(max_goods_num)
        if not operation_id or max_goods_num < 0:
            raise ValueError("valid invitation reward claim is required")

        invited_ids = tuple(
            sorted(
                {
                    str(invited_id).strip()
                    for invited_id in invited_user_ids
                    if str(invited_id).strip() and str(invited_id).strip() != user_id
                }
            )
        )
        rewards = self._normalize_rewards(rewards_by_threshold)
        requested = tuple(sorted({int(value) for value in requested_thresholds}))
        legacy_claimed = tuple(
            sorted({int(value) for value in legacy_claimed_thresholds if int(value) > 0})
        )
        payload = json.dumps(
            [user_id, requested],
            ensure_ascii=True,
            separators=(",", ":"),
        )

        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_tables(conn)
                for invited_id in invited_ids:
                    conn.execute(
                        "INSERT INTO invitation_reward_invites(inviter_id,invited_id,source) "
                        "VALUES (%s,%s,'legacy_json') "
                        "ON CONFLICT(inviter_id,invited_id) DO NOTHING",
                        (user_id, invited_id),
                    )
                for threshold in legacy_claimed:
                    conn.execute(
                        "INSERT INTO invitation_reward_claims(user_id,threshold,source) "
                        "VALUES (%s,%s,'legacy_json') ON CONFLICT(user_id,threshold) DO NOTHING",
                        (user_id, threshold),
                    )

                previous = conn.execute(
                    "SELECT payload,thresholds_json,invitation_count "
                    "FROM invitation_reward_operations WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.commit()
                    if str(previous[0]) != payload:
                        return InvitationRewardClaimResult("operation_conflict")
                    return InvitationRewardClaimResult(
                        "duplicate",
                        tuple(int(value) for value in json.loads(str(previous[1]))),
                        int(previous[2]),
                    )

                user = conn.execute(
                    "SELECT 1 FROM user_xiuxian WHERE user_id=%s", (user_id,)
                ).fetchone()
                if user is None:
                    conn.commit()
                    return InvitationRewardClaimResult("user_missing")

                invitation_count = int(
                    conn.execute(
                        "SELECT COUNT(*) FROM invitation_reward_invites WHERE inviter_id=%s",
                        (user_id,),
                    ).fetchone()[0]
                )
                claimed = {
                    int(row[0])
                    for row in conn.execute(
                        "SELECT threshold FROM invitation_reward_claims WHERE user_id=%s",
                        (user_id,),
                    ).fetchall()
                }
                eligible = tuple(
                    threshold
                    for threshold in requested
                    if threshold in rewards
                    and threshold <= invitation_count
                    and threshold not in claimed
                )
                if not eligible:
                    conn.commit()
                    return InvitationRewardClaimResult(
                        "no_available", invitation_count=invitation_count
                    )

                stone = 0
                items = {}
                for threshold in eligible:
                    for kind, item_id, name, item_type, quantity in rewards[threshold]:
                        if kind == "stone":
                            stone += quantity
                            continue
                        metadata = [name, item_type]
                        if item_id in items and items[item_id][:2] != metadata:
                            raise ValueError("conflicting reward metadata")
                        items.setdefault(item_id, metadata + [0])[2] += quantity

                for item_id, (_, _, quantity) in items.items():
                    current = conn.execute(
                        "SELECT COALESCE(goods_num,0) FROM back "
                        "WHERE user_id=%s AND goods_id=%s",
                        (user_id, item_id),
                    ).fetchone()
                    if (int(current[0]) if current else 0) + quantity > max_goods_num:
                        conn.rollback()
                        return InvitationRewardClaimResult(
                            "inventory_full", invitation_count=invitation_count
                        )

                now = datetime.now()
                if stone:
                    changed = conn.execute(
                        "UPDATE user_xiuxian SET stone=COALESCE(stone,0)+%s "
                        "WHERE user_id=%s",
                        (stone, user_id),
                    )
                    if changed.rowcount != 1:
                        raise db_backend.IntegrityError("invitation reward user disappeared")
                for item_id, (name, item_type, quantity) in items.items():
                    conn.execute(
                        "INSERT INTO back(user_id,goods_id,goods_name,goods_type,goods_num,"
                        "create_time,update_time,bind_num) VALUES (%s,%s,%s,%s,%s,%s,%s,%s) "
                        "ON CONFLICT(user_id,goods_id) DO UPDATE SET "
                        "goods_name=excluded.goods_name,goods_type=excluded.goods_type,"
                        "goods_num=back.goods_num+excluded.goods_num,"
                        "bind_num=COALESCE(back.bind_num,0)+excluded.bind_num,"
                        "update_time=excluded.update_time",
                        (user_id, item_id, name, item_type, quantity, now, now, quantity),
                    )
                for threshold in eligible:
                    conn.execute(
                        "INSERT INTO invitation_reward_claims(user_id,threshold,source) "
                        "VALUES (%s,%s,'transaction')",
                        (user_id, threshold),
                    )
                conn.execute(
                    "INSERT INTO invitation_reward_operations(operation_id,payload,"
                    "thresholds_json,invitation_count) VALUES (%s,%s,%s,%s)",
                    (
                        operation_id,
                        payload,
                        json.dumps(eligible, separators=(",", ":")),
                        invitation_count,
                    ),
                )
                conn.commit()
                return InvitationRewardClaimResult("applied", eligible, invitation_count)
            except Exception:
                conn.rollback()
                raise

__all__ = [
    "CompensationDefinitionConflict",
    "CompensationDefinition",
    "CompensationMutationResult",
    "CompensationDefinitionService",
    "RewardClaim",
    "RewardClaimService",
    "InvitationRewardClaimResult",
    "InvitationRewardClaimService",
]
