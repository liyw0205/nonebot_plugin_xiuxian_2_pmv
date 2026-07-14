from __future__ import annotations

import json
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from threading import RLock
from typing import Any, Mapping

from ..xiuxian_utils import db_backend


@dataclass(frozen=True)
class ActivityConfigState:
    revision: int
    config: dict[str, Any]


@dataclass(frozen=True)
class ActivityConfigMutationResult:
    status: str
    revision: int = 0
    config: dict[str, Any] | None = None
    result_text: str = ""

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "unchanged", "duplicate"}


class ActivityConfigEventService:
    """Version activity configuration and replay administrative mutations."""

    _STATE_KEY = "runtime"

    def __init__(self, database: str | Path, lock: RLock | None = None) -> None:
        self._database = Path(database)
        self._lock = lock or RLock()

    @staticmethod
    def _ensure_schema(conn) -> None:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS activity_config_state("
            "state_key TEXT PRIMARY KEY,revision INTEGER NOT NULL,"
            "config_json TEXT NOT NULL,updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
        )
        conn.execute(
            "CREATE TABLE IF NOT EXISTS activity_config_operations("
            "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,outcome TEXT NOT NULL,"
            "revision INTEGER NOT NULL,result_json TEXT NOT NULL,result_text TEXT NOT NULL,"
            "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
        )

    @staticmethod
    def _canonical_config(config: Mapping[str, Any]) -> tuple[dict[str, Any], str]:
        if not isinstance(config, Mapping):
            raise ValueError("valid activity config is required")
        payload = json.dumps(
            dict(config),
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
        )
        normalized = json.loads(payload)
        if not isinstance(normalized, dict):
            raise ValueError("valid activity config is required")
        return normalized, payload

    @staticmethod
    def _request_payload(request_identity) -> str:
        return json.dumps(
            request_identity,
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
        )

    @staticmethod
    def _decode_config(raw) -> dict[str, Any]:
        config = json.loads(str(raw))
        if not isinstance(config, dict):
            raise ValueError("stored activity config is invalid")
        return config

    @classmethod
    def _state(cls, row) -> ActivityConfigState:
        return ActivityConfigState(int(row[0]), cls._decode_config(row[1]))

    @classmethod
    def _operation_result(
        cls, row, status: str = "duplicate"
    ) -> ActivityConfigMutationResult:
        return ActivityConfigMutationResult(
            status,
            int(row[2]),
            cls._decode_config(row[3]),
            str(row[4]),
        )

    @staticmethod
    def _operation(conn, operation_id: str):
        return conn.execute(
            "SELECT payload,outcome,revision,result_json,result_text "
            "FROM activity_config_operations WHERE operation_id=%s",
            (operation_id,),
        ).fetchone()

    def load_or_import(self, legacy_config: Mapping[str, Any]) -> ActivityConfigState:
        normalized, payload = self._canonical_config(legacy_config)
        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_schema(conn)
                row = conn.execute(
                    "SELECT revision,config_json FROM activity_config_state "
                    "WHERE state_key=%s",
                    (self._STATE_KEY,),
                ).fetchone()
                if row is None:
                    conn.execute(
                        "INSERT INTO activity_config_state("
                        "state_key,revision,config_json) VALUES(%s,1,%s)",
                        (self._STATE_KEY, payload),
                    )
                    result = ActivityConfigState(1, normalized)
                else:
                    result = self._state(row)
                conn.commit()
                return result
            except Exception:
                conn.rollback()
                raise

    def replay(
        self, operation_id, request_identity
    ) -> ActivityConfigMutationResult | None:
        operation_id = str(operation_id).strip()
        if not operation_id:
            raise ValueError("operation is required")
        payload = self._request_payload(request_identity)
        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_schema(conn)
                previous = self._operation(conn, operation_id)
                if previous is None:
                    conn.commit()
                    return None
                if str(previous[0]) != payload:
                    conn.commit()
                    return ActivityConfigMutationResult("operation_conflict")
                result = self._operation_result(previous)
                conn.commit()
                return result
            except Exception:
                conn.rollback()
                raise

    def replace(
        self,
        operation_id,
        request_identity,
        expected_revision,
        config: Mapping[str, Any],
        *,
        result_text: str = "",
    ) -> ActivityConfigMutationResult:
        operation_id = str(operation_id).strip()
        expected_revision = int(expected_revision)
        result_text = str(result_text)
        if not operation_id or expected_revision <= 0:
            raise ValueError("operation and positive expected revision are required")
        request_payload = self._request_payload(request_identity)
        normalized, config_payload = self._canonical_config(config)

        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_schema(conn)
                previous = self._operation(conn, operation_id)
                if previous is not None:
                    if str(previous[0]) != request_payload:
                        conn.commit()
                        return ActivityConfigMutationResult("operation_conflict")
                    result = self._operation_result(previous)
                    conn.commit()
                    return result

                current = conn.execute(
                    "SELECT revision,config_json FROM activity_config_state "
                    "WHERE state_key=%s",
                    (self._STATE_KEY,),
                ).fetchone()
                if current is None:
                    conn.commit()
                    return ActivityConfigMutationResult("state_missing")
                current_revision = int(current[0])
                if current_revision != expected_revision:
                    conn.commit()
                    return ActivityConfigMutationResult(
                        "state_changed",
                        current_revision,
                        self._decode_config(current[1]),
                    )

                unchanged = str(current[1]) == config_payload
                next_revision = current_revision if unchanged else current_revision + 1
                outcome = "unchanged" if unchanged else "applied"
                if not unchanged:
                    conn.execute(
                        "UPDATE activity_config_state SET revision=%s,config_json=%s,"
                        "updated_at=CURRENT_TIMESTAMP WHERE state_key=%s AND revision=%s",
                        (
                            next_revision,
                            config_payload,
                            self._STATE_KEY,
                            current_revision,
                        ),
                    )
                conn.execute(
                    "INSERT INTO activity_config_operations("
                    "operation_id,payload,outcome,revision,result_json,result_text) "
                    "VALUES(%s,%s,%s,%s,%s,%s)",
                    (
                        operation_id,
                        request_payload,
                        outcome,
                        next_revision,
                        config_payload,
                        result_text,
                    ),
                )
                conn.commit()
                return ActivityConfigMutationResult(
                    outcome,
                    next_revision,
                    normalized,
                    result_text,
                )
            except Exception:
                conn.rollback()
                raise


__all__ = [
    "ActivityConfigEventService",
    "ActivityConfigMutationResult",
    "ActivityConfigState",
]
