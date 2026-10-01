from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from ...core.errors import OperationConflictError
from ...core.result import OperationOutcome
from ...infrastructure.database import DatabaseUnitOfWork, OperationLedger


@dataclass(frozen=True)
class PuppetEnabledUser:
    row_id: int
    user_id: str


@dataclass(frozen=True)
class PuppetStatusResult:
    status: str
    operation_id: str
    user_id: str
    enabled: int
    affected_rows: int = 0

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


class PuppetStatusSqlRepository:
    ACTION = "puppet.status"
    DEFAULT_PAGE_SIZE = 200
    MAX_PAGE_SIZE = 1000

    def __init__(self, database: str | Path, *, ledger: OperationLedger | None = None) -> None:
        self.database = Path(database)
        self.ledger = ledger or OperationLedger()

    @staticmethod
    def _has_columns(uow: DatabaseUnitOfWork, table: str, required: set[str]) -> bool:
        columns = {
            str(row["name"]).casefold()
            for row in uow.query_all(f'PRAGMA main.table_info("{table}")')
        }
        return required.issubset(columns)

    @classmethod
    def _player_schema_ready(cls, uow: DatabaseUnitOfWork) -> bool:
        return cls._has_columns(uow, "user_xiuxian", {"user_id", "puppet_status"})

    @classmethod
    def _write_schema_ready(cls, uow: DatabaseUnitOfWork) -> bool:
        return (
            cls._player_schema_ready(uow)
            and cls._has_columns(
                uow,
                "operation_ledger",
                {
                    "operation_id", "action", "request_hash", "status", "result_json",
                    "created_at", "updated_at",
                },
            )
            and cls._has_columns(
                uow,
                "operation_audit",
                {
                    "operation_id", "action", "status", "before_json", "after_json",
                    "consumed_json", "granted_json", "category", "occurred_at", "created_at",
                },
            )
        )

    @staticmethod
    def _from_outcome(
        outcome: OperationOutcome[dict] | None,
        operation_id: str,
        user_id: str,
        status: str,
    ) -> PuppetStatusResult:
        data = dict(outcome.data or {}) if outcome is not None else {}
        return PuppetStatusResult(
            status=status,
            operation_id=operation_id,
            user_id=user_id,
            enabled=int(data.get("enabled", 0) or 0),
            affected_rows=int(data.get("affected_rows", 0) or 0),
        )

    def set_enabled(
        self,
        operation_id: str,
        user_id: str,
        enabled: int | bool,
    ) -> PuppetStatusResult:
        operation_id, user_id = str(operation_id).strip(), str(user_id).strip()
        enabled = 1 if bool(enabled) else 0
        if not operation_id or not user_id:
            return PuppetStatusResult("invalid", operation_id, user_id, enabled)
        if not self.database.is_file():
            return PuppetStatusResult("schema_missing", operation_id, user_id, enabled)

        payload = {"user_id": user_id, "enabled": enabled}
        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            if not self._write_schema_ready(uow):
                return PuppetStatusResult("schema_missing", operation_id, user_id, enabled)
            try:
                existing = self.ledger.begin(uow, operation_id, self.ACTION, payload)
            except OperationConflictError:
                return PuppetStatusResult("operation_conflict", operation_id, user_id, enabled)
            if existing is not None:
                previous = existing.outcome()
                if previous is None:
                    return PuppetStatusResult("in_progress", operation_id, user_id, enabled)
                return self._from_outcome(previous, operation_id, user_id, "duplicate")

            state = uow.query_one(
                "SELECT COUNT(*) AS target_rows,"
                "COALESCE(SUM(CASE WHEN puppet_status IS NOT ? THEN 1 ELSE 0 END),0) AS changed_rows,"
                "MIN(COALESCE(puppet_status,0)) AS previous_min,"
                "MAX(COALESCE(puppet_status,0)) AS previous_max "
                "FROM user_xiuxian WHERE user_id=?",
                (enabled, user_id),
            )
            target_rows = int(state["target_rows"] or 0)
            if target_rows == 0:
                outcome = OperationOutcome.rejected(
                    operation_id,
                    self.ACTION,
                    "傀儡玩家状态不存在",
                    code="user_missing",
                    data={"status": "user_missing", "enabled": enabled, "affected_rows": 0},
                    audit_category="puppet",
                )
                self.ledger.finish(uow, outcome)
                return PuppetStatusResult("user_missing", operation_id, user_id, enabled)

            changed_rows = int(state["changed_rows"] or 0)
            updated = uow.execute(
                "UPDATE user_xiuxian SET puppet_status=? "
                "WHERE user_id=? AND puppet_status IS NOT ?",
                (enabled, user_id, enabled),
            )
            affected_rows = int(updated.rowcount)
            if affected_rows != changed_rows:
                raise RuntimeError("puppet status target changed")
            outcome = OperationOutcome.applied(
                operation_id,
                self.ACTION,
                data={"status": "applied", "enabled": enabled, "affected_rows": affected_rows},
                before={
                    "target_rows": target_rows,
                    "status_min": int(state["previous_min"] or 0),
                    "status_max": int(state["previous_max"] or 0),
                },
                after={"enabled": enabled, "affected_rows": affected_rows},
                audit_category="puppet",
            )
            self.ledger.finish(uow, outcome)
            return PuppetStatusResult("applied", operation_id, user_id, enabled, affected_rows)

    def get_status(self, user_id: str) -> int:
        user_id = str(user_id).strip()
        if not self.database.is_file():
            raise RuntimeError("puppet.002 schema_missing")
        with DatabaseUnitOfWork(self.database, read_only=True) as uow:
            if not self._player_schema_ready(uow):
                raise RuntimeError("puppet.002 schema_missing")
            row = uow.query_one(
                "SELECT puppet_status FROM user_xiuxian WHERE user_id=? ORDER BY rowid LIMIT 1",
                (user_id,),
            )
        return int(row["puppet_status"] or 0) if row is not None else 0

    def enabled_user_high_watermark(self) -> int:
        if not self.database.is_file():
            raise RuntimeError("puppet.002 schema_missing")
        with DatabaseUnitOfWork(self.database, read_only=True) as uow:
            if not self._player_schema_ready(uow):
                raise RuntimeError("puppet.002 schema_missing")
            row = uow.query_one("SELECT COALESCE(MAX(rowid),0) AS max_rowid FROM user_xiuxian")
        return int(row["max_rowid"] or 0)

    def list_enabled_users(
        self,
        after_row_id: int,
        through_row_id: int,
        *,
        limit: int = DEFAULT_PAGE_SIZE,
    ) -> list[PuppetEnabledUser]:
        limit = max(1, min(int(limit), self.MAX_PAGE_SIZE))
        after_row_id, through_row_id = int(after_row_id), int(through_row_id)
        if not self.database.is_file():
            raise RuntimeError("puppet.002 schema_missing")
        with DatabaseUnitOfWork(self.database, read_only=True) as uow:
            if not self._player_schema_ready(uow):
                raise RuntimeError("puppet.002 schema_missing")
            rows = uow.query_all(
                "SELECT rowid AS row_id,user_id FROM user_xiuxian "
                "WHERE puppet_status=1 AND rowid>? AND rowid<=? ORDER BY rowid LIMIT ?",
                (after_row_id, through_row_id, limit),
            )
        return [PuppetEnabledUser(int(row["row_id"]), str(row["user_id"])) for row in rows]


__all__ = ["PuppetEnabledUser", "PuppetStatusResult", "PuppetStatusSqlRepository"]
