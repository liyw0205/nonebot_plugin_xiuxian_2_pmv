from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ...infrastructure.database import DatabaseUnitOfWork


@dataclass(frozen=True)
class DongfuInfiltrationPlanResult:
    status: str
    plan: dict[str, Any] | None = None

    @property
    def succeeded(self) -> bool:
        return self.status in {"planned", "existing"}


class DongfuInfiltrationPlanSqlRepository:
    MAX_PAYLOAD_BYTES = 256 * 1024

    def __init__(self, database: str | Path) -> None:
        self.database = Path(database)

    @staticmethod
    def _canonical(value: Any) -> str:
        return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))

    @staticmethod
    def _schema_ready(uow: DatabaseUnitOfWork) -> bool:
        rows = uow.query_all('PRAGMA main.table_info("dongfu_infiltration_operations")')
        columns = {str(row["name"]) for row in rows}
        return (
            {"operation_id", "user_id", "payload"}.issubset(columns)
            and any(
                str(row["name"]) == "operation_id" and int(row["pk"] or 0) == 1
                for row in rows
            )
        )

    def _decode(self, raw: object, request_identity: dict[str, Any], user_id: str):
        try:
            stored = json.loads(str(raw))
        except (TypeError, ValueError):
            return DongfuInfiltrationPlanResult("plan_invalid")
        if not isinstance(stored, dict) or not isinstance(stored.get("request"), dict):
            return DongfuInfiltrationPlanResult("plan_invalid")
        plan = stored.get("plan")
        if not isinstance(plan, dict):
            return DongfuInfiltrationPlanResult("plan_invalid")
        if str(stored.get("user_id", "")) != user_id:
            return DongfuInfiltrationPlanResult("operation_conflict")
        if self._canonical(stored["request"]) != self._canonical(request_identity):
            return DongfuInfiltrationPlanResult("operation_conflict")
        return DongfuInfiltrationPlanResult("existing", plan)

    def get(
        self,
        operation_id: str,
        user_id: str,
        request_identity: dict[str, Any],
    ) -> DongfuInfiltrationPlanResult:
        operation_id, user_id = str(operation_id).strip(), str(user_id)
        if not operation_id or not self.database.is_file():
            return DongfuInfiltrationPlanResult("missing")
        with DatabaseUnitOfWork(self.database, read_only=True) as uow:
            if not self._schema_ready(uow):
                return DongfuInfiltrationPlanResult("schema_missing")
            row = uow.query_one(
                "SELECT user_id,payload FROM dongfu_infiltration_operations WHERE operation_id=?",
                (operation_id,),
            )
        if row is None:
            return DongfuInfiltrationPlanResult("missing")
        try:
            stored = json.loads(str(row["payload"]))
        except (TypeError, ValueError):
            return DongfuInfiltrationPlanResult("plan_invalid")
        if not isinstance(stored, dict):
            return DongfuInfiltrationPlanResult("plan_invalid")
        stored.setdefault("user_id", str(row["user_id"]))
        return self._decode(self._canonical(stored), request_identity, user_id)

    def prepare(
        self,
        operation_id: str,
        user_id: str,
        request_identity: dict[str, Any],
        plan: dict[str, Any],
    ) -> DongfuInfiltrationPlanResult:
        operation_id, user_id = str(operation_id).strip(), str(user_id)
        if not operation_id or not user_id or not isinstance(request_identity, dict) or not isinstance(plan, dict):
            return DongfuInfiltrationPlanResult("plan_invalid")
        payload = self._canonical(
            {"user_id": user_id, "request": request_identity, "plan": plan}
        )
        if len(payload.encode("utf-8")) > self.MAX_PAYLOAD_BYTES:
            return DongfuInfiltrationPlanResult("plan_invalid")
        if not self.database.is_file():
            return DongfuInfiltrationPlanResult("schema_missing")
        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            if not self._schema_ready(uow):
                return DongfuInfiltrationPlanResult("schema_missing")
            row = uow.query_one(
                "SELECT user_id,payload FROM dongfu_infiltration_operations WHERE operation_id=?",
                (operation_id,),
            )
            if row is not None:
                result = self._decode(row["payload"], request_identity, user_id)
                if str(row["user_id"]) != user_id:
                    return DongfuInfiltrationPlanResult("operation_conflict")
                return result
            uow.execute(
                "INSERT INTO dongfu_infiltration_operations(operation_id,user_id,payload) VALUES(?,?,?)",
                (operation_id, user_id, payload),
            )
        return DongfuInfiltrationPlanResult("planned", plan)


__all__ = ["DongfuInfiltrationPlanResult", "DongfuInfiltrationPlanSqlRepository"]
