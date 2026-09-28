from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

from ...infrastructure.database import DatabaseUnitOfWork


@dataclass(frozen=True)
class WorkRefreshResult:
    status: str
    remaining_count: int = 0
    offer: dict[str, Any] | None = None

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


class WorkRefreshSqlRepository:
    """Atomically consume a refresh and persist its fixed offer snapshot."""

    def __init__(self, database: str | Path) -> None:
        self.database = Path(database)

    @staticmethod
    def _schema_ready(uow: DatabaseUnitOfWork) -> bool:
        tables = {
            str(row["name"])
            for row in uow.query_all(
                "SELECT name FROM sqlite_master WHERE type='table' "
                "AND name IN ('work_refresh_operations','work_offer_snapshots')"
            )
        }
        return tables == {"work_refresh_operations", "work_offer_snapshots"}

    @staticmethod
    def _payload(user_id: str, force: bool) -> str:
        return json.dumps([user_id, force], ensure_ascii=True, separators=(",", ":"))

    @staticmethod
    def _payload_matches(stored: Any, expected: str) -> bool:
        try:
            return json.loads(str(stored)) == json.loads(expected)
        except (TypeError, ValueError):
            return False

    @staticmethod
    def _clean(value: Any) -> Any:
        if value is None:
            return None
        if isinstance(value, str) and value.strip().lower() in {"", "none", "null"}:
            return None
        return value

    @classmethod
    def _cooldown(cls, row: Mapping[str, Any], *, actual: bool) -> dict[str, Any]:
        if actual:
            return {
                "type": int(row.get("type") or 0),
                "create_time": cls._clean(row.get("create_time")),
                "scheduled_time": cls._clean(row.get("scheduled_time")),
            }
        return {
            "type": int(row.get("type", 0) or 0),
            "create_time": cls._clean(row.get("create_time")),
            "scheduled_time": cls._clean(row.get("scheduled_time")),
        }

    def get_result(self, operation_id: str) -> WorkRefreshResult | None:
        operation_id = str(operation_id).strip()
        if not operation_id:
            return None
        if not self.database.is_file():
            return WorkRefreshResult("schema_missing")
        with DatabaseUnitOfWork(self.database, read_only=True) as uow:
            if not self._schema_ready(uow):
                return WorkRefreshResult("schema_missing")
            previous = uow.query_one(
                "SELECT remaining_count,offer_snapshot FROM work_refresh_operations "
                "WHERE operation_id=?",
                (operation_id,),
            )
            if previous is None:
                return None
            return WorkRefreshResult(
                "duplicate",
                int(previous["remaining_count"]),
                json.loads(str(previous["offer_snapshot"])),
            )

    def refresh(
        self,
        operation_id: str,
        user_id: str,
        expected_count: int,
        expected_cd: Mapping[str, Any],
        expected_offer: Mapping[str, Any] | None,
        new_offer: Mapping[str, Any],
        force: bool = False,
    ) -> WorkRefreshResult:
        operation_id, user_id = str(operation_id).strip(), str(user_id)
        expected_count, force = int(expected_count), bool(force)
        expected_cd = dict(expected_cd or {})
        expected_offer = dict(expected_offer) if expected_offer else None
        new_offer = dict(new_offer)
        if not self.database.is_file():
            return WorkRefreshResult("schema_missing")

        payload = self._payload(user_id, force)
        offer_json = json.dumps(
            new_offer, ensure_ascii=True, sort_keys=True, separators=(",", ":"), default=str
        )
        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            if not self._schema_ready(uow):
                return WorkRefreshResult("schema_missing")

            previous = uow.query_one(
                "SELECT payload,remaining_count,offer_snapshot FROM work_refresh_operations "
                "WHERE operation_id=?",
                (operation_id,),
            )
            if previous is not None:
                if not self._payload_matches(previous["payload"], payload):
                    return WorkRefreshResult("operation_conflict")
                return WorkRefreshResult(
                    "duplicate",
                    int(previous["remaining_count"]),
                    json.loads(str(previous["offer_snapshot"])),
                )

            user = uow.query_one(
                "SELECT COALESCE(work_num,0) AS work_num FROM user_xiuxian WHERE user_id=?",
                (user_id,),
            )
            cooldown = uow.query_one(
                "SELECT COALESCE(type,0) AS type,create_time,scheduled_time "
                "FROM user_cd WHERE user_id=?",
                (user_id,),
            )
            if user is None or cooldown is None:
                return WorkRefreshResult("user_missing")

            actual_cd = self._cooldown(cooldown, actual=True)
            normalized_cd = self._cooldown(expected_cd, actual=False)
            try:
                actual_count = int(float(user["work_num"] or 0))
            except (TypeError, ValueError, OverflowError):
                return WorkRefreshResult("state_changed")
            if actual_count != expected_count or actual_cd != normalized_cd or actual_cd["type"] != 0:
                return WorkRefreshResult("state_changed")

            stored = uow.query_one(
                "SELECT snapshot FROM work_offer_snapshots WHERE user_id=?", (user_id,)
            )
            stored_offer = json.loads(str(stored["snapshot"])) if stored else None
            if stored_offer is not None and stored_offer != expected_offer:
                return WorkRefreshResult("state_changed")
            if not force and expected_offer and int(expected_offer.get("status", 1)) == 1:
                return WorkRefreshResult("offer_exists")

            remaining = expected_count - 1
            changed = uow.execute(
                "UPDATE user_xiuxian SET work_num=? WHERE user_id=? "
                "AND CAST(COALESCE(work_num,0) AS INTEGER)=?",
                (remaining, user_id, expected_count),
            )
            if changed.rowcount < 1:
                return WorkRefreshResult("state_changed")

            uow.execute(
                "INSERT INTO work_offer_snapshots(user_id,snapshot,updated_at) VALUES(?,?,?) "
                "ON CONFLICT(user_id) DO UPDATE SET snapshot=excluded.snapshot,"
                "updated_at=excluded.updated_at",
                (user_id, offer_json, str(new_offer.get("refresh_time", ""))),
            )
            uow.execute(
                "INSERT INTO work_refresh_operations "
                "(operation_id,payload,remaining_count,offer_snapshot) VALUES(?,?,?,?)",
                (operation_id, payload, remaining, offer_json),
            )
            return WorkRefreshResult("applied", remaining, new_offer)


__all__ = ["WorkRefreshResult", "WorkRefreshSqlRepository"]
