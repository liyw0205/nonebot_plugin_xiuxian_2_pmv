from __future__ import annotations

from ...core.numeric import as_int_like
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

from ...infrastructure.database import DatabaseUnitOfWork


@dataclass(frozen=True)
class WorkAbortCleanupResult:
    status: str
    penalty: int = 0
    stone_remaining: int = 0
    reason: str = ""
    subject: str = ""

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


class _WorkAbortCleanupWriteRejected(Exception):
    def __init__(self, result: WorkAbortCleanupResult) -> None:
        self.result = result


class WorkAbortCleanupSqlRepository:
    """Clear work projections and apply any abort penalty in one game-db UoW."""

    def __init__(self, database: str | Path) -> None:
        self.database = Path(database)

    @staticmethod
    def _schema_ready(uow: DatabaseUnitOfWork) -> bool:
        tables = {
            str(row["name"])
            for row in uow.query_all(
                "SELECT name FROM sqlite_master WHERE type='table' "
                "AND name IN ('work_offer_snapshots','work_active_snapshots',"
                "'work_abort_cleanup_operations')"
            )
        }
        return tables == {
            "work_offer_snapshots",
            "work_active_snapshots",
            "work_abort_cleanup_operations",
        }

    @staticmethod
    def _payload(
        user_id: str,
        reason: str,
        expected_cd: Mapping[str, Any],
        expected_offer: Mapping[str, Any] | None,
        expected_stone: int | None,
        penalty: int,
    ) -> str:
        return json.dumps(
            [user_id, reason, expected_cd, expected_offer, expected_stone, penalty],
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
            default=str,
        )

    @staticmethod
    def _payload_matches(stored: Any, expected: str) -> bool:
        try:
            return json.loads(str(stored)) == json.loads(expected)
        except (TypeError, ValueError):
            return False

    @staticmethod
    def _write_rejected(stage: str, subject: str) -> WorkAbortCleanupResult:
        return WorkAbortCleanupResult(
            "state_changed",
            reason=f"work_abort_cleanup_{stage}_write_rejected",
            subject=str(subject),
        )

    @staticmethod
    def _raise_write_rejected(stage: str, subject: str) -> None:
        raise _WorkAbortCleanupWriteRejected(
            WorkAbortCleanupSqlRepository._write_rejected(stage, subject)
        )

    def cleanup(
        self,
        operation_id: str,
        user_id: str,
        reason: str,
        expected_cd: Mapping[str, Any],
        expected_offer: Mapping[str, Any] | None = None,
        expected_stone: int | None = None,
        penalty: int = 0,
    ) -> WorkAbortCleanupResult:
        operation_id, user_id, reason = str(operation_id).strip(), str(user_id), str(reason).strip()
        expected_cd = dict(expected_cd or {})
        expected_offer = dict(expected_offer) if expected_offer else None
        penalty = int(penalty)
        expected_stone = None if expected_stone is None else int(expected_stone)
        if not operation_id or reason not in {"active_abort", "offer_abort", "expired", "reset"} or penalty < 0:
            raise ValueError("invalid work cleanup request")
        if reason == "active_abort" and expected_stone is None:
            raise ValueError("active abort requires a stone snapshot")
        if not self.database.is_file():
            return WorkAbortCleanupResult("schema_missing")

        payload = self._payload(
            user_id, reason, expected_cd, expected_offer, expected_stone, penalty
        )
        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            if not self._schema_ready(uow):
                return WorkAbortCleanupResult("schema_missing")

            previous = uow.query_one(
                "SELECT payload,penalty,stone_remaining FROM work_abort_cleanup_operations "
                "WHERE operation_id=?",
                (operation_id,),
            )
            if previous is not None:
                if not self._payload_matches(previous["payload"], payload):
                    return WorkAbortCleanupResult("operation_conflict")
                return WorkAbortCleanupResult(
                    "duplicate", int(previous["penalty"]), int(previous["stone_remaining"])
                )

            cooldown = uow.query_one(
                "SELECT COALESCE(type,0) AS type,create_time,scheduled_time "
                "FROM user_cd WHERE user_id=?",
                (user_id,),
            )
            user = uow.query_one(
                "SELECT COALESCE(stone,0) AS stone FROM user_xiuxian WHERE user_id=?",
                (user_id,),
            )
            if cooldown is None or user is None:
                return WorkAbortCleanupResult("user_missing")

            actual_cd = {
                "type": int(cooldown["type"]),
                "create_time": cooldown["create_time"],
                "scheduled_time": cooldown["scheduled_time"],
            }
            normalized_cd = {
                "type": int(expected_cd.get("type", 0)),
                "create_time": expected_cd.get("create_time"),
                "scheduled_time": expected_cd.get("scheduled_time"),
            }
            if actual_cd != normalized_cd:
                return WorkAbortCleanupResult("state_changed")
            if reason == "active_abort" and actual_cd["type"] != 2:
                return WorkAbortCleanupResult("state_changed")

            stored = uow.query_one(
                "SELECT snapshot FROM work_offer_snapshots WHERE user_id=?", (user_id,)
            )
            stored_offer = json.loads(str(stored["snapshot"])) if stored else None
            if stored_offer is not None and stored_offer != expected_offer:
                return WorkAbortCleanupResult("state_changed")

            stone = as_int_like(user["stone"])
            if expected_stone is not None and stone != expected_stone:
                return WorkAbortCleanupResult("state_changed")
            applied_penalty = min(penalty, stone) if reason == "active_abort" else 0
            remaining = stone - applied_penalty
            active_before = uow.query_one(
                "SELECT user_id FROM work_active_snapshots WHERE user_id=?", (user_id,)
            )
            try:
                with uow.savepoint("work_abort_cleanup"):
                    if applied_penalty:
                        changed = uow.execute(
                            "UPDATE user_xiuxian SET stone=xiuxian_int_store(?,'stone','actor') WHERE user_id=? "
                            "AND xiuxian_int_cmp(COALESCE(stone,0),?,1,'stone','actor')=0",
                            (remaining, user_id, stone),
                        )
                        saved_user = uow.query_one(
                            "SELECT COALESCE(stone,0) AS stone FROM user_xiuxian WHERE user_id=?",
                            (user_id,),
                        )
                        if (
                            changed.rowcount != 1
                            or saved_user is None
                            or as_int_like(saved_user["stone"]) != remaining
                        ):
                            self._raise_write_rejected("stone", user_id)

                    changed_cd = uow.execute(
                        "UPDATE user_cd SET type=0,create_time=0,scheduled_time=NULL WHERE user_id=?",
                        (user_id,),
                    )
                    saved_cd = uow.query_one(
                        "SELECT COALESCE(type,0) AS type,create_time,scheduled_time "
                        "FROM user_cd WHERE user_id=?",
                        (user_id,),
                    )
                    if (
                        changed_cd.rowcount != 1
                        or saved_cd is None
                        or int(saved_cd["type"] or 0) != 0
                        or str(saved_cd["create_time"]) != "0"
                        or saved_cd["scheduled_time"] is not None
                    ):
                        self._raise_write_rejected("cooldown", user_id)

                    if stored is not None:
                        changed_offer = uow.execute(
                            "DELETE FROM work_offer_snapshots WHERE user_id=?", (user_id,)
                        )
                        if (
                            changed_offer.rowcount != 1
                            or uow.query_one(
                                "SELECT user_id FROM work_offer_snapshots WHERE user_id=?",
                                (user_id,),
                            )
                            is not None
                        ):
                            self._raise_write_rejected("offer_snapshot", user_id)

                    if active_before is not None:
                        changed_active = uow.execute(
                            "DELETE FROM work_active_snapshots WHERE user_id=?", (user_id,)
                        )
                        if (
                            changed_active.rowcount != 1
                            or uow.query_one(
                                "SELECT user_id FROM work_active_snapshots WHERE user_id=?",
                                (user_id,),
                            )
                            is not None
                        ):
                            self._raise_write_rejected("active_snapshot", user_id)

                    written = uow.execute(
                        "INSERT INTO work_abort_cleanup_operations "
                        "(operation_id,payload,reason,penalty,stone_remaining) VALUES(?,?,?,?,?)",
                        (operation_id, payload, reason, applied_penalty, remaining),
                    )
                    receipt = uow.query_one(
                        "SELECT payload,reason,penalty,stone_remaining "
                        "FROM work_abort_cleanup_operations WHERE operation_id=?",
                        (operation_id,),
                    )
                    if (
                        written.rowcount != 1
                        or receipt is None
                        or not self._payload_matches(receipt["payload"], payload)
                        or str(receipt["reason"]) != reason
                        or as_int_like(receipt["penalty"]) != applied_penalty
                        or as_int_like(receipt["stone_remaining"]) != remaining
                    ):
                        self._raise_write_rejected("receipt", operation_id)
            except _WorkAbortCleanupWriteRejected as exc:
                return exc.result
            return WorkAbortCleanupResult("applied", applied_penalty, remaining)


__all__ = ["WorkAbortCleanupResult", "WorkAbortCleanupSqlRepository"]
