from __future__ import annotations

from ...core.numeric import as_int_like, crystal_precision_reason
from ...core.errors import DomainError
from ..impart.legacy_state import crystal_balance
import json
from dataclasses import dataclass
from pathlib import Path

from ...infrastructure.database import DatabaseUnitOfWork


@dataclass(frozen=True)
class AdminImpartStoneSnapshot:
    status: str
    stone: int | None = None
    reason: str = ""
    subject: str = ""


@dataclass(frozen=True)
class AdminImpartStoneResult:
    status: str
    previous_stone: int = 0
    final_stone: int = 0
    applied_delta: int = 0
    reason: str = ""
    subject: str = ""

    @property
    def succeeded(self) -> bool:
        return self.status in {"adjusted", "duplicate"}


class AdminImpartStoneSqlRepository:
    def __init__(self, game_database: str | Path, impart_database: str | Path) -> None:
        self.game_database = Path(game_database)
        self.impart_database = Path(impart_database)

    @staticmethod
    def _columns(uow: DatabaseUnitOfWork, table: str, schema: str = "main") -> set[str]:
        rows = uow.query_all(f'PRAGMA "{schema}".table_info("{table}")')
        return {str(row["name"]).casefold() for row in rows}

    @classmethod
    def _schema_ready(cls, uow: DatabaseUnitOfWork) -> bool:
        return (
            {"user_id"}.issubset(cls._columns(uow, "user_xiuxian"))
            and {
                "operation_id", "payload", "previous_stone", "final_stone", "applied_delta",
                "previous_stone_exact", "final_stone_exact",
                "previous_stone_precision_state", "final_stone_precision_state",
                "applied_delta_exact", "applied_delta_precision_state",
            }.issubset(cls._columns(uow, "admin_impart_stone_operations"))
            and {
                "user_id", "source", "action", "item_delta", "detail", "trace_id", "created_at"
            }.issubset(cls._columns(uow, "economy_log"))
            and {"user_id", "stone_num", "stone_num_exact", "stone_num_precision_state"}.issubset(
                cls._columns(uow, "xiuxian_impart", "impart_data")
            )
        )

    def snapshot(self, user_id: str) -> AdminImpartStoneSnapshot:
        user_id = str(user_id).strip()
        if not user_id:
            raise ValueError("user id is required")
        if not self.impart_database.is_file():
            return AdminImpartStoneSnapshot("schema_missing")

        with DatabaseUnitOfWork(self.impart_database, read_only=True) as uow:
            columns = self._columns(uow, "xiuxian_impart")
            if not {"user_id", "stone_num"}.issubset(columns):
                return AdminImpartStoneSnapshot("schema_missing")
            if {"stone_num_exact", "stone_num_precision_state"}.issubset(columns):
                rows = uow.query_all(
                    "SELECT user_id,stone_num,stone_num_exact,stone_num_precision_state "
                    "FROM xiuxian_impart WHERE user_id=?",
                    (user_id,),
                )
            else:
                rows = uow.query_all(
                    "SELECT stone_num,typeof(stone_num) AS stone_num_storage_type "
                    "FROM xiuxian_impart WHERE user_id=?",
                    (user_id,),
                )
            if len(rows) > 1:
                return AdminImpartStoneSnapshot(
                    "invalid_state", reason="admin_impart_rows_duplicate", subject="target"
                )
            try:
                stone = crystal_balance(rows[0], subject=user_id) if rows else None
            except DomainError as exc:
                if exc.code != "impart_crystal_precision_unavailable":
                    raise
                return AdminImpartStoneSnapshot(
                    "precision_unavailable",
                    reason=str(exc.details.get("reason") or "crystal_invalid_numeric_value"),
                    subject="target",
                )
            return AdminImpartStoneSnapshot("ok", stone)

    def adjust(
        self,
        operation_id: str,
        operator_id: str,
        user_id: str,
        expected_stone: int | None,
        requested_delta: int,
        *,
        target_name: str = "",
    ) -> AdminImpartStoneResult:
        operation_id = str(operation_id).strip()
        operator_id = str(operator_id).strip()
        user_id = str(user_id).strip()
        expected_stone = None if expected_stone is None else int(expected_stone)
        requested_delta = int(requested_delta)
        if not operation_id or not operator_id or not user_id:
            raise ValueError("operation, operator and user are required")
        if expected_stone is not None and expected_stone < 0:
            raise ValueError("stone snapshot cannot be negative")
        if requested_delta == 0:
            raise ValueError("adjustment cannot be zero")

        if not self.game_database.is_file() or not self.impart_database.is_file():
            return AdminImpartStoneResult("schema_missing")

        payload = json.dumps(
            [operator_id, user_id, requested_delta],
            ensure_ascii=True,
            separators=(",", ":"),
        )
        with DatabaseUnitOfWork(self.game_database, immediate=True) as uow:
            uow.attach_database(self.impart_database, "impart_data")
            if not self._schema_ready(uow):
                return AdminImpartStoneResult("schema_missing")

            previous = uow.query_one(
                "SELECT payload,previous_stone,final_stone,previous_stone_exact,"
                "final_stone_exact,previous_stone_precision_state,"
                "final_stone_precision_state,applied_delta,applied_delta_exact,"
                "applied_delta_precision_state "
                "FROM admin_impart_stone_operations WHERE operation_id=?",
                (operation_id,),
            )
            if previous is not None:
                if str(previous["payload"]) != payload:
                    return AdminImpartStoneResult("operation_conflict")
                if (
                    previous["previous_stone_precision_state"] != "exact"
                    or previous["final_stone_precision_state"] != "exact"
                    or previous["applied_delta_precision_state"] != "exact"
                ):
                    return AdminImpartStoneResult(
                        "precision_unavailable",
                        reason=crystal_precision_reason(
                            next(
                                state for state in (
                                    previous["final_stone_precision_state"],
                                    previous["previous_stone_precision_state"],
                                    previous["applied_delta_precision_state"],
                                ) if state != "exact"
                            )
                        ),
                        subject="target",
                    )
                try:
                    return AdminImpartStoneResult(
                        "duplicate",
                        crystal_balance(previous, subject="target", field="previous_stone"),
                        crystal_balance(previous, subject="target", field="final_stone"),
                        crystal_balance(previous, subject="target", field="applied_delta"),
                    )
                except DomainError as exc:
                    if exc.code != "impart_crystal_precision_unavailable":
                        raise
                    return AdminImpartStoneResult(
                        "precision_unavailable", reason=exc.details["reason"], subject="target"
                    )

            if uow.query_one(
                "SELECT 1 FROM user_xiuxian WHERE user_id=?", (user_id,)
            ) is None:
                return AdminImpartStoneResult("user_missing")
            rows = uow.query_all(
                "SELECT stone_num,stone_num_exact,stone_num_precision_state "
                "FROM impart_data.xiuxian_impart WHERE user_id=?",
                (user_id,),
            )
            if len(rows) > 1:
                return AdminImpartStoneResult(
                    "invalid_state", reason="admin_impart_rows_duplicate", subject="target"
                )
            try:
                actual_stone = crystal_balance(rows[0], subject=user_id) if rows else None
            except DomainError as exc:
                if exc.code != "impart_crystal_precision_unavailable":
                    raise
                return AdminImpartStoneResult(
                    "precision_unavailable",
                    reason=str(exc.details.get("reason") or "crystal_invalid_numeric_value"),
                    subject="target",
                )
            if actual_stone != expected_stone:
                current = int(actual_stone or 0)
                return AdminImpartStoneResult(
                    "state_changed", current, current,
                    reason="crystal_changed" if rows else "impart_missing",
                    subject="target",
                )

            previous_stone = int(actual_stone or 0)
            final_stone = max(0, previous_stone + requested_delta)
            applied_delta = final_stone - previous_stone
            if actual_stone is None:
                uow.execute(
                    "INSERT INTO impart_data.xiuxian_impart"
                    "(user_id,stone_num,stone_num_exact,stone_num_precision_state) "
                    "VALUES(?,?,?,'exact')",
                    (user_id, final_stone, str(final_stone)),
                )
            else:
                changed = uow.execute(
                    "UPDATE impart_data.xiuxian_impart SET stone_num=?,stone_num_exact=?,"
                    "stone_num_precision_state='exact' WHERE user_id=? "
                    "AND xiuxian_int_cmp(COALESCE(stone_num_exact,stone_num,0),?)=0 "
                    "AND stone_num_precision_state='exact'",
                    (final_stone, str(final_stone), user_id, previous_stone),
                )
                if changed.rowcount != 1:
                    raise DomainError("impart_write_rejected", "admin impart adjustment was not saved")

            item_delta = json.dumps(
                [{
                    "id": "impart_stone",
                    "name": "思恋结晶",
                    "type": "传承货币",
                    "amount": applied_delta,
                }],
                ensure_ascii=False,
                separators=(",", ":"),
            )
            detail = json.dumps(
                {
                    "operator_id": operator_id,
                    "target_name": str(target_name),
                    "requested_delta": requested_delta,
                    "previous_stone": previous_stone,
                    "final_stone": final_stone,
                },
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            )
            uow.execute(
                "INSERT INTO economy_log("
                "user_id,source,action,item_delta,detail,trace_id,created_at) "
                "VALUES(?,'admin',?,?,?,?,CURRENT_TIMESTAMP)",
                (
                    user_id,
                    "admin_impart_stone_add"
                    if requested_delta > 0
                    else "admin_impart_stone_cost",
                    item_delta,
                    detail,
                    operation_id,
                ),
            )
            uow.execute(
                "INSERT INTO admin_impart_stone_operations("
                "operation_id,payload,previous_stone,final_stone,applied_delta,"
                "previous_stone_exact,final_stone_exact,applied_delta_exact) "
                "VALUES(?,?,?,?,?,?,?,?)",
                (operation_id, payload, previous_stone, final_stone, applied_delta,
                 str(previous_stone), str(final_stone), str(applied_delta)),
            )
            return AdminImpartStoneResult(
                "adjusted", previous_stone, final_stone, applied_delta
            )


__all__ = [
    "AdminImpartStoneSqlRepository",
    "AdminImpartStoneResult",
    "AdminImpartStoneSnapshot",
]
