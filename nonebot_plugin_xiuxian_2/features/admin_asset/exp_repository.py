from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

from ...infrastructure.database import DatabaseUnitOfWork


@dataclass(frozen=True)
class AdminExpAdjustmentResult:
    status: str
    previous_exp: int = 0
    final_exp: int = 0
    applied_delta: int = 0

    @property
    def succeeded(self) -> bool:
        return self.status in {"adjusted", "duplicate"}


@dataclass(frozen=True)
class AdminExpBatchAdjustmentResult:
    status: str
    affected_users: int = 0
    applied_delta: int = 0

    @property
    def succeeded(self) -> bool:
        return self.status in {"adjusted", "duplicate"}


class AdminExpAdjustmentSqlRepository:
    def __init__(self, database: str | Path) -> None:
        self.database = Path(database)

    @staticmethod
    def _schema_ready(uow: DatabaseUnitOfWork) -> bool:
        tables = {
            str(row["name"])
            for row in uow.query_all(
                "SELECT name FROM sqlite_master WHERE type='table' "
                "AND name IN (?,?,?)",
                ("admin_exp_adjustment_operations", "economy_log", "user_xiuxian"),
            )
        }
        if tables != {"admin_exp_adjustment_operations", "economy_log", "user_xiuxian"}:
            return False

        required_columns = {
            "admin_exp_adjustment_operations": {
                "operation_id", "payload", "previous_exp", "final_exp", "applied_delta",
            },
            "economy_log": {
                "user_id", "source", "action", "exp_delta", "item_delta",
                "detail", "trace_id", "created_at",
            },
            "user_xiuxian": {"user_id", "exp"},
        }
        for table, required in required_columns.items():
            columns = {
                str(row["name"]).casefold()
                for row in uow.query_all(f'PRAGMA table_info("{table}")')
            }
            if not required.issubset(columns):
                return False
        return True

    def adjust(self, operation_id: str, operator_id: str, user_id: str, expected_exp: int, requested_delta: int, *, target_name: str = "") -> AdminExpAdjustmentResult:
        operation_id, operator_id, user_id = str(operation_id).strip(), str(operator_id).strip(), str(user_id).strip()
        expected_exp, requested_delta = int(expected_exp), int(requested_delta)
        if not operation_id or not operator_id or not user_id or expected_exp < 0 or requested_delta == 0:
            raise ValueError("valid experience adjustment request is required")
        payload = json.dumps([operator_id, user_id, requested_delta], ensure_ascii=True, separators=(",", ":"))
        if not self.database.is_file():
            return AdminExpAdjustmentResult("schema_missing")
        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            if not self._schema_ready(uow):
                return AdminExpAdjustmentResult("schema_missing")
            previous = uow.query_one("SELECT payload,previous_exp,final_exp,applied_delta FROM admin_exp_adjustment_operations WHERE operation_id=?", (operation_id,))
            if previous is not None:
                if str(previous["payload"]) != payload:
                    return AdminExpAdjustmentResult("operation_conflict")
                return AdminExpAdjustmentResult("duplicate", int(previous["previous_exp"]), int(previous["final_exp"]), int(previous["applied_delta"]))
            row = uow.query_one("SELECT COALESCE(exp,0) AS exp FROM user_xiuxian WHERE user_id=?", (user_id,))
            if row is None:
                return AdminExpAdjustmentResult("user_missing")
            actual = int(row["exp"])
            if actual != expected_exp:
                return AdminExpAdjustmentResult("state_changed", actual, actual)
            final = max(0, expected_exp + requested_delta)
            applied = final - expected_exp
            if uow.execute("UPDATE user_xiuxian SET exp=? WHERE user_id=? AND COALESCE(exp,0)=?", (final, user_id, expected_exp)).rowcount != 1:
                return AdminExpAdjustmentResult("state_changed")
            detail = json.dumps({"operator_id": operator_id, "target_name": str(target_name), "requested_delta": requested_delta, "previous_exp": expected_exp, "final_exp": final}, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
            uow.execute("INSERT INTO economy_log(user_id,source,action,exp_delta,item_delta,detail,trace_id,created_at) VALUES(?,'admin',?,?,'[]',?,?,CURRENT_TIMESTAMP)", (user_id, "admin_exp_add" if applied > 0 else "admin_exp_cost", applied, detail, operation_id))
            uow.execute("INSERT INTO admin_exp_adjustment_operations(operation_id,payload,previous_exp,final_exp,applied_delta) VALUES(?,?,?,?,?)", (operation_id, payload, expected_exp, final, applied))
            return AdminExpAdjustmentResult("adjusted", expected_exp, final, applied)

    def adjust_all(
        self, operation_id: str, operator_id: str, requested_delta: int
    ) -> AdminExpBatchAdjustmentResult:
        operation_id, operator_id = str(operation_id).strip(), str(operator_id).strip()
        requested_delta = int(requested_delta)
        if not operation_id or not operator_id:
            raise ValueError("valid experience batch adjustment request is required")
        payload = json.dumps(
            [operator_id, requested_delta], ensure_ascii=True, separators=(",", ":")
        )
        if not self.database.is_file():
            return AdminExpBatchAdjustmentResult("schema_missing")
        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            tables = {
                str(row["name"])
                for row in uow.query_all(
                    "SELECT name FROM sqlite_master WHERE type='table' "
                    "AND name IN (?,?)",
                    ("admin_exp_batch_adjustment_operations", "user_xiuxian"),
                )
            }
            required_columns = {
                "admin_exp_batch_adjustment_operations": {
                    "operation_id", "payload", "affected_users", "applied_delta",
                },
                "user_xiuxian": {"user_id", "exp"},
            }
            if tables != set(required_columns) or any(
                not required.issubset(
                    {
                        str(row["name"]).casefold()
                        for row in uow.query_all(f'PRAGMA table_info("{table}")')
                    }
                )
                for table, required in required_columns.items()
            ):
                return AdminExpBatchAdjustmentResult("schema_missing")

            previous = uow.query_one(
                "SELECT payload,affected_users,applied_delta "
                "FROM admin_exp_batch_adjustment_operations WHERE operation_id=?",
                (operation_id,),
            )
            if previous is not None:
                if str(previous["payload"]) != payload:
                    return AdminExpBatchAdjustmentResult("operation_conflict")
                return AdminExpBatchAdjustmentResult(
                    "duplicate",
                    int(previous["affected_users"]),
                    int(previous["applied_delta"]),
                )

            if requested_delta == 0:
                affected_users = 0
            else:
                affected_users = int(
                    uow.execute(
                        "UPDATE user_xiuxian SET exp="
                        "CAST(COALESCE(exp,0) AS REAL)+CAST(? AS REAL)",
                        (requested_delta,),
                    ).rowcount
                )
            status = "adjusted" if affected_users else "no_targets"
            applied_delta = requested_delta * affected_users
            uow.execute(
                "INSERT INTO admin_exp_batch_adjustment_operations("
                "operation_id,payload,affected_users,applied_delta) VALUES(?,?,?,?)",
                (operation_id, payload, affected_users, applied_delta),
            )
            return AdminExpBatchAdjustmentResult(
                status, affected_users, applied_delta
            )


__all__ = [
    "AdminExpAdjustmentSqlRepository",
    "AdminExpAdjustmentResult",
    "AdminExpBatchAdjustmentResult",
]
