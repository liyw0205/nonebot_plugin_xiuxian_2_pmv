from __future__ import annotations

import json
from pathlib import Path

from ...infrastructure.database import DatabaseUnitOfWork


class AdminStoneResult:
    def __init__(self, status: str, user_id: str, previous_stone: int = 0, final_stone: int = 0, applied_delta: int = 0) -> None:
        self.status = status
        self.user_id = user_id
        self.previous_stone = previous_stone
        self.final_stone = final_stone
        self.applied_delta = applied_delta


class AdminStoneSqlRepository:
    def __init__(self, database: str | Path) -> None:
        self.database = str(database)

    def adjust(self, operation_id: str, operator_id: str, user_id: str, expected_stone: int, requested_delta: int, **kwargs) -> AdminStoneResult:
        operation_id = str(operation_id).strip()
        operator_id = str(operator_id).strip()
        user_id = str(user_id).strip()
        expected_stone = int(expected_stone)
        requested_delta = int(requested_delta)
        target_name = str(kwargs.get("target_name", ""))
        payload = json.dumps(
            [operator_id, user_id, requested_delta],
            ensure_ascii=True,
            separators=(",", ":"),
        )
        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            tables = {
                str(row["name"])
                for row in uow.query_all(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name IN (?,?)",
                    ("admin_stone_adjustment_operations", "economy_log"),
                )
            }
            if tables != {"admin_stone_adjustment_operations", "economy_log"}:
                return AdminStoneResult("not_ready", user_id)
            economy_columns = {
                str(row["name"]).casefold()
                for row in uow.query_all('PRAGMA table_info("economy_log")')
            }
            if "trace_id" not in economy_columns:
                return AdminStoneResult("not_ready", user_id)

            previous = uow.query_one(
                "SELECT payload,previous_stone,final_stone,applied_delta "
                "FROM admin_stone_adjustment_operations WHERE operation_id=?",
                (operation_id,),
            )
            if previous is not None:
                if str(previous["payload"]) != payload:
                    return AdminStoneResult("operation_conflict", user_id)
                return AdminStoneResult(
                    "duplicate",
                    user_id,
                    int(previous["previous_stone"]),
                    int(previous["final_stone"]),
                    int(previous["applied_delta"]),
                )

            old_feature_table = uow.query_one(
                "SELECT 1 AS present FROM sqlite_master "
                "WHERE type='table' AND name='admin_stone_operations'"
            )
            if old_feature_table is not None:
                old_receipt = uow.query_one(
                    "SELECT payload,user_id,previous_stone,final_stone,applied_delta "
                    "FROM admin_stone_operations WHERE operation_id=?",
                    (operation_id,),
                )
                if old_receipt is not None:
                    try:
                        old_payload = json.loads(str(old_receipt["payload"]))
                    except (TypeError, ValueError, json.JSONDecodeError):
                        old_payload = None
                    same_effect = (
                        isinstance(old_payload, list)
                        and old_payload == [operator_id, user_id]
                        and str(old_receipt["user_id"]) == user_id
                        and int(old_receipt["previous_stone"]) == expected_stone
                        and int(old_receipt["applied_delta"]) == requested_delta
                        and int(old_receipt["final_stone"]) == max(0, expected_stone + requested_delta)
                    )
                    if same_effect:
                        return AdminStoneResult(
                            "duplicate",
                            user_id,
                            int(old_receipt["previous_stone"]),
                            int(old_receipt["final_stone"]),
                            int(old_receipt["applied_delta"]),
                        )
                    return AdminStoneResult("operation_conflict", user_id)

            row = uow.query_one("SELECT COALESCE(stone,0) AS stone FROM user_xiuxian WHERE user_id=?", (user_id,))
            if row is None:
                return AdminStoneResult("user_missing", user_id)
            current = int(row["stone"])
            if current != int(expected_stone):
                return AdminStoneResult("state_changed", user_id, current, current, 0)
            final = max(0, current + requested_delta)
            applied_delta = final - current
            changed = uow.execute(
                "UPDATE user_xiuxian SET stone=? WHERE user_id=? AND COALESCE(stone,0)=?",
                (final, user_id, current),
            )
            if changed.rowcount != 1:
                return AdminStoneResult("state_changed", user_id)
            detail = json.dumps(
                {
                    "operator_id": operator_id,
                    "target_name": target_name,
                    "requested_delta": requested_delta,
                    "previous_stone": current,
                    "final_stone": final,
                },
                ensure_ascii=True,
                sort_keys=True,
                separators=(",", ":"),
            )
            uow.execute(
                "INSERT INTO economy_log(user_id,source,action,stone_delta,item_delta,detail,trace_id,created_at) "
                "VALUES(?,'admin',?,?,'[]',?,?,CURRENT_TIMESTAMP)",
                (
                    user_id,
                    "admin_stone_add" if applied_delta > 0 else "admin_stone_cost",
                    applied_delta,
                    detail,
                    operation_id,
                ),
            )
            uow.execute(
                "INSERT INTO admin_stone_adjustment_operations("
                "operation_id,payload,previous_stone,final_stone,applied_delta) VALUES(?,?,?,?,?)",
                (operation_id, payload, current, final, applied_delta),
            )
            return AdminStoneResult("adjusted", user_id, current, final, applied_delta)


__all__ = ["AdminStoneSqlRepository", "AdminStoneResult"]
