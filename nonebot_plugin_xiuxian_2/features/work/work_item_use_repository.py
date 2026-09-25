from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping

from ...infrastructure.database import DatabaseUnitOfWork


@dataclass(frozen=True)
class WorkItemUseResult:
    status: str
    action: str | None = None
    item_remaining: int = 0
    result_snapshot: dict | None = None

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


class WorkItemUseSqlRepository:
    """Atomically consume an acceleration order and finish the active work timer."""

    def __init__(self, database: str | Path) -> None:
        self.database = str(database)

    def accelerate(
        self,
        operation_id: str,
        user_id: str,
        item_id: int,
        expected_item_count: int,
        expected_work: Mapping[str, object],
        accelerated_at: str,
    ) -> WorkItemUseResult:
        operation_id = str(operation_id).strip()
        user_id = str(user_id)
        item_id = int(item_id)
        expected_item_count = int(expected_item_count)
        expected = dict(expected_work)
        result = {"accelerated_at": str(accelerated_at)}
        if not operation_id or expected_item_count <= 0:
            raise ValueError("valid operation and item snapshot are required")

        payload = json.dumps(
            [user_id, item_id, expected_item_count, "accelerate", expected, result],
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
            default=str,
        )
        result_json = json.dumps(result, ensure_ascii=True, sort_keys=True, separators=(",", ":"))

        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            previous = uow.query_one(
                "SELECT payload,action,item_remaining,result_snapshot "
                "FROM work_item_use_operations WHERE operation_id=?",
                (operation_id,),
            )
            if previous is not None:
                if str(previous["payload"]) != payload:
                    return WorkItemUseResult("operation_conflict")
                return WorkItemUseResult(
                    "duplicate",
                    str(previous["action"]),
                    int(previous["item_remaining"]),
                    json.loads(str(previous["result_snapshot"])),
                )

            item = uow.query_one(
                "SELECT COALESCE(goods_num,0) AS goods_num,COALESCE(bind_num,0) AS bind_num "
                "FROM back WHERE user_id=? AND goods_id=?",
                (user_id, item_id),
            )
            work = uow.query_one(
                "SELECT COALESCE(type,0) AS type,create_time,scheduled_time "
                "FROM user_cd WHERE user_id=?",
                (user_id,),
            )
            if work is None:
                return WorkItemUseResult("user_missing")
            if item is None or int(item["goods_num"] or 0) < 1:
                return WorkItemUseResult("item_missing")
            if int(item["goods_num"]) != expected_item_count:
                return WorkItemUseResult("state_changed")

            actual_work = {
                "type": int(work["type"]),
                "create_time": str(work["create_time"]),
                "scheduled_time": str(work["scheduled_time"]),
            }
            normalized_expected = {
                "type": int(expected.get("type", 0)),
                "create_time": str(expected.get("create_time")),
                "scheduled_time": str(expected.get("scheduled_time")),
            }
            if actual_work != normalized_expected or actual_work["type"] != 2:
                return WorkItemUseResult("state_changed")

            updated_work = uow.execute(
                "UPDATE user_cd SET create_time=? WHERE user_id=? AND type=2 "
                "AND create_time=? AND scheduled_time=?",
                (
                    result["accelerated_at"],
                    user_id,
                    work["create_time"],
                    work["scheduled_time"],
                ),
            )
            if updated_work.rowcount != 1:
                return WorkItemUseResult("state_changed")

            remaining = expected_item_count - 1
            bind_remaining = min(max(0, int(item["bind_num"] or 0) - 1), remaining)
            updated_item = uow.execute(
                "UPDATE back SET goods_num=?,bind_num=? "
                "WHERE user_id=? AND goods_id=? AND goods_num=?",
                (remaining, bind_remaining, user_id, item_id, expected_item_count),
            )
            if updated_item.rowcount != 1:
                uow.execute("ROLLBACK")
                return WorkItemUseResult("state_changed")

            uow.execute(
                "INSERT INTO work_item_use_operations "
                "(operation_id,payload,action,item_remaining,result_snapshot) "
                "VALUES(?,?,?,?,?)",
                (operation_id, payload, "accelerate", remaining, result_json),
            )
            return WorkItemUseResult("applied", "accelerate", remaining, result)


__all__ = ["WorkItemUseResult", "WorkItemUseSqlRepository"]
