from __future__ import annotations

import json
from pathlib import Path
from typing import Mapping

from ...infrastructure.database import DatabaseUnitOfWork


class WorkClaimResult:
    def __init__(self, status: str, task_name: str = "", started_at: str = "", remaining_count: int = 0) -> None:
        self.status = status
        self.task_name = task_name
        self.started_at = started_at
        self.remaining_count = remaining_count


class WorkClaimSqlRepository:
    def __init__(self, database: str | Path) -> None:
        self.database = str(database)

    def claim(self, operation_id: str, user_id: str, expected_count: int, expected_offer: Mapping, task_index: int, started_at: str) -> WorkClaimResult:
        operation_id, user_id = str(operation_id).strip(), str(user_id)
        expected_count, task_index = int(expected_count), int(task_index)
        tasks = dict(expected_offer.get("tasks", {}))
        if not operation_id or expected_count < 0 or task_index < 1 or task_index > len(tasks):
            raise ValueError("valid operation, count, task and offer are required")
        names = list(tasks)
        task_name = names[task_index - 1]
        payload = json.dumps([user_id, task_index], ensure_ascii=False, separators=(",", ":"))
        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            uow.execute("CREATE TABLE IF NOT EXISTS work_claim_operations(operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,task_name TEXT NOT NULL,started_at TEXT NOT NULL,remaining_count INTEGER NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)")
            uow.execute("CREATE TABLE IF NOT EXISTS work_active_snapshots(user_id TEXT PRIMARY KEY,snapshot TEXT NOT NULL,updated_at TEXT NOT NULL)")
            previous = uow.query_one("SELECT payload,task_name,started_at,remaining_count FROM work_claim_operations WHERE operation_id=?", (operation_id,))
            if previous is not None:
                if str(previous["payload"]) != payload:
                    return WorkClaimResult("state_changed")
                return WorkClaimResult("duplicate", str(previous["task_name"]), str(previous["started_at"]), int(previous["remaining_count"]))
            user = uow.query_one("SELECT COALESCE(work_num,0) AS work_num FROM user_xiuxian WHERE user_id=?", (user_id,))
            cooldown = uow.query_one("SELECT COALESCE(type,0) AS type FROM user_cd WHERE user_id=?", (user_id,))
            if user is None:
                return WorkClaimResult("user_missing")
            current_count = int(user["work_num"])
            if cooldown is None or int(cooldown["type"]) != 0 or current_count != expected_count:
                return WorkClaimResult("state_changed", remaining_count=current_count)
            remaining = current_count
            snapshot = json.dumps(expected_offer, ensure_ascii=False, sort_keys=True)
            uow.execute("UPDATE user_xiuxian SET work_num=? WHERE user_id=? AND work_num=?", (remaining, user_id, expected_count))
            uow.execute("UPDATE user_cd SET type=2,create_time=?,scheduled_time=? WHERE user_id=? AND COALESCE(type,0)=0", (started_at, task_name, user_id))
            uow.execute("INSERT INTO work_active_snapshots(user_id,snapshot,updated_at) VALUES(?,?,?) ON CONFLICT(user_id) DO UPDATE SET snapshot=excluded.snapshot,updated_at=excluded.updated_at", (user_id, snapshot, started_at))
            uow.execute("INSERT INTO work_claim_operations(operation_id,payload,task_name,started_at,remaining_count) VALUES(?,?,?,?,?)", (operation_id, payload, task_name, started_at, remaining))
            return WorkClaimResult("applied", task_name, started_at, remaining)


__all__ = ["WorkClaimSqlRepository", "WorkClaimResult"]
