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

    @staticmethod
    def _schema_ready(uow: DatabaseUnitOfWork) -> bool:
        tables = {
            str(row["name"])
            for row in uow.query_all(
                "SELECT name FROM sqlite_master WHERE type='table' "
                "AND name IN ("
                "'work_claim_operations','work_active_snapshots','work_offer_snapshots')"
            )
        }
        return tables == {
            "work_claim_operations",
            "work_active_snapshots",
            "work_offer_snapshots",
        }

    def get_active_snapshot(self, user_id: str) -> dict | None:
        if not Path(self.database).is_file():
            return None
        with DatabaseUnitOfWork(self.database, read_only=True) as uow:
            table = uow.query_one(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='work_active_snapshots'"
            )
            if table is None:
                return None
            row = uow.query_one(
                "SELECT s.snapshot,cd.type AS cd_type,cd.create_time AS cd_create_time,"
                "cd.scheduled_time AS cd_scheduled_time "
                "FROM work_active_snapshots AS s LEFT JOIN user_cd AS cd ON cd.user_id=s.user_id "
                "WHERE s.user_id=?",
                (str(user_id),),
            )
            if row is None:
                return None
            snapshot = json.loads(str(row["snapshot"]))
            if not isinstance(snapshot, dict):
                raise ValueError("active work snapshot must be an object")
            if int(row.get("cd_type") or 0) == 2:
                scheduled_time = str(row.get("cd_scheduled_time") or "")
                create_time = str(row.get("cd_create_time") or "")
                if int(snapshot.get("status", 0) or 0) == 1:
                    snapshot["status"] = 2
                snapshot.setdefault("scheduled_time", scheduled_time)
                snapshot.setdefault("create_time", create_time)
            return snapshot

    @staticmethod
    def _ordered_task_names(expected_offer: Mapping) -> list[str]:
        tasks = dict(expected_offer.get("tasks", {}))
        task_order = expected_offer.get("task_order")
        if not isinstance(task_order, list) or not task_order:
            return list(tasks)

        names: list[str] = []
        seen: set[str] = set()
        for name in task_order:
            key = str(name)
            if key in tasks and key not in seen:
                names.append(key)
                seen.add(key)
        names.extend(name for name in tasks if name not in seen)
        return names

    def claim(
        self,
        operation_id: str,
        user_id: str,
        expected_count: int,
        expected_offer: Mapping,
        task_index: int,
        started_at: str,
    ) -> WorkClaimResult:
        operation_id, user_id = str(operation_id).strip(), str(user_id)
        expected_count, task_index = int(expected_count), int(task_index)
        tasks = dict(expected_offer.get("tasks", {}))
        if not operation_id or expected_count < 0 or task_index < 1 or task_index > len(tasks):
            raise ValueError("valid operation, count, task and offer are required")
        names = self._ordered_task_names(expected_offer)
        task_name = names[task_index - 1]
        payload = json.dumps([user_id, task_index], ensure_ascii=False, separators=(",", ":"))
        if not Path(self.database).is_file():
            return WorkClaimResult("schema_missing")
        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            if not self._schema_ready(uow):
                return WorkClaimResult("schema_missing")
            previous = uow.query_one(
                "SELECT payload,task_name,started_at,remaining_count "
                "FROM work_claim_operations WHERE operation_id=?",
                (operation_id,),
            )
            if previous is not None:
                if str(previous["payload"]) != payload:
                    return WorkClaimResult("state_changed")
                return WorkClaimResult(
                    "duplicate",
                    str(previous["task_name"]),
                    str(previous["started_at"]),
                    int(previous["remaining_count"]),
                )
            user = uow.query_one("SELECT COALESCE(work_num,0) AS work_num FROM user_xiuxian WHERE user_id=?", (user_id,))
            cooldown = uow.query_one("SELECT COALESCE(type,0) AS type FROM user_cd WHERE user_id=?", (user_id,))
            if user is None:
                return WorkClaimResult("user_missing")
            current_count = int(user["work_num"])
            if cooldown is None or int(cooldown["type"]) != 0 or current_count != expected_count:
                return WorkClaimResult("state_changed", remaining_count=current_count)
            remaining = current_count
            task_order = expected_offer.get("task_order") or list(tasks)
            offer_projection = {
                "tasks": tasks,
                "task_order": list(task_order),
                "status": 2,
                "refresh_time": expected_offer.get("refresh_time"),
                "user_level": expected_offer.get("user_level"),
            }
            offer_json = json.dumps(offer_projection, ensure_ascii=True, sort_keys=True)
            active_snapshot = {
                **offer_projection,
                "scheduled_time": task_name,
                "create_time": started_at,
            }
            active_json = json.dumps(active_snapshot, ensure_ascii=False, sort_keys=True)
            uow.execute(
                "UPDATE user_xiuxian SET work_num=? WHERE user_id=? AND work_num=?",
                (remaining, user_id, expected_count),
            )
            uow.execute(
                "UPDATE user_cd SET type=2,create_time=?,scheduled_time=? "
                "WHERE user_id=? AND COALESCE(type,0)=0",
                (started_at, task_name, user_id),
            )
            uow.execute(
                "INSERT INTO work_active_snapshots(user_id,snapshot,updated_at) VALUES(?,?,?) "
                "ON CONFLICT(user_id) DO UPDATE SET snapshot=excluded.snapshot,"
                "updated_at=excluded.updated_at",
                (user_id, active_json, started_at),
            )
            uow.execute(
                "INSERT INTO work_offer_snapshots(user_id,snapshot,updated_at) VALUES(?,?,?) "
                "ON CONFLICT(user_id) DO UPDATE SET snapshot=excluded.snapshot,updated_at=excluded.updated_at",
                (user_id, offer_json, str(offer_projection.get("refresh_time") or "")),
            )
            uow.execute(
                "INSERT INTO work_claim_operations"
                "(operation_id,payload,task_name,started_at,remaining_count) VALUES(?,?,?,?,?)",
                (operation_id, payload, task_name, started_at, remaining),
            )
            return WorkClaimResult("applied", task_name, started_at, remaining)


__all__ = ["WorkClaimSqlRepository", "WorkClaimResult"]
