from __future__ import annotations

import json
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from threading import RLock
from ..xiuxian_utils import db_backend
from ..xiuxian_utils.numeric_bind import operation_payload_matches

@dataclass(frozen=True)
class WorkClaimResult:
    status: str
    task_name: str | None = None
    started_at: str | None = None
    remaining_count: int = 0

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}

class WorkClaimService:
    """Atomically claim one work offer and persist its immutable snapshot.

    work_num 是「刷新次数」，只在刷新时消耗；接取不扣次数。
    """

    def __init__(self, database: str | Path, lock: RLock | None = None) -> None:
        self._database = Path(database)
        self._lock = lock or RLock()

    @staticmethod
    def _payload(user_id, task_index) -> str:
        # Request identity only — count/offer/start time are concurrency checks or outcomes.
        return json.dumps([str(user_id), int(task_index)], ensure_ascii=True, separators=(",", ":"))

    def get_result(self, operation_id: str) -> WorkClaimResult | None:
        operation_id = str(operation_id).strip()
        if not operation_id:
            return None
        with self._lock, closing(db_backend.connect(self._database)) as conn:
            conn.execute(
                "CREATE TABLE IF NOT EXISTS work_claim_operations ("
                "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,task_name TEXT NOT NULL,"
                "started_at TEXT NOT NULL,remaining_count INTEGER NOT NULL,"
                "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
            )
            previous = conn.execute(
                "SELECT payload,task_name,started_at,remaining_count FROM work_claim_operations "
                "WHERE operation_id=%s",
                (operation_id,),
            ).fetchone()
            if previous is None:
                return None
            return WorkClaimResult("duplicate", str(previous[1]), str(previous[2]), int(previous[3]))

    def claim(
        self,
        operation_id,
        user_id,
        expected_count,
        expected_offer,
        task_index,
        started_at,
    ) -> WorkClaimResult:
        operation_id = str(operation_id).strip()
        user_id = str(user_id)
        expected_count = int(expected_count)
        task_index = int(task_index)
        started_at = str(started_at)
        offer = dict(expected_offer)
        tasks = list(dict(offer.get("tasks") or {}).items())
        # 优先使用刷新时固化的 task_order，避免 sort_keys 后编号错位
        order = offer.get("task_order")
        if isinstance(order, list) and order:
            task_map = dict(tasks)
            ordered = []
            for name in order:
                key = str(name)
                if key in task_map:
                    ordered.append((key, task_map[key]))
            for name, data in tasks:
                if name not in {n for n, _ in ordered}:
                    ordered.append((name, data))
            tasks = ordered
        if not operation_id:
            raise ValueError("operation_id is required")
        # 编号越界：返回状态，不要抛异常把 Matcher 打成 ERROR
        # expected_count 仅作并发快照（刷新次数），接取不消耗
        if expected_count < 0:
            return WorkClaimResult("state_changed")
        if task_index < 1 or task_index > len(tasks):
            return WorkClaimResult("invalid_task")
        task_name, task_data = tasks[task_index - 1]
        snapshot = {
            "tasks": offer["tasks"],
            "task_order": [name for name, _ in tasks],
            "status": 2,
            "refresh_time": offer.get("refresh_time"),
            "user_level": offer.get("user_level"),
            "selected_task": task_name,
            "selected_task_data": task_data,
        }
        payload = self._payload(user_id, task_index)
        snapshot_json = json.dumps(snapshot, ensure_ascii=True, sort_keys=True, separators=(",", ":"))

        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS work_claim_operations ("
                    "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,task_name TEXT NOT NULL,"
                    "started_at TEXT NOT NULL,remaining_count INTEGER NOT NULL,"
                    "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
                )
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS work_active_snapshots ("
                    "user_id TEXT PRIMARY KEY,snapshot TEXT NOT NULL,updated_at TEXT NOT NULL)"
                )
                previous = conn.execute(
                    "SELECT payload,task_name,started_at,remaining_count FROM work_claim_operations "
                    "WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    if not operation_payload_matches(previous[0], payload):
                        return WorkClaimResult("operation_conflict")
                    return WorkClaimResult("duplicate", str(previous[1]), str(previous[2]), int(previous[3]))

                user = conn.execute(
                    "SELECT COALESCE(work_num,0) FROM user_xiuxian WHERE user_id=%s", (user_id,)
                ).fetchone()
                work = conn.execute(
                    "SELECT COALESCE(type,0),create_time,scheduled_time FROM user_cd WHERE user_id=%s",
                    (user_id,),
                ).fetchone()
                if user is None or work is None:
                    conn.rollback()
                    return WorkClaimResult("user_missing")
                # 并发校验：刷新次数快照 + 当前空闲(type=0)
                if int(user[0]) != expected_count or int(work[0]) != 0:
                    conn.rollback()
                    return WorkClaimResult("state_changed")

                # 接取不扣 work_num（刷新次数）
                remaining = expected_count
                updated = conn.execute(
                    "UPDATE user_cd SET type=2,create_time=%s,scheduled_time=%s "
                    "WHERE user_id=%s AND COALESCE(type,0)=0",
                    (started_at, task_name, user_id),
                )
                if getattr(updated, "rowcount", 1) == 0:
                    conn.rollback()
                    return WorkClaimResult("state_changed")
                conn.execute(
                    "INSERT INTO work_active_snapshots(user_id,snapshot,updated_at) VALUES(%s,%s,%s) "
                    "ON CONFLICT(user_id) DO UPDATE SET snapshot=EXCLUDED.snapshot,updated_at=EXCLUDED.updated_at",
                    (user_id, snapshot_json, started_at),
                )
                conn.execute(
                    "INSERT INTO work_claim_operations(operation_id,payload,task_name,started_at,remaining_count) "
                    "VALUES(%s,%s,%s,%s,%s)",
                    (operation_id, payload, task_name, started_at, remaining),
                )
                conn.commit()
                return WorkClaimResult("applied", task_name, started_at, remaining)
            except Exception:
                conn.rollback()
                raise

from ...compatibility.legacy_work_item_use import WorkItemUseResult, WorkItemUseService
from ...compatibility.legacy_work_settlement import WorkSettlementResult, WorkSettlementService

from ...compatibility.legacy_work_daily_refresh_reset import (
    WorkDailyRefreshResetResult,
    WorkDailyRefreshResetService,
)

from ...compatibility.legacy_work_abort_cleanup import WorkAbortCleanupResult, WorkAbortCleanupService
from ...compatibility.legacy_work_refresh import WorkRefreshResult, WorkRefreshSettlementService


__all__ = [
    "WorkClaimResult",
    "WorkClaimService",
    "WorkSettlementResult",
    "WorkSettlementService",
    "WorkItemUseResult",
    "WorkItemUseService",
    "WorkRefreshResult",
    "WorkRefreshSettlementService",
    "WorkAbortCleanupResult",
    "WorkAbortCleanupService",
    "WorkDailyRefreshResetResult",
    "WorkDailyRefreshResetService",
]
