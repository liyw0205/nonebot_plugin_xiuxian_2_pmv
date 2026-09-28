from __future__ import annotations

import json
from contextlib import closing
from dataclasses import dataclass, field
from pathlib import Path
from threading import RLock
from ..xiuxian_utils import db_backend
from ..xiuxian_utils.numeric_bind import operation_payload_matches
from datetime import datetime
from datetime import date, datetime

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

@dataclass(frozen=True)
class WorkDailyRefreshResetResult:
    status: str
    business_date: str
    task_status: str = ""
    reset_count: int = 0
    total: int = 0
    completed: int = 0
    changed: int = 0

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}

class WorkDailyRefreshResetService:
    """Reset a date-frozen player set in durable chunks."""

    def __init__(self, database: str | Path, lock: RLock | None = None) -> None:
        self._database = Path(database)
        self._lock = lock or RLock()

    @staticmethod
    def _normalize_date(value) -> str:
        if isinstance(value, datetime):
            value = value.date()
        if isinstance(value, date):
            return value.isoformat()
        return date.fromisoformat(str(value).strip()).isoformat()

    @staticmethod
    def _ensure_schema(conn) -> None:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS work_daily_refresh_reset_operations("
            "business_date TEXT PRIMARY KEY,reset_count INTEGER NOT NULL,total INTEGER NOT NULL,"
            "completed INTEGER NOT NULL DEFAULT 0,changed INTEGER NOT NULL DEFAULT 0,"
            "status TEXT NOT NULL DEFAULT 'running',created_at TEXT NOT NULL,updated_at TEXT NOT NULL)"
        )
        conn.execute(
            "CREATE TABLE IF NOT EXISTS work_daily_refresh_reset_targets("
            "business_date TEXT NOT NULL,user_id TEXT NOT NULL,status TEXT NOT NULL DEFAULT 'pending',"
            "previous_count INTEGER,final_count INTEGER,updated_at TEXT NOT NULL,"
            "PRIMARY KEY(business_date,user_id))"
        )

    @staticmethod
    def _result(conn, business_date, status):
        row = conn.execute(
            "SELECT reset_count,total,completed,changed,status "
            "FROM work_daily_refresh_reset_operations WHERE business_date=%s",
            (business_date,),
        ).fetchone()
        if row is None:
            return WorkDailyRefreshResetResult(status, business_date)
        return WorkDailyRefreshResetResult(
            status,
            business_date,
            str(row[4]),
            int(row[0]),
            int(row[1]),
            int(row[2]),
            int(row[3]),
        )

    def reset(
        self,
        business_date,
        reset_count,
        *,
        chunk_size=500,
        updated_at=None,
    ) -> WorkDailyRefreshResetResult:
        business_date = self._normalize_date(business_date)
        reset_count = int(reset_count)
        chunk_size = max(1, int(chunk_size))
        updated_at = str(updated_at or datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        if reset_count < 0:
            raise ValueError("reset count must not be negative")

        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_schema(conn)
                operation = conn.execute(
                    "SELECT reset_count,status FROM work_daily_refresh_reset_operations "
                    "WHERE business_date=%s",
                    (business_date,),
                ).fetchone()
                if operation is None:
                    user_ids = tuple(
                        str(row[0])
                        for row in conn.execute(
                            "SELECT DISTINCT user_id FROM user_xiuxian ORDER BY user_id"
                        ).fetchall()
                    )
                    task_status = "completed" if not user_ids else "running"
                    conn.execute(
                        "INSERT INTO work_daily_refresh_reset_operations("
                        "business_date,reset_count,total,status,created_at,updated_at) "
                        "VALUES(%s,%s,%s,%s,%s,%s)",
                        (
                            business_date,
                            reset_count,
                            len(user_ids),
                            task_status,
                            updated_at,
                            updated_at,
                        ),
                    )
                    conn.executemany(
                        "INSERT INTO work_daily_refresh_reset_targets("
                        "business_date,user_id,updated_at) VALUES(%s,%s,%s)",
                        (
                            (business_date, user_id, updated_at)
                            for user_id in user_ids
                        ),
                    )
                    conn.commit()
                    if not user_ids:
                        return self._result(conn, business_date, "applied")
                else:
                    if int(operation[0]) != reset_count:
                        result = self._result(conn, business_date, "operation_conflict")
                        conn.rollback()
                        return result
                    if str(operation[1]) == "completed":
                        result = self._result(conn, business_date, "duplicate")
                        conn.rollback()
                        return result
                    conn.commit()

                conn.execute("BEGIN IMMEDIATE")
                pending = conn.execute(
                    "SELECT user_id FROM work_daily_refresh_reset_targets "
                    "WHERE business_date=%s AND status='pending' ORDER BY user_id LIMIT %s",
                    (business_date, chunk_size),
                ).fetchall()
                changed = 0
                for pending_row in pending:
                    user_id = str(pending_row[0])
                    user = conn.execute(
                        "SELECT COUNT(*),MIN(COALESCE(work_num,0)),"
                        "MAX(COALESCE(work_num,0)) FROM user_xiuxian WHERE user_id=%s",
                        (user_id,),
                    ).fetchone()
                    row_count = int(user[0] or 0) if user is not None else 0
                    if row_count == 0:
                        conn.execute(
                            "UPDATE work_daily_refresh_reset_targets SET status='skipped',"
                            "updated_at=%s WHERE business_date=%s AND user_id=%s AND status='pending'",
                            (updated_at, business_date, user_id),
                        )
                        continue
                    previous_count = int(user[1] or 0)
                    previous_max = int(user[2] or 0)
                    updated = conn.execute(
                        "UPDATE user_xiuxian SET work_num=%s WHERE user_id=%s",
                        (reset_count, user_id),
                    )
                    if updated.rowcount != row_count:
                        raise db_backend.IntegrityError("work refresh reset target changed")
                    changed += int(
                        previous_count != reset_count or previous_max != reset_count
                    )
                    conn.execute(
                        "UPDATE work_daily_refresh_reset_targets SET status='applied',"
                        "previous_count=%s,final_count=%s,updated_at=%s "
                        "WHERE business_date=%s AND user_id=%s AND status='pending'",
                        (
                            previous_count,
                            reset_count,
                            updated_at,
                            business_date,
                            user_id,
                        ),
                    )

                progress = conn.execute(
                    "SELECT COUNT(*),COALESCE(SUM(CASE WHEN status='pending' THEN 1 ELSE 0 END),0) "
                    "FROM work_daily_refresh_reset_targets WHERE business_date=%s",
                    (business_date,),
                ).fetchone()
                completed = int(progress[0]) - int(progress[1])
                task_status = "completed" if int(progress[1]) == 0 else "running"
                conn.execute(
                    "UPDATE work_daily_refresh_reset_operations SET completed=%s,changed=changed+%s,"
                    "status=%s,updated_at=%s WHERE business_date=%s",
                    (completed, changed, task_status, updated_at, business_date),
                )
                result = self._result(conn, business_date, "applied")
                conn.commit()
                return result
            except Exception:
                conn.rollback()
                raise

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
