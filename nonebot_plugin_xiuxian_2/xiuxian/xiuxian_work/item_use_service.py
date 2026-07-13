from __future__ import annotations

import json
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from threading import RLock

from ..xiuxian_utils import db_backend


@dataclass(frozen=True)
class WorkItemUseResult:
    status: str
    action: str | None = None
    item_remaining: int = 0
    result_snapshot: dict | None = None

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


class WorkItemUseService:
    """Consume work items together with the work-state transition they cause."""

    def __init__(self, database: str | Path, lock: RLock | None = None) -> None:
        self._database = Path(database)
        self._lock = lock or RLock()

    def accelerate(
        self, operation_id, user_id, item_id, expected_item_count, expected_work, accelerated_at
    ) -> WorkItemUseResult:
        return self._apply(
            operation_id, user_id, item_id, expected_item_count, "accelerate",
            dict(expected_work), {"accelerated_at": str(accelerated_at)},
        )

    def capture(
        self, operation_id, user_id, item_id, expected_item_count, expected_work_type, new_offer
    ) -> WorkItemUseResult:
        return self._apply(
            operation_id, user_id, item_id, expected_item_count, "capture",
            {"type": int(expected_work_type)}, {"offer": dict(new_offer)},
        )

    def _apply(self, operation_id, user_id, item_id, expected_item_count, action, expected, result):
        operation_id = str(operation_id).strip()
        user_id, item_id = str(user_id), int(item_id)
        expected_item_count = int(expected_item_count)
        if not operation_id or expected_item_count <= 0 or action not in {"accelerate", "capture"}:
            raise ValueError("valid operation, item snapshot and action are required")
        payload = json.dumps(
            [user_id, item_id, expected_item_count, action, expected, result],
            ensure_ascii=True, sort_keys=True, separators=(",", ":"), default=str,
        )
        result_json = json.dumps(result, ensure_ascii=True, sort_keys=True, separators=(",", ":"))

        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS work_item_use_operations ("
                    "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,action TEXT NOT NULL,"
                    "item_remaining INTEGER NOT NULL,result_snapshot TEXT NOT NULL,"
                    "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
                )
                previous = conn.execute(
                    "SELECT payload,action,item_remaining,result_snapshot FROM work_item_use_operations "
                    "WHERE operation_id=%s", (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    if str(previous[0]) != payload:
                        return WorkItemUseResult("operation_conflict")
                    return WorkItemUseResult(
                        "duplicate", str(previous[1]), int(previous[2]), json.loads(str(previous[3]))
                    )

                item = conn.execute(
                    "SELECT COALESCE(goods_num,0),COALESCE(bind_num,0) FROM back "
                    "WHERE user_id=%s AND goods_id=%s", (user_id, item_id),
                ).fetchone()
                work = conn.execute(
                    "SELECT COALESCE(type,0),create_time,scheduled_time FROM user_cd WHERE user_id=%s",
                    (user_id,),
                ).fetchone()
                if work is None:
                    conn.rollback()
                    return WorkItemUseResult("user_missing")
                if item is None or int(item[0]) < 1:
                    conn.rollback()
                    return WorkItemUseResult("item_missing")
                if int(item[0]) != expected_item_count:
                    conn.rollback()
                    return WorkItemUseResult("state_changed")

                if action == "accelerate":
                    actual_work = {
                        "type": int(work[0]), "create_time": str(work[1]),
                        "scheduled_time": str(work[2]),
                    }
                    normalized_expected = {
                        "type": int(expected.get("type", 0)),
                        "create_time": str(expected.get("create_time")),
                        "scheduled_time": str(expected.get("scheduled_time")),
                    }
                    if actual_work != normalized_expected or actual_work["type"] != 2:
                        conn.rollback()
                        return WorkItemUseResult("state_changed")
                    conn.execute(
                        "UPDATE user_cd SET create_time=%s WHERE user_id=%s AND type=2",
                        (result["accelerated_at"], user_id),
                    )
                else:
                    if int(work[0]) != int(expected["type"]) or int(work[0]) != 0:
                        conn.rollback()
                        return WorkItemUseResult("state_changed")
                    conn.execute(
                        "CREATE TABLE IF NOT EXISTS work_offer_snapshots ("
                        "user_id TEXT PRIMARY KEY,snapshot TEXT NOT NULL,updated_at TEXT NOT NULL)"
                    )
                    offer = dict(result["offer"])
                    conn.execute(
                        "INSERT INTO work_offer_snapshots(user_id,snapshot,updated_at) VALUES(%s,%s,%s) "
                        "ON CONFLICT(user_id) DO UPDATE SET snapshot=EXCLUDED.snapshot,updated_at=EXCLUDED.updated_at",
                        (user_id, json.dumps(offer, ensure_ascii=True, sort_keys=True), str(offer.get("refresh_time", ""))),
                    )

                remaining = expected_item_count - 1
                bind_remaining = min(max(0, int(item[1]) - 1), remaining)
                conn.execute(
                    "UPDATE back SET goods_num=%s,bind_num=%s WHERE user_id=%s AND goods_id=%s AND goods_num=%s",
                    (remaining, bind_remaining, user_id, item_id, expected_item_count),
                )
                conn.execute(
                    "INSERT INTO work_item_use_operations(operation_id,payload,action,item_remaining,result_snapshot) "
                    "VALUES(%s,%s,%s,%s,%s)",
                    (operation_id, payload, action, remaining, result_json),
                )
                conn.commit()
                return WorkItemUseResult("applied", action, remaining, result)
            except Exception:
                conn.rollback()
                raise


__all__ = ["WorkItemUseResult", "WorkItemUseService"]
