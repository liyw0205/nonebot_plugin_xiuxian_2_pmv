from __future__ import annotations

import json
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from threading import RLock

from ..xiuxian_utils import db_backend


@dataclass(frozen=True)
class RiftSpeedupResult:
    status: str
    new_time: int = 0
    rift_data: dict | None = None
    create_time: str | None = None

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


class RiftSpeedupService:
    """Atomically consume a speedup item and shorten an active rift."""

    def __init__(self, database: str | Path, lock: RLock | None = None) -> None:
        self._database = Path(database)
        self._lock = lock or RLock()

    def apply(
        self,
        operation_id,
        user_id,
        item_id,
        expected_rift=None,
        expected_cd=None,
        remaining_ratio=None,
    ) -> RiftSpeedupResult:
        if isinstance(expected_rift, (int, float)) and isinstance(expected_cd, (int, float)) and remaining_ratio is None:
            return self._apply_legacy(operation_id, user_id, item_id, int(expected_rift), int(expected_cd))
        operation_id = str(operation_id).strip()
        user_id = str(user_id)
        item_id = int(item_id)
        if remaining_ratio is None:
            raise ValueError("remaining ratio is required")
        remaining_ratio = int(remaining_ratio)
        if not operation_id or not user_id or item_id <= 0 or not 0 < remaining_ratio < 100:
            raise ValueError("valid operation, user, item and remaining ratio are required")

        expected_time = 0
        expected_snapshot = None
        if expected_rift is None:
            payload = json.dumps([user_id, item_id, remaining_ratio], ensure_ascii=True)
        else:
            expected_rift = dict(expected_rift)
            expected_time = int(expected_rift.get("time", 0))
            if expected_time <= 0 or expected_cd is None:
                raise ValueError("active rift duration and cooldown state are required")
            expected_cd = {
                "type": int(expected_cd.get("type", 0)),
                "create_time": expected_cd.get("create_time"),
                "scheduled_time": expected_cd.get("scheduled_time"),
            }
            expected_snapshot = json.dumps(expected_rift, ensure_ascii=False, sort_keys=True)
            payload = json.dumps(
                [user_id, item_id, expected_snapshot, expected_cd, remaining_ratio],
                ensure_ascii=True,
                sort_keys=True,
            )

        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS rift_speedup_operations("
                    "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,new_time INTEGER NOT NULL,"
                    "rift_data TEXT NOT NULL,create_time TEXT,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
                )
                if not conn.column_exists("rift_speedup_operations", "rift_data"):
                    conn.execute("ALTER TABLE rift_speedup_operations ADD COLUMN rift_data TEXT NOT NULL DEFAULT '{}'")
                if not conn.column_exists("rift_speedup_operations", "create_time"):
                    conn.execute("ALTER TABLE rift_speedup_operations ADD COLUMN create_time TEXT")
                old = conn.execute(
                    "SELECT payload,new_time,rift_data,create_time FROM rift_speedup_operations WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if old is not None:
                    conn.rollback()
                    if str(old[0]) != payload:
                        return RiftSpeedupResult("state_changed", expected_time, expected_rift)
                    return RiftSpeedupResult("duplicate", int(old[1]), json.loads(str(old[2])), old[3])

                entry = conn.execute(
                    "SELECT rift_data,status,duration FROM rift_entries WHERE user_id=%s", (user_id,)
                ).fetchone()
                cd = conn.execute(
                    "SELECT type,create_time,scheduled_time FROM user_cd WHERE user_id=%s", (user_id,)
                ).fetchone()
                if entry is None or str(entry[1]) != "active" or cd is None or int(cd[0]) != 3:
                    conn.rollback()
                    return RiftSpeedupResult("not_active", expected_time, expected_rift)
                current_rift = json.loads(str(entry[0]))
                current_time = int(entry[2])
                current_cd = {
                    "type": int(cd[0]),
                    "create_time": cd[1],
                    "scheduled_time": cd[2],
                }
                if (
                    int(current_rift.get("time", 0)) != current_time
                    or (
                        expected_snapshot is not None
                        and (
                            current_rift != json.loads(expected_snapshot)
                            or current_time != expected_time
                            or current_cd != expected_cd
                        )
                    )
                ):
                    conn.rollback()
                    return RiftSpeedupResult("state_changed", expected_time, expected_rift)
                if current_time <= 10:
                    conn.rollback()
                    return RiftSpeedupResult("not_needed", current_time, current_rift, current_cd["create_time"])

                new_time = max(1, current_time * remaining_ratio // 100)
                updated_rift = dict(current_rift)
                updated_rift["time"] = new_time
                updated_snapshot = json.dumps(updated_rift, ensure_ascii=False, sort_keys=True)
                bind_update = ""
                if conn.column_exists("back", "bind_num"):
                    bind_update = (
                        ",bind_num=MIN("
                        "MAX(COALESCE(bind_num,0)-1,0),goods_num-1)"
                    )
                consumed = conn.execute(
                    "UPDATE back SET goods_num=goods_num-1" + bind_update + " "
                    "WHERE user_id=%s AND goods_id=%s AND COALESCE(goods_num,0)>=1",
                    (user_id, item_id),
                )
                if consumed.rowcount != 1:
                    conn.rollback()
                    return RiftSpeedupResult("item_missing", current_time, current_rift, current_cd["create_time"])
                entry_updated = conn.execute(
                    "UPDATE rift_entries SET rift_data=%s,duration=%s WHERE user_id=%s AND status='active'",
                    (updated_snapshot, new_time, user_id),
                )
                cd_updated = conn.execute(
                    "UPDATE user_cd SET scheduled_time=%s WHERE user_id=%s AND type=3",
                    (new_time, user_id),
                )
                if entry_updated.rowcount != 1 or cd_updated.rowcount != 1:
                    conn.rollback()
                    return RiftSpeedupResult("state_changed", current_time, current_rift, current_cd["create_time"])
                conn.execute(
                    "INSERT INTO rift_speedup_operations(operation_id,payload,new_time,rift_data,create_time) "
                    "VALUES(%s,%s,%s,%s,%s)",
                    (operation_id, payload, new_time, updated_snapshot, current_cd["create_time"]),
                )
                conn.commit()
                return RiftSpeedupResult("applied", new_time, updated_rift, current_cd["create_time"])
            except Exception:
                conn.rollback()
                raise

    def _apply_legacy(self, operation_id, user_id, item_id, expected_time, new_time) -> RiftSpeedupResult:
        """Keep the pre-transaction-service API usable for old callers and tests."""
        operation_id, user_id, item_id = str(operation_id), str(user_id), int(item_id)
        payload = json.dumps([user_id, item_id, expected_time, new_time])
        if not operation_id or not 0 < new_time < expected_time:
            raise ValueError("invalid speedup")
        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS rift_speedup_operations("
                    "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,new_time INTEGER NOT NULL,"
                    "rift_data TEXT NOT NULL DEFAULT '{}',created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
                )
                old = conn.execute(
                    "SELECT payload,new_time FROM rift_speedup_operations WHERE operation_id=%s", (operation_id,)
                ).fetchone()
                if old is not None:
                    conn.rollback()
                    return RiftSpeedupResult(
                        "duplicate" if str(old[0]) == payload else "state_changed",
                        int(old[1]) if str(old[0]) == payload else expected_time,
                    )
                row = conn.execute(
                    "SELECT duration FROM rift_entries WHERE user_id=%s AND status='active'", (user_id,)
                ).fetchone()
                item = conn.execute(
                    "SELECT goods_num FROM back WHERE user_id=%s AND goods_id=%s", (user_id, item_id)
                ).fetchone()
                if row is None or int(row[0]) != expected_time:
                    conn.rollback()
                    return RiftSpeedupResult("state_changed", expected_time)
                if item is None or int(item[0]) < 1:
                    conn.rollback()
                    return RiftSpeedupResult("item_missing", expected_time)
                bind_update = ""
                if conn.column_exists("back", "bind_num"):
                    bind_update = (
                        ",bind_num=MIN("
                        "MAX(COALESCE(bind_num,0)-1,0),goods_num-1)"
                    )
                conn.execute(
                    "UPDATE back SET goods_num=goods_num-1" + bind_update + " "
                    "WHERE user_id=%s AND goods_id=%s",
                    (user_id, item_id),
                )
                conn.execute("UPDATE rift_entries SET duration=%s WHERE user_id=%s", (new_time, user_id))
                conn.execute("UPDATE user_cd SET scheduled_time=%s WHERE user_id=%s", (new_time, user_id))
                conn.execute(
                    "INSERT INTO rift_speedup_operations(operation_id,payload,new_time) VALUES(%s,%s,%s)",
                    (operation_id, payload, new_time),
                )
                conn.commit()
                return RiftSpeedupResult("applied", new_time)
            except Exception:
                conn.rollback()
                raise


__all__ = ["RiftSpeedupResult", "RiftSpeedupService"]
