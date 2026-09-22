from __future__ import annotations

import json
from pathlib import Path

from ...infrastructure.database import DatabaseUnitOfWork


class RiftSpeedupResult:
    def __init__(self, status: str, new_time: int = 0, rift_data: dict | None = None, create_time: str | None = None) -> None:
        self.status, self.new_time, self.rift_data, self.create_time = status, new_time, rift_data or {}, create_time


class RiftSpeedupSqlRepository:
    def __init__(self, database: str | Path) -> None:
        self.database = str(database)

    def apply(self, operation_id: str, user_id: str, item_id: int, expected_rift: dict | None, expected_cd: dict | None, remaining_ratio: int) -> RiftSpeedupResult:
        if not operation_id or not user_id or int(item_id) <= 0 or not 0 < int(remaining_ratio) < 100:
            raise ValueError("valid operation, user, item and remaining ratio are required")
        expected_rift = dict(expected_rift or {})
        expected_cd = dict(expected_cd or {})
        payload = json.dumps([user_id, int(item_id), expected_rift, expected_cd, int(remaining_ratio)], ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            uow.execute("CREATE TABLE IF NOT EXISTS rift_speedup_operations(operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,new_time INTEGER NOT NULL,rift_data TEXT NOT NULL,create_time TEXT,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)")
            previous = uow.query_one("SELECT payload,new_time,rift_data,create_time FROM rift_speedup_operations WHERE operation_id=?", (operation_id,))
            if previous is not None:
                if str(previous["payload"]) != payload:
                    return RiftSpeedupResult("state_changed")
                return RiftSpeedupResult("duplicate", int(previous["new_time"]), json.loads(str(previous["rift_data"])), previous["create_time"])
            entry = uow.query_one("SELECT rift_data,status,duration FROM rift_entries WHERE user_id=?", (user_id,))
            cd = uow.query_one("SELECT type,create_time,scheduled_time FROM user_cd WHERE user_id=?", (user_id,))
            if entry is None or str(entry["status"]) != "active" or cd is None or int(cd["type"]) != 3:
                return RiftSpeedupResult("not_active")
            current = json.loads(str(entry["rift_data"]))
            current_cd = {"type": int(cd["type"]), "create_time": cd["create_time"], "scheduled_time": cd["scheduled_time"]}
            if expected_rift and (current != expected_rift or current_cd != expected_cd or int(entry["duration"]) != int(current.get("time", 0))):
                return RiftSpeedupResult("state_changed", int(entry["duration"]), current, cd["create_time"])
            if int(entry["duration"]) <= 10:
                return RiftSpeedupResult("not_needed", int(entry["duration"]), current, cd["create_time"])
            new_time = max(1, int(entry["duration"]) * int(remaining_ratio) // 100)
            updated = dict(current)
            updated["time"] = new_time
            consumed = uow.execute("UPDATE back SET goods_num=goods_num-1,bind_num=MAX(COALESCE(bind_num,0)-1,0) WHERE user_id=? AND goods_id=? AND COALESCE(goods_num,0)>=1", (user_id, int(item_id)))
            if consumed.rowcount != 1:
                return RiftSpeedupResult("item_missing", int(entry["duration"]), current, cd["create_time"])
            snapshot = json.dumps(updated, ensure_ascii=False, sort_keys=True)
            if uow.execute("UPDATE rift_entries SET rift_data=?,duration=? WHERE user_id=? AND status='active'", (snapshot, new_time, user_id)).rowcount != 1:
                return RiftSpeedupResult("state_changed", int(entry["duration"]), current, cd["create_time"])
            if uow.execute("UPDATE user_cd SET scheduled_time=? WHERE user_id=? AND type=3", (new_time, user_id)).rowcount != 1:
                return RiftSpeedupResult("state_changed", int(entry["duration"]), current, cd["create_time"])
            uow.execute("INSERT INTO rift_speedup_operations(operation_id,payload,new_time,rift_data,create_time) VALUES(?,?,?,?,?)", (operation_id, payload, new_time, snapshot, cd["create_time"]))
            return RiftSpeedupResult("applied", new_time, updated, cd["create_time"])


__all__ = ["RiftSpeedupSqlRepository", "RiftSpeedupResult"]
