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
        expected_snapshot = None
        if expected_rift is None:
            expected_rift = {}
            expected_cd = {}
            payload = json.dumps([user_id, int(item_id), int(remaining_ratio)], ensure_ascii=True)
        else:
            expected_rift = dict(expected_rift)
            if int(expected_rift.get("time", 0)) <= 0 or expected_cd is None:
                raise ValueError("active rift duration and cooldown state are required")
            expected_cd = {
                "type": int(expected_cd.get("type", 0)),
                "create_time": expected_cd.get("create_time"),
                "scheduled_time": expected_cd.get("scheduled_time"),
            }
            expected_snapshot = json.dumps(expected_rift, ensure_ascii=False, sort_keys=True)
            payload = json.dumps(
                [user_id, int(item_id), expected_snapshot, expected_cd, int(remaining_ratio)],
                ensure_ascii=True,
                sort_keys=True,
            )
        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            if uow.query_one(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name='rift_speedup_operations'"
            ) is None:
                return RiftSpeedupResult("schema_missing")
            previous = uow.query_one(
                "SELECT payload,new_time,rift_data,create_time FROM rift_speedup_operations WHERE operation_id=?",
                (operation_id,),
            )
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
            current_duration = int(entry["duration"])
            if current_duration != int(current.get("time", 0)) or (
                expected_snapshot is not None
                and (current != expected_rift or current_cd != expected_cd)
            ):
                return RiftSpeedupResult("state_changed", int(entry["duration"]), current, cd["create_time"])
            if int(entry["duration"]) <= 10:
                return RiftSpeedupResult("not_needed", int(entry["duration"]), current, cd["create_time"])
            new_time = max(1, int(entry["duration"]) * int(remaining_ratio) // 100)
            updated = dict(current)
            updated["time"] = new_time
            back_columns = {
                str(row["name"]) for row in uow.query_all("PRAGMA table_info(back)")
            }
            bind_update = ""
            if "bind_num" in back_columns:
                bind_update = ",bind_num=MIN(MAX(COALESCE(bind_num,0)-1,0),goods_num-1)"
            consumed = uow.execute(
                "UPDATE back SET goods_num=goods_num-1" + bind_update
                + " WHERE user_id=? AND goods_id=? AND COALESCE(goods_num,0)>=1",
                (user_id, int(item_id)),
            )
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
