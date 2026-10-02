from __future__ import annotations

import json
from datetime import datetime, timedelta
from pathlib import Path

from ...infrastructure.clock import SystemClock
from ...infrastructure.database import DatabaseUnitOfWork


class NormalTrainingStartResult:
    def __init__(self, status: str, kind: str = "", create_time: str = "", scheduled_time: str = "") -> None:
        self.status, self.kind, self.create_time, self.scheduled_time = status, kind, create_time, scheduled_time


class NormalTrainingStartSqlRepository:
    def __init__(self, database: str | Path) -> None:
        self.database = str(database)

    def start(self, operation_id: str, user_id: str, kind: str, expected_exp: int, expected_stone: int, reward: int, exp_cap: int, power_multiplier: float, duration_seconds: int = 60, now: datetime | None = None) -> NormalTrainingStartResult:
        if not Path(self.database).is_file():
            return NormalTrainingStartResult("schema_missing")
        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            return self.start_in_uow(
                uow, operation_id, user_id, kind, expected_exp, expected_stone,
                reward, exp_cap, power_multiplier, duration_seconds, now,
            )

    def start_in_uow(self, uow: DatabaseUnitOfWork, operation_id: str, user_id: str, kind: str, expected_exp: int, expected_stone: int, reward: int, exp_cap: int, power_multiplier: float, duration_seconds: int = 60, now: datetime | None = None) -> NormalTrainingStartResult:
        if not operation_id or kind not in {"cultivation", "mining"} or min(int(expected_exp), int(expected_stone), int(reward), int(exp_cap), int(duration_seconds)) < 0:
            raise ValueError("invalid normal training lifecycle arguments")
        current = now or SystemClock().now()
        if current.tzinfo is not None:
            current = current.astimezone().replace(tzinfo=None)
        create_time = current.strftime("%Y-%m-%d %H:%M:%S.%f")
        scheduled_time = (current + timedelta(seconds=int(duration_seconds))).strftime("%Y-%m-%d %H:%M:%S.%f")
        payload = json.dumps([str(user_id), kind, int(expected_exp), int(expected_stone), int(reward), int(exp_cap), float(power_multiplier), int(duration_seconds)], separators=(",", ":"))
        required = {
            "normal_training_operations": {"operation_id", "user_id", "payload", "kind", "create_time", "scheduled_time", "status", "result_json"},
            "user_xiuxian": {"user_id", "exp", "stone"},
            "user_cd": {"user_id", "type", "create_time", "scheduled_time"},
        }
        for table, columns in required.items():
            actual = {str(row["name"]) for row in uow.query_all(f"PRAGMA table_info({table})")}
            if not columns <= actual:
                return NormalTrainingStartResult("schema_missing")
        previous = uow.query_one("SELECT payload,kind,create_time,scheduled_time,status FROM normal_training_operations WHERE operation_id=?", (operation_id,))
        if previous is not None:
            if str(previous["payload"]) != payload:
                return NormalTrainingStartResult("operation_conflict")
            return NormalTrainingStartResult("duplicate", str(previous["kind"]), str(previous["create_time"]), str(previous["scheduled_time"]))
        user = uow.query_one("SELECT COALESCE(exp,0) AS exp,COALESCE(stone,0) AS stone FROM user_xiuxian WHERE user_id=?", (user_id,))
        cd = uow.query_one("SELECT COALESCE(type,0) AS type FROM user_cd WHERE user_id=?", (user_id,))
        if user is None or cd is None:
            return NormalTrainingStartResult("user_missing")
        if int(cd["type"]) != 0 or (int(user["exp"]), int(user["stone"])) != (int(expected_exp), int(expected_stone)):
            return NormalTrainingStartResult("state_changed")
        if uow.execute("UPDATE user_cd SET type=5,create_time=?,scheduled_time=? WHERE user_id=? AND COALESCE(type,0)=0", (create_time, scheduled_time, user_id)).rowcount != 1:
            return NormalTrainingStartResult("state_changed")
        uow.execute("INSERT INTO normal_training_operations(operation_id,user_id,payload,kind,create_time,scheduled_time,status) VALUES(?,?,?,?,?,?,?)", (operation_id, user_id, payload, kind, create_time, scheduled_time, "pending"))
        return NormalTrainingStartResult("started", kind, create_time, scheduled_time)


__all__ = ["NormalTrainingStartSqlRepository", "NormalTrainingStartResult"]
