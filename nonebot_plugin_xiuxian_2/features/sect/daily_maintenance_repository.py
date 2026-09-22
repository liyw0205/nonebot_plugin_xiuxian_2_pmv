from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

from ...infrastructure.database import DatabaseUnitOfWork


@dataclass(frozen=True)
class SectMaintenanceOutcome:
    sect_id: int
    sect_name: str
    status: str
    from_level: int
    to_level: int
    materials_cost: int


@dataclass(frozen=True)
class SectDailyResetResult:
    status: str
    business_date: str
    user_count: int = 0
    outcomes: tuple[SectMaintenanceOutcome, ...] = ()


class SectDailyMaintenanceSqlRepository:
    def __init__(self, database: str | Path) -> None:
        self.database = str(database)

    def settle(self, business_date: str, costs_by_level: dict[int, int]) -> SectDailyResetResult:
        day = str(business_date).strip()
        costs = {int(level): max(int(cost), 0) for level, cost in dict(costs_by_level).items()}
        if not day or not costs or any(level <= 0 for level in costs):
            raise ValueError("business date and positive room levels are required")
        payload = json.dumps(costs, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            uow.execute("CREATE TABLE IF NOT EXISTS sect_daily_reset_operations(business_date TEXT PRIMARY KEY,payload TEXT NOT NULL,user_count INTEGER NOT NULL,outcomes TEXT NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)")
            previous = uow.query_one("SELECT payload,user_count,outcomes FROM sect_daily_reset_operations WHERE business_date=?", (day,))
            if previous is not None:
                if str(previous["payload"]) != payload:
                    return SectDailyResetResult("operation_conflict", day)
                return SectDailyResetResult("duplicate", day, int(previous["user_count"]), tuple(SectMaintenanceOutcome(**row) for row in json.loads(str(previous["outcomes"]))))
            user_count = int(uow.query_one("SELECT COUNT(*) AS count FROM user_xiuxian")["count"])
            uow.execute("UPDATE user_xiuxian SET sect_task=0,sect_elixir_get=0")
            rows = uow.query_all("SELECT sect_id,COALESCE(sect_name,'') AS sect_name,sect_owner,COALESCE(elixir_room_level,0) AS level,COALESCE(sect_materials,0) AS materials FROM sects ORDER BY sect_id")
            outcomes = []
            for row in rows:
                sid, name, owner, level, materials = int(row["sect_id"]), str(row["sect_name"]), row["sect_owner"], int(row["level"]), int(row["materials"])
                cost = costs.get(level, 0)
                target = level
                if owner is None: status = "inactive"
                elif level <= 0: status = "no_room"
                elif level not in costs: status = "level_unsupported"
                elif materials >= cost:
                    status = "charged"
                    uow.execute("UPDATE sects SET sect_materials=sect_materials-? WHERE sect_id=? AND elixir_room_level=?", (cost, sid, level))
                else:
                    target = max(level - 1, 0)
                    status = "disabled" if target == 0 else "downgraded"
                    uow.execute("UPDATE sects SET elixir_room_level=? WHERE sect_id=? AND elixir_room_level=?", (target, sid, level))
                outcomes.append(SectMaintenanceOutcome(sid, name, status, level, target, cost))
            encoded = json.dumps([row.__dict__ for row in outcomes], ensure_ascii=True, sort_keys=True)
            uow.execute("INSERT INTO sect_daily_reset_operations(business_date,payload,user_count,outcomes) VALUES(?,?,?,?)", (day, payload, user_count, encoded))
            return SectDailyResetResult("applied", day, user_count, tuple(outcomes))


__all__ = ["SectDailyMaintenanceSqlRepository", "SectDailyResetResult", "SectMaintenanceOutcome"]
