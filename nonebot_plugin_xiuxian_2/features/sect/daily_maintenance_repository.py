from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

from ...core.errors import DomainError
from ...core.numeric import require_exact_integer
from ...infrastructure.database import DatabaseUnitOfWork
from .schema import DAILY_RESET_COLUMNS, SCHEMA_MISSING, table_ready


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
    reason: str = ""
    subject: str = ""

    @property
    def applied(self) -> bool:
        return self.status in {"applied", "duplicate"}


class SectDailyMaintenanceSqlRepository:
    def __init__(self, database: str | Path) -> None:
        self.database = str(database)

    def settle(self, business_date: str, costs_by_level: dict[int, int]) -> SectDailyResetResult:
        day = str(business_date).strip()
        costs = {int(level): max(int(cost), 0) for level, cost in dict(costs_by_level).items()}
        if not day or not costs or any(level <= 0 for level in costs):
            raise ValueError("business date and positive room levels are required")
        payload = json.dumps(costs, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
        try:
            return self._settle(day, costs, payload)
        except DomainError as exc:
            return SectDailyResetResult("state_changed", day, reason=exc.details.get("reason") or exc.code,
                                        subject=str(exc.details.get("subject") or day))

    @staticmethod
    def _reject(step: str, subject: str | int) -> None:
        reason = f"sect_daily_reset_{step}_write_rejected"
        raise DomainError(reason, f"{reason}: {subject}", {"reason": reason, "subject": str(subject)})

    @staticmethod
    def _number(value, field, subject):
        return require_exact_integer(value, field=field, subject=str(subject))

    def _replay(self, previous, day, payload):
        try:
            old_costs = json.loads(str(previous["payload"]))
            if not isinstance(old_costs, dict):
                raise ValueError("costs must be an object")
            if old_costs != json.loads(payload):
                return SectDailyResetResult("operation_conflict", day, reason="operation_conflict", subject=day)
            raw = json.loads(str(previous["outcomes"]))
            if not isinstance(raw, list):
                raise ValueError("outcomes must be a list")
            outcomes = tuple(SectMaintenanceOutcome(**row) for row in raw)
            count = self._number(previous["user_count"], "sect_daily_reset_user_count", day)
        except (ValueError, TypeError, KeyError) as exc:
            raise DomainError("sect_daily_reset_receipt_invalid", "invalid daily reset receipt",
                              {"subject": day}) from exc
        return SectDailyResetResult("duplicate", day, count, outcomes)

    def _check_users(self, uow, user_ids, day):
        saved = uow.query_all("SELECT user_id,sect_task,sect_elixir_get FROM user_xiuxian ORDER BY user_id")
        for row in saved:
            uid = str(row["user_id"])
            if uid not in user_ids or row["sect_task"] != 0 or row["sect_elixir_get"] != 0:
                self._reject("user", uid)
        if {str(row["user_id"]) for row in saved} != user_ids:
            self._reject("user", day)

    def _check_sects(self, uow, expected, day):
        saved = uow.query_all("SELECT sect_id,COALESCE(elixir_room_level,0) AS level,"
                              "COALESCE(sect_materials,0) AS materials FROM sects ORDER BY sect_id")
        if {row["sect_id"] for row in saved} != set(expected):
            self._reject("level", day)
        for row in saved:
            sid = row["sect_id"]
            level, materials, numeric_materials = expected[sid]
            if self._number(row["level"], "elixir_room_level", sid) != level:
                self._reject("level", sid)
            actual = self._number(row["materials"], "sect_materials", sid) if numeric_materials else row["materials"]
            if actual != materials:
                self._reject("material", sid)

    def _settle(self, day, costs, payload):
        if not Path(self.database).is_file():
            return SectDailyResetResult(SCHEMA_MISSING, day, reason=SCHEMA_MISSING, subject=day)
        with DatabaseUnitOfWork(self.database, immediate=True, require_exists=True) as uow:
            if not table_ready(uow, "sect_daily_reset_operations", DAILY_RESET_COLUMNS):
                return SectDailyResetResult(SCHEMA_MISSING, day, reason=SCHEMA_MISSING, subject=day)
            previous = uow.query_one("SELECT payload,user_count,outcomes FROM sect_daily_reset_operations WHERE business_date=?", (day,))
            if previous is not None:
                return self._replay(previous, day, payload)
            user_ids = {str(row["user_id"]) for row in uow.query_all("SELECT user_id FROM user_xiuxian")}
            rows = uow.query_all("SELECT sect_id,COALESCE(sect_name,'') AS sect_name,sect_owner,COALESCE(elixir_room_level,0) AS level,COALESCE(sect_materials,0) AS materials FROM sects ORDER BY sect_id")
            reset = uow.execute("UPDATE user_xiuxian SET sect_task=0,sect_elixir_get=0")
            self._check_users(uow, user_ids, day)
            if reset.rowcount != len(user_ids):
                self._reject("user", day)
            outcomes = []
            expected = {}
            for row in rows:
                sid, name, owner = int(row["sect_id"]), str(row["sect_name"]), row["sect_owner"]
                level = self._number(row["level"], "elixir_room_level", sid)
                materials = row["materials"]
                cost = costs.get(level, 0)
                target = level
                if owner is None: status = "inactive"
                elif level <= 0: status = "no_room"
                elif level not in costs: status = "level_unsupported"
                else:
                    materials = self._number(materials, "sect_materials", sid)
                    if materials >= cost:
                        status = "charged"
                        materials -= cost
                        changed = uow.execute("UPDATE sects SET sect_materials=xiuxian_int_store(?) "
                                              "WHERE sect_id=? AND elixir_room_level=?", (str(materials), sid, level))
                        if changed.rowcount != 1:
                            self._reject("material", sid)
                    else:
                        target = max(level - 1, 0)
                        status = "disabled" if target == 0 else "downgraded"
                        changed = uow.execute("UPDATE sects SET elixir_room_level=? WHERE sect_id=? AND elixir_room_level=?", (target, sid, level))
                        if changed.rowcount != 1:
                            self._reject("level", sid)
                expected[sid] = (target, materials, status in {"charged", "downgraded", "disabled"})
                outcomes.append(SectMaintenanceOutcome(sid, name, status, level, target, cost))
            encoded = json.dumps([row.__dict__ for row in outcomes], ensure_ascii=True, sort_keys=True)
            recorded = uow.execute("INSERT INTO sect_daily_reset_operations(business_date,payload,user_count,outcomes) VALUES(?,?,?,?)", (day, payload, len(user_ids), encoded))
            if recorded.rowcount != 1:
                self._reject("receipt", day)
            self._check_users(uow, user_ids, day)
            self._check_sects(uow, expected, day)
            receipt = uow.query_one("SELECT payload,user_count,outcomes FROM sect_daily_reset_operations WHERE business_date=?", (day,))
            if receipt != {"payload": payload, "user_count": len(user_ids), "outcomes": encoded}:
                self._reject("receipt", day)
            return SectDailyResetResult("applied", day, len(user_ids), tuple(outcomes))


__all__ = ["SectDailyMaintenanceSqlRepository", "SectDailyResetResult", "SectMaintenanceOutcome"]
