from __future__ import annotations

import json
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from threading import RLock

from ..xiuxian_utils import db_backend


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

    @property
    def applied(self) -> bool:
        return self.status in {"applied", "duplicate"}


class SectDailyResetMaintenanceService:
    """Reset daily sect counters and settle all elixir-room maintenance."""

    def __init__(self, database: str | Path, lock: RLock | None = None) -> None:
        self._database = Path(database)
        self._lock = lock or RLock()

    @staticmethod
    def _ensure_operations(conn) -> None:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS sect_daily_reset_operations (
                business_date TEXT PRIMARY KEY,
                payload TEXT NOT NULL,
                user_count INTEGER NOT NULL,
                outcomes TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

    @staticmethod
    def _normalize_costs(costs_by_level) -> dict[int, int]:
        costs = {
            int(level): max(int(cost), 0)
            for level, cost in dict(costs_by_level).items()
        }
        if not costs or any(level <= 0 for level in costs):
            raise ValueError("positive elixir-room levels are required")
        return costs

    @staticmethod
    def _decode_outcomes(raw: str) -> tuple[SectMaintenanceOutcome, ...]:
        return tuple(SectMaintenanceOutcome(**item) for item in json.loads(raw))

    def settle(self, business_date, costs_by_level) -> SectDailyResetResult:
        business_date = str(business_date).strip()
        if not business_date:
            raise ValueError("business_date must not be empty")
        costs = self._normalize_costs(costs_by_level)
        payload = json.dumps(costs, ensure_ascii=True, sort_keys=True)

        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_operations(conn)
                previous = conn.execute(
                    "SELECT payload, user_count, outcomes "
                    "FROM sect_daily_reset_operations WHERE business_date=%s",
                    (business_date,),
                ).fetchone()
                if previous:
                    if str(previous[0]) != payload:
                        conn.rollback()
                        return SectDailyResetResult("operation_conflict", business_date)
                    conn.rollback()
                    return SectDailyResetResult(
                        "duplicate",
                        business_date,
                        int(previous[1]),
                        self._decode_outcomes(str(previous[2])),
                    )

                user_count = int(
                    conn.execute("SELECT COUNT(*) FROM user_xiuxian").fetchone()[0]
                )
                conn.execute(
                    "UPDATE user_xiuxian SET sect_task=0, sect_elixir_get=0"
                )

                sects = conn.execute(
                    "SELECT sect_id, COALESCE(sect_name, ''), sect_owner, "
                    "COALESCE(elixir_room_level, 0), COALESCE(sect_materials, 0) "
                    "FROM sects ORDER BY sect_id"
                ).fetchall()
                outcomes: list[SectMaintenanceOutcome] = []
                for row in sects:
                    sect_id = int(row[0])
                    sect_name = str(row[1])
                    owner = row[2]
                    room_level = int(row[3])
                    materials = int(row[4])
                    cost = costs.get(room_level, 0)
                    to_level = room_level

                    if owner is None:
                        status = "inactive"
                    elif room_level <= 0:
                        status = "no_room"
                    elif room_level not in costs:
                        status = "level_unsupported"
                    elif materials >= cost:
                        status = "charged"
                        conn.execute(
                            "UPDATE sects SET sect_materials=sect_materials-%s "
                            "WHERE sect_id=%s AND elixir_room_level=%s",
                            (cost, sect_id, room_level),
                        )
                    else:
                        to_level = max(room_level - 1, 0)
                        status = "disabled" if to_level == 0 else "downgraded"
                        conn.execute(
                            "UPDATE sects SET elixir_room_level=%s "
                            "WHERE sect_id=%s AND elixir_room_level=%s",
                            (to_level, sect_id, room_level),
                        )

                    outcomes.append(
                        SectMaintenanceOutcome(
                            sect_id,
                            sect_name,
                            status,
                            room_level,
                            to_level,
                            cost,
                        )
                    )

                outcomes_json = json.dumps(
                    [outcome.__dict__ for outcome in outcomes],
                    ensure_ascii=True,
                    sort_keys=True,
                )
                conn.execute(
                    "INSERT INTO sect_daily_reset_operations "
                    "(business_date, payload, user_count, outcomes) "
                    "VALUES (%s, %s, %s, %s)",
                    (business_date, payload, user_count, outcomes_json),
                )
                conn.commit()
                return SectDailyResetResult(
                    "applied", business_date, user_count, tuple(outcomes)
                )
            except Exception:
                conn.rollback()
                raise
