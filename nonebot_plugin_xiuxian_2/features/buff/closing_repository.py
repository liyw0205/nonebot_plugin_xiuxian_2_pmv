from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from ...infrastructure.clock import SystemClock
from ...infrastructure.database import DatabaseUnitOfWork, OutboxStore


class ClosingSettlementResult:
    def __init__(
        self,
        status: str,
        exp_gain: int = 0,
        stone_cost: int = 0,
        hp: int = 0,
        mp: int = 0,
        atk: int = 0,
        power: int = 0,
        exp_time: int = 0,
        effects_event_id: str | None = None,
        occurred_at: str | None = None,
    ) -> None:
        self.status = status
        self.exp_gain = int(exp_gain)
        self.stone_cost = int(stone_cost)
        self.hp = int(hp)
        self.mp = int(mp)
        self.atk = int(atk)
        self.power = int(power)
        self.exp_time = int(exp_time)
        self.effects_event_id = effects_event_id
        self.occurred_at = occurred_at


class ClosingSettlementSqlRepository:
    effects_event_type = "buff.closing.effects"

    def __init__(
        self,
        database: str | Path,
        *,
        clock: Any | None = None,
        outbox: OutboxStore | None = None,
    ) -> None:
        self.database = str(database)
        self.clock = clock or SystemClock()
        self.outbox = outbox or OutboxStore(clock=self.clock)

    @staticmethod
    def _columns(uow: DatabaseUnitOfWork, table: str) -> set[str]:
        return {
            str(row["name"])
            for row in uow.query_all(f'PRAGMA table_info("{table}")')
        }

    @classmethod
    def _schema_ready(cls, uow: DatabaseUnitOfWork) -> bool:
        required = {
            "closing_settlement_operations": {
                "operation_id", "payload", "result_json", "effects_event_id", "occurred_at",
            },
            "domain_outbox": {
                "event_id", "aggregate_type", "aggregate_id", "event_type", "payload_json",
                "status", "attempts", "next_attempt_at", "created_at", "updated_at",
            },
            "user_xiuxian": {"user_id", "stone", "exp", "hp", "mp", "atk", "power"},
            "user_cd": {"user_id", "type", "create_time", "scheduled_time"},
        }
        return all(required_table <= cls._columns(uow, table) for table, required_table in required.items())

    @staticmethod
    def _from_row(row, status: str) -> ClosingSettlementResult:
        values = json.loads(str(row["result_json"]))
        return ClosingSettlementResult(
            status,
            *values,
            exp_time=int(row.get("exp_time") or 0),
            effects_event_id=str(row["effects_event_id"]) if row.get("effects_event_id") else None,
            occurred_at=str(row["occurred_at"]) if row.get("occurred_at") else None,
        )

    def get_result(self, operation_id: str) -> ClosingSettlementResult | None:
        with DatabaseUnitOfWork(self.database, read_only=True) as uow:
            if not self._schema_ready(uow):
                return None
            row = uow.query_one(
                "SELECT result_json,exp_time,effects_event_id,occurred_at "
                "FROM closing_settlement_operations WHERE operation_id=?",
                (str(operation_id),),
            )
        return self._from_row(row, "duplicate") if row is not None else None

    @staticmethod
    def _effect_payload(
        *, operation_id: str, user_id: str, exp_time: int, values: tuple[int, ...], occurred_at: str
    ) -> dict[str, Any]:
        return {
            "operation_id": str(operation_id),
            "user_id": str(user_id),
            "exp_time": int(exp_time),
            "exp_gain": int(values[0]),
            "stone_cost": int(values[1]),
            "occurred_at": str(occurred_at),
        }

    def settle(
        self,
        operation_id: str,
        user_id: str,
        expected_create_time: str,
        exp_gain: int,
        stone_cost: int,
        hp: int,
        mp: int,
        atk: int,
        power: int,
        exp_time: int = 0,
    ) -> ClosingSettlementResult:
        values = tuple(int(float(value)) for value in (exp_gain, stone_cost, hp, mp, atk, power))
        duration = int(exp_time)
        if not operation_id or min((*values, duration)) < 0:
            raise ValueError("valid closing settlement values are required")
        payload = json.dumps(
            [str(user_id), str(expected_create_time), *values, duration],
            separators=(",", ":"),
        )
        effects_event_id = f"buff.closing.effects:{operation_id}"
        occurred_at = self.clock.now().isoformat()
        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            if not self._schema_ready(uow):
                return ClosingSettlementResult("schema_missing")
            previous = uow.query_one(
                "SELECT payload,result_json,exp_time,effects_event_id,occurred_at "
                "FROM closing_settlement_operations WHERE operation_id=?",
                (operation_id,),
            )
            if previous is not None:
                if str(previous["payload"]) != payload:
                    return ClosingSettlementResult("state_changed")
                return self._from_row(previous, "duplicate")

            user = uow.query_one(
                "SELECT COALESCE(stone,0) AS stone FROM user_xiuxian WHERE user_id=? ORDER BY rowid LIMIT 1",
                (user_id,),
            )
            cd = uow.query_one(
                "SELECT type,create_time FROM user_cd WHERE user_id=? ORDER BY rowid LIMIT 1",
                (user_id,),
            )
            if user is None or cd is None:
                return ClosingSettlementResult("user_missing")
            if int(cd["type"] or 0) != 1 or str(cd["create_time"]) != str(expected_create_time):
                return ClosingSettlementResult("state_changed")
            if int(user["stone"]) < values[1]:
                return ClosingSettlementResult("stone_insufficient")

            changed = uow.execute(
                "UPDATE user_xiuxian SET exp=COALESCE(exp,0)+?,stone=COALESCE(stone,0)-?,"
                "hp=?,mp=?,atk=?,power=? WHERE rowid=(SELECT rowid FROM user_xiuxian "
                "WHERE user_id=? ORDER BY rowid LIMIT 1) AND stone>=?",
                (values[0], values[1], values[2], values[3], values[4], values[5], user_id, values[1]),
            )
            cleared = uow.execute(
                "UPDATE user_cd SET type=0,create_time=0,scheduled_time=NULL "
                "WHERE rowid=(SELECT rowid FROM user_cd WHERE user_id=? ORDER BY rowid LIMIT 1) "
                "AND type=1 AND CAST(create_time AS TEXT)=?",
                (user_id, str(expected_create_time)),
            )
            if changed.rowcount != 1 or cleared.rowcount != 1:
                return ClosingSettlementResult("state_changed")

            event_payload = self._effect_payload(
                operation_id=operation_id,
                user_id=user_id,
                exp_time=duration,
                values=values,
                occurred_at=occurred_at,
            )
            self.outbox.append(
                uow,
                event_id=effects_event_id,
                aggregate_type="player",
                aggregate_id=str(user_id),
                event_type=self.effects_event_type,
                payload=event_payload,
            )
            uow.execute(
                "INSERT INTO closing_settlement_operations "
                "(operation_id,payload,result_json,exp_time,effects_event_id,occurred_at) VALUES(?,?,?,?,?,?)",
                (
                    operation_id,
                    payload,
                    json.dumps(values, separators=(",", ":")),
                    duration,
                    effects_event_id,
                    occurred_at,
                ),
            )
            return ClosingSettlementResult(
                "applied", *values, exp_time=duration,
                effects_event_id=effects_event_id, occurred_at=occurred_at,
            )


__all__ = ["ClosingSettlementSqlRepository", "ClosingSettlementResult"]
