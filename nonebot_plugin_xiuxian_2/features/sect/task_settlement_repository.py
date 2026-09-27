from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from ...infrastructure.clock import SystemClock
from ...infrastructure.database import DatabaseUnitOfWork


class SectTaskSettlementSqlRepository:
    _OPERATION_COLUMNS = {
        "operation_id",
        "user_id",
        "sect_id",
        "period",
        "cost_type",
        "cost",
        "exp_reward",
        "sect_reward",
        "materials_reward",
        "created_at",
    }

    def __init__(self, database: str | Path, *, clock: Any | None = None) -> None:
        self.database = str(database)
        self.clock = clock or SystemClock()

    @classmethod
    def _schema_ready(cls, uow: DatabaseUnitOfWork) -> bool:
        table = uow.query_one(
            "SELECT 1 AS present FROM sqlite_master "
            "WHERE type='table' AND name='sect_task_settlement_operations'"
        )
        if table is None:
            return False
        columns = {
            str(row["name"])
            for row in uow.query_all("PRAGMA table_info(sect_task_settlement_operations)")
        }
        return cls._OPERATION_COLUMNS.issubset(columns)

    def settle(
        self,
        operation_id: str,
        user_id: str,
        sect_id: int,
        period: str,
        cost_type: str,
        cost: int,
        exp_reward: int,
        sect_reward: int,
        expected_task_key: str | None = None,
        expected_task_data: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        operation_id = str(operation_id).strip()
        user_id = str(user_id)
        period = str(period).strip()
        cost_type = str(cost_type).strip().lower()
        sect_id, cost, exp_reward, sect_reward = map(
            int, (sect_id, cost, exp_reward, sect_reward)
        )
        materials_reward = sect_reward * 10
        if not operation_id or not period:
            raise ValueError("operation_id and period are required")

        base = {
            "user_id": user_id,
            "sect_id": sect_id,
            "period": period,
            "cost_type": cost_type,
            "cost": cost,
            "exp_reward": exp_reward,
            "sect_reward": sect_reward,
            "materials_reward": materials_reward,
        }
        if cost_type not in {"hp", "stone"}:
            return {"status": "invalid_cost_type", **base}
        if min(cost, exp_reward, sect_reward) < 0:
            return {"status": "invalid_amount", **base}

        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            if not self._schema_ready(uow):
                return {"status": "schema_missing", **base}

            previous = uow.query_one(
                "SELECT cost_type,cost,exp_reward,sect_reward,materials_reward "
                "FROM sect_task_settlement_operations WHERE operation_id=?",
                (operation_id,),
            )
            if previous:
                return {
                    "status": "duplicate",
                    **base,
                    "cost_type": str(previous["cost_type"]),
                    "cost": int(previous["cost"]),
                    "exp_reward": int(previous["exp_reward"]),
                    "sect_reward": int(previous["sect_reward"]),
                    "materials_reward": int(previous["materials_reward"]),
                }

            task = uow.query_one(
                "SELECT sect_id,status,task_key,task_data FROM sect_task_state "
                "WHERE user_id=? AND period=?",
                (user_id, period),
            )
            if task is None or str(task["status"]) != "accepted":
                return {"status": "task_missing", **base}
            if int(task["sect_id"]) != sect_id:
                return {"status": "task_sect_changed", **base}
            if expected_task_key is not None and str(task["task_key"]) != str(expected_task_key):
                return {"status": "task_snapshot_changed", **base}
            if expected_task_data is not None and json.loads(task["task_data"] or "{}") != dict(expected_task_data):
                return {"status": "task_snapshot_changed", **base}

            user = uow.query_one(
                "SELECT sect_id,stone,hp FROM user_xiuxian WHERE user_id=?",
                (user_id,),
            )
            if user is None:
                return {"status": "user_missing", **base}
            if user["sect_id"] is None or int(user["sect_id"]) != sect_id:
                return {"status": "sect_changed", **base}

            balance = int(user["hp"] if cost_type == "hp" else user["stone"] or 0)
            if balance < cost:
                return {"status": f"{cost_type}_insufficient", **base}
            if uow.query_one("SELECT sect_id FROM sects WHERE sect_id=?", (sect_id,)) is None:
                return {"status": "sect_missing", **base}

            column = "hp" if cost_type == "hp" else "stone"
            now = self.clock.now()
            now = now.astimezone() if now.tzinfo is not None else now
            now = now.replace(tzinfo=None).strftime("%Y-%m-%d %H:%M:%S")
            user_update = uow.execute(
                f"UPDATE user_xiuxian SET {column}={column}-?,"
                "exp=COALESCE(exp,0)+?,sect_task=COALESCE(sect_task,0)+1,"
                "sect_contribution=COALESCE(sect_contribution,0)+? "
                f"WHERE user_id=? AND sect_id=? AND {column}>=?",
                (cost, exp_reward, sect_reward, user_id, sect_id, cost),
            )
            sect_update = uow.execute(
                "UPDATE sects SET sect_used_stone=COALESCE(sect_used_stone,0)+?,"
                "sect_scale=COALESCE(sect_scale,0)+?,"
                "sect_materials=COALESCE(sect_materials,0)+? WHERE sect_id=?",
                (sect_reward, sect_reward, materials_reward, sect_id),
            )
            task_update = uow.execute(
                'UPDATE sect_task_state SET status="completed",progress=target, '
                "updated_at=?,completed_at=? WHERE user_id=? AND period=? "
                'AND sect_id=? AND status="accepted"',
                (now, now, user_id, period, sect_id),
            )
            if user_update.rowcount != 1 or sect_update.rowcount != 1 or task_update.rowcount != 1:
                raise RuntimeError("task settlement state changed concurrently")

            uow.execute(
                "INSERT INTO sect_task_settlement_operations("
                "operation_id,user_id,sect_id,period,cost_type,cost,exp_reward,"
                "sect_reward,materials_reward) VALUES(?,?,?,?,?,?,?,?,?)",
                (operation_id, user_id, sect_id, period, cost_type, cost, exp_reward, sect_reward, materials_reward),
            )
            return {"status": "settled", **base}
