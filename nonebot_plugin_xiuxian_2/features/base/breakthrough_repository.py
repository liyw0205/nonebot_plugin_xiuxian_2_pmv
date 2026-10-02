from __future__ import annotations

import json
import math
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from ...core.numeric import as_int_like
from ...infrastructure.clock import SystemClock
from ...infrastructure.database import DatabaseUnitOfWork


@dataclass(frozen=True)
class BaseDirectBreakthroughResult:
    status: str
    user_id: str
    outcome: str = ""
    from_level: str = ""
    to_level: str = ""
    exp_loss: int = 0

    @property
    def applied(self) -> bool:
        return self.status == "applied"


class BaseDirectBreakthroughSqlRepository:
    OPERATION_TABLE = "direct_breakthrough_operations"
    PLAYER_COLUMNS = {
        "user_id", "level", "exp", "hp", "mp", "atk", "power",
        "level_up_rate", "level_up_cd",
    }

    def __init__(self, database: str | Path, *, clock: Any | None = None) -> None:
        self.database = Path(database)
        self.clock = clock or SystemClock()

    @staticmethod
    def _columns(uow: DatabaseUnitOfWork, table: str) -> set[str]:
        return {
            str(row["name"]).casefold()
            for row in uow.query_all(f'PRAGMA main.table_info("{table}")')
        }

    @classmethod
    def _schema_ready(cls, uow: DatabaseUnitOfWork) -> bool:
        table = uow.query_one(
            "SELECT 1 AS present FROM sqlite_master WHERE type='table' AND name=?",
            (cls.OPERATION_TABLE,),
        )
        return (
            table is not None
            and {
                "operation_id", "user_id", "outcome", "from_level", "to_level",
                "exp_loss", "payload",
            }.issubset(cls._columns(uow, cls.OPERATION_TABLE))
            and cls.PLAYER_COLUMNS.issubset(cls._columns(uow, "user_xiuxian"))
        )

    @staticmethod
    def _result(
        status: str,
        user_id: str,
        outcome: str = "",
        from_level: str = "",
        to_level: str = "",
        exp_loss: int = 0,
    ) -> BaseDirectBreakthroughResult:
        return BaseDirectBreakthroughResult(
            status, str(user_id), str(outcome), str(from_level), str(to_level), int(exp_loss)
        )

    def apply(
        self,
        operation_id: str,
        user_id: str,
        outcome: str,
        expected_level: str,
        target_level: str,
        expected_exp: int,
        expected_hp: int,
        expected_mp: int,
        expected_rate: int,
        *,
        exp_loss: int = 0,
        new_hp: int = 0,
        new_mp: int = 0,
        new_rate: int = 0,
        root_rate: float = 0.0,
        level_spend: float = 0.0,
        occurred_at: datetime | str | None = None,
    ) -> BaseDirectBreakthroughResult:
        operation_id = str(operation_id).strip()
        user_id = str(user_id).strip()
        outcome = str(outcome).strip().casefold()
        expected_level, target_level = str(expected_level), str(target_level)
        if not operation_id or not user_id or outcome not in {"success", "failure"}:
            return self._result("invalid", user_id, outcome)
        try:
            expected_exp = int(expected_exp)
            expected_hp = int(expected_hp)
            expected_mp = int(expected_mp)
            expected_rate = int(expected_rate)
            exp_loss = max(int(exp_loss), 0)
            new_hp, new_mp, new_rate = int(new_hp), int(new_mp), int(new_rate)
            root_rate, level_spend = float(root_rate), float(level_spend)
        except (TypeError, ValueError, OverflowError):
            return self._result("invalid", user_id, outcome)
        if not math.isfinite(root_rate) or not math.isfinite(level_spend):
            return self._result("invalid", user_id, outcome)
        if outcome == "success" and (not target_level or root_rate <= 0 or level_spend <= 0):
            return self._result("invalid", user_id, outcome)

        payload = json.dumps(
            [
                user_id, outcome, expected_level, target_level, expected_exp,
                expected_hp, expected_mp, expected_rate, exp_loss, new_hp,
                new_mp, new_rate, root_rate, level_spend,
            ],
            ensure_ascii=True,
            separators=(",", ":"),
        )
        if not self.database.is_file():
            return self._result("schema_missing", user_id, outcome)

        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            if not self._schema_ready(uow):
                return self._result("schema_missing", user_id, outcome)
            previous = uow.query_one(
                f"SELECT user_id,outcome,from_level,to_level,exp_loss,payload "
                f"FROM {self.OPERATION_TABLE} WHERE operation_id=?",
                (operation_id,),
            )
            if previous is not None:
                if str(previous["user_id"]) != user_id:
                    return self._result("operation_conflict", user_id, outcome)
                old_payload = str(previous["payload"] or "")
                if old_payload and old_payload != payload:
                    return self._result("operation_conflict", user_id, outcome)
                return self._result(
                    "duplicate",
                    user_id,
                    str(previous["outcome"]),
                    str(previous["from_level"]),
                    str(previous["to_level"]),
                    as_int_like(previous["exp_loss"]),
                )

            event_time = occurred_at or self.clock.now().astimezone().replace(tzinfo=None)

            row = uow.query_one(
                "SELECT rowid AS _rowid,level,exp,hp,mp,level_up_rate "
                "FROM user_xiuxian WHERE user_id=? ORDER BY rowid ASC LIMIT 1",
                (user_id,),
            )
            if row is None:
                return self._result("user_missing", user_id, outcome)
            if (
                str(row["level"] or "") != expected_level
                or as_int_like(row["exp"]) != expected_exp
                or as_int_like(row["hp"]) != expected_hp
                or as_int_like(row["mp"]) != expected_mp
                or as_int_like(row["level_up_rate"]) != expected_rate
            ):
                current_level = str(row["level"] or "")
                return self._result("state_changed", user_id, outcome, current_level, current_level)

            occurred_value: Any = event_time
            if outcome == "failure":
                changed = uow.execute(
                    "UPDATE user_xiuxian SET "
                    "exp=MAX(CAST(COALESCE(exp,0) AS REAL)-CAST(? AS REAL),0),"
                    "hp=?,mp=?,level_up_rate=?,level_up_cd=? "
                    "WHERE rowid=? AND user_id=?",
                    (
                        str(exp_loss), str(new_hp), str(new_mp), str(new_rate), occurred_value,
                        row["_rowid"], user_id,
                    ),
                )
            else:
                changed = uow.execute(
                    "UPDATE user_xiuxian SET level=?,power=ROUND(exp*?*?,0),"
                    "level_up_cd=?,level_up_rate=0,hp=exp/2,mp=exp,atk=exp/10 "
                    "WHERE rowid=? AND user_id=?",
                    (
                        target_level, root_rate, level_spend, occurred_value,
                        row["_rowid"], user_id,
                    ),
                )
            if changed.rowcount != 1:
                return self._result("state_changed", user_id, outcome)

            uow.execute(
                f"INSERT INTO {self.OPERATION_TABLE} "
                "(operation_id,user_id,outcome,from_level,to_level,exp_loss,payload) "
                "VALUES(?,?,?,?,?,?,?)",
                (
                    operation_id, user_id, outcome, expected_level, target_level,
                    str(exp_loss), payload,
                ),
            )
            return self._result("applied", user_id, outcome, expected_level, target_level, exp_loss)


__all__ = ["BaseDirectBreakthroughResult", "BaseDirectBreakthroughSqlRepository"]
