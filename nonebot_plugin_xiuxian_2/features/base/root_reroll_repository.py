from __future__ import annotations

import json
import math
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

from ...core.numeric import as_int_like
from ...infrastructure.database import DatabaseUnitOfWork


@dataclass(frozen=True)
class BaseRootRerollResult:
    status: str
    user_id: str
    root: str = ""
    root_type: str = ""
    power: int = 0
    stone: int = 0

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


class BaseRootRerollSqlRepository:
    OPERATION_TABLE = "player_root_reroll_operations"
    PLAYER_COLUMNS = {
        "user_id", "root", "root_type", "root_level", "level", "exp", "power", "stone",
    }
    SNAPSHOT_FIELDS = ("root", "root_type", "root_level", "level", "exp", "power", "stone")

    def __init__(self, database: str | Path) -> None:
        self.database = Path(database)

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
            and {"operation_id", "user_id", "payload", "result_json", "created_at"}.issubset(
                cls._columns(uow, cls.OPERATION_TABLE)
            )
            and cls.PLAYER_COLUMNS.issubset(cls._columns(uow, "user_xiuxian"))
        )

    @classmethod
    def _snapshot(cls, values: Mapping[str, Any]) -> tuple[Any, ...]:
        return (
            str(values.get("root") or ""),
            str(values.get("root_type") or ""),
            as_int_like(values.get("root_level")),
            str(values.get("level") or ""),
            as_int_like(values.get("exp")),
            as_int_like(values.get("power")),
            as_int_like(values.get("stone")),
        )

    @classmethod
    def _row_snapshot(cls, row: Mapping[str, Any]) -> tuple[Any, ...]:
        return cls._snapshot(dict(row))

    @staticmethod
    def _result(status: str, user_id: str, data: Mapping[str, Any] | None = None):
        values = data or {}
        return BaseRootRerollResult(
            status=status,
            user_id=str(user_id),
            root=str(values.get("root") or ""),
            root_type=str(values.get("root_type") or ""),
            power=as_int_like(values.get("power")),
            stone=as_int_like(values.get("stone")),
        )

    def get_result(self, operation_id: str, user_id: str | None = None) -> BaseRootRerollResult | None:
        operation_id = str(operation_id).strip()
        if not operation_id or not self.database.is_file():
            return None
        with DatabaseUnitOfWork(self.database, read_only=True) as uow:
            if not self._schema_ready(uow):
                return None
            row = uow.query_one(
                f"SELECT user_id,result_json FROM {self.OPERATION_TABLE} WHERE operation_id=?",
                (operation_id,),
            )
        if row is None or (user_id is not None and str(row["user_id"]) != str(user_id)):
            return None
        return self._result("duplicate", str(row["user_id"]), json.loads(str(row["result_json"])))

    def reroll(
        self,
        operation_id: str,
        user_id: str,
        expected_snapshot: Mapping[str, Any],
        root: str,
        root_type: str,
        stone_cost: int,
        root_rate: float,
        level_spend: float,
    ) -> BaseRootRerollResult:
        operation_id = str(operation_id).strip()
        user_id = str(user_id).strip()
        root, root_type = str(root).strip(), str(root_type).strip()
        try:
            stone_cost, root_rate, level_spend = int(stone_cost), float(root_rate), float(level_spend)
        except (TypeError, ValueError, OverflowError):
            return self._result("invalid", user_id)
        if (
            not operation_id or not user_id or not root or not root_type
            or stone_cost < 0 or not math.isfinite(root_rate) or not math.isfinite(level_spend)
            or root_rate <= 0 or level_spend <= 0
            or not isinstance(expected_snapshot, Mapping)
            or not set(self.SNAPSHOT_FIELDS).issubset(expected_snapshot)
        ):
            return self._result("invalid", user_id)
        expected = self._snapshot(expected_snapshot)
        payload = json.dumps(
            [user_id, expected, root, root_type, stone_cost, root_rate, level_spend],
            ensure_ascii=True,
            separators=(",", ":"),
        )
        if not self.database.is_file():
            return self._result("schema_missing", user_id)

        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            if not self._schema_ready(uow):
                return self._result("schema_missing", user_id)
            previous = uow.query_one(
                f"SELECT user_id,payload,result_json FROM {self.OPERATION_TABLE} "
                "WHERE operation_id=?",
                (operation_id,),
            )
            if previous is not None:
                if str(previous["user_id"]) != user_id or str(previous["payload"]) != payload:
                    return self._result("operation_conflict", user_id)
                return self._result("duplicate", user_id, json.loads(str(previous["result_json"])))

            row = uow.query_one(
                "SELECT rowid AS _rowid,user_id,root,root_type,root_level,level,exp,power,stone "
                "FROM user_xiuxian WHERE user_id=? ORDER BY rowid ASC LIMIT 1",
                (user_id,),
            )
            if row is None:
                return self._result("user_missing", user_id)
            if self._row_snapshot(row) != expected:
                return self._result("state_changed", user_id)
            if as_int_like(row["stone"]) < stone_cost:
                return self._result("stone_insufficient", user_id)

            changed = uow.execute(
                "UPDATE user_xiuxian SET root=?,root_type=?,"
                "stone=CAST(COALESCE(stone,0) AS REAL)-CAST(? AS REAL),"
                "power=round(exp * ? * ?,0) "
                "WHERE rowid=? AND user_id=? AND root IS ? AND root_type IS ? "
                "AND root_level IS ? AND level IS ? AND exp IS ? AND power IS ? AND stone IS ?",
                (
                    root, root_type, stone_cost, root_rate, level_spend, row["_rowid"], user_id,
                    row["root"], row["root_type"], row["root_level"], row["level"],
                    row["exp"], row["power"], row["stone"],
                ),
            )
            if changed.rowcount != 1:
                return self._result("state_changed", user_id)
            updated = uow.query_one(
                "SELECT root,root_type,power,stone FROM user_xiuxian WHERE rowid=?",
                (row["_rowid"],),
            )
            data = {
                "root": str(updated["root"] or ""),
                "root_type": str(updated["root_type"] or ""),
                "power": as_int_like(updated["power"]),
                "stone": as_int_like(updated["stone"]),
            }
            uow.execute(
                f"INSERT INTO {self.OPERATION_TABLE}"
                "(operation_id,user_id,payload,result_json) VALUES(?,?,?,?)",
                (operation_id, user_id, payload, json.dumps(data, ensure_ascii=True, separators=(",", ":"))),
            )
            return self._result("applied", user_id, data)


__all__ = ["BaseRootRerollResult", "BaseRootRerollSqlRepository"]
