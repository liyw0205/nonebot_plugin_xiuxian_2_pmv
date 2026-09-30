from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

from ...infrastructure.database import DatabaseUnitOfWork


@dataclass(frozen=True)
class AdminImpartStoneSnapshot:
    status: str
    stone: int | None = None


@dataclass(frozen=True)
class AdminImpartStoneResult:
    status: str
    previous_stone: int = 0
    final_stone: int = 0
    applied_delta: int = 0

    @property
    def succeeded(self) -> bool:
        return self.status in {"adjusted", "duplicate"}


class AdminImpartStoneSqlRepository:
    def __init__(self, game_database: str | Path, impart_database: str | Path) -> None:
        self.game_database = Path(game_database)
        self.impart_database = Path(impart_database)

    @staticmethod
    def _columns(uow: DatabaseUnitOfWork, table: str, schema: str = "main") -> set[str]:
        rows = uow.query_all(f'PRAGMA "{schema}".table_info("{table}")')
        return {str(row["name"]).casefold() for row in rows}

    @classmethod
    def _schema_ready(cls, uow: DatabaseUnitOfWork) -> bool:
        return (
            {"user_id"}.issubset(cls._columns(uow, "user_xiuxian"))
            and {
                "operation_id", "payload", "previous_stone", "final_stone", "applied_delta"
            }.issubset(cls._columns(uow, "admin_impart_stone_operations"))
            and {
                "user_id", "source", "action", "item_delta", "detail", "trace_id", "created_at"
            }.issubset(cls._columns(uow, "economy_log"))
            and {"user_id", "stone_num"}.issubset(
                cls._columns(uow, "xiuxian_impart", "impart_data")
            )
        )

    def snapshot(self, user_id: str) -> AdminImpartStoneSnapshot:
        user_id = str(user_id).strip()
        if not user_id:
            raise ValueError("user id is required")
        if not self.impart_database.is_file():
            return AdminImpartStoneSnapshot("schema_missing")

        with DatabaseUnitOfWork(self.impart_database, read_only=True) as uow:
            columns = self._columns(uow, "xiuxian_impart")
            if not {"user_id", "stone_num"}.issubset(columns):
                return AdminImpartStoneSnapshot("schema_missing")
            rows = uow.query_all(
                "SELECT COALESCE(stone_num,0) AS stone_num "
                "FROM xiuxian_impart WHERE user_id=?",
                (user_id,),
            )
            if len(rows) > 1:
                return AdminImpartStoneSnapshot("invalid_state")
            return AdminImpartStoneSnapshot(
                "ok", int(rows[0]["stone_num"]) if rows else None
            )

    def adjust(
        self,
        operation_id: str,
        operator_id: str,
        user_id: str,
        expected_stone: int | None,
        requested_delta: int,
        *,
        target_name: str = "",
    ) -> AdminImpartStoneResult:
        operation_id = str(operation_id).strip()
        operator_id = str(operator_id).strip()
        user_id = str(user_id).strip()
        expected_stone = None if expected_stone is None else int(expected_stone)
        requested_delta = int(requested_delta)
        if not operation_id or not operator_id or not user_id:
            raise ValueError("operation, operator and user are required")
        if expected_stone is not None and expected_stone < 0:
            raise ValueError("stone snapshot cannot be negative")
        if requested_delta == 0:
            raise ValueError("adjustment cannot be zero")

        if not self.game_database.is_file() or not self.impart_database.is_file():
            return AdminImpartStoneResult("schema_missing")

        payload = json.dumps(
            [operator_id, user_id, requested_delta],
            ensure_ascii=True,
            separators=(",", ":"),
        )
        with DatabaseUnitOfWork(self.game_database, immediate=True) as uow:
            uow.attach_database(self.impart_database, "impart_data")
            if not self._schema_ready(uow):
                return AdminImpartStoneResult("schema_missing")

            previous = uow.query_one(
                "SELECT payload,previous_stone,final_stone,applied_delta "
                "FROM admin_impart_stone_operations WHERE operation_id=?",
                (operation_id,),
            )
            if previous is not None:
                if str(previous["payload"]) != payload:
                    return AdminImpartStoneResult("operation_conflict")
                return AdminImpartStoneResult(
                    "duplicate",
                    int(previous["previous_stone"]),
                    int(previous["final_stone"]),
                    int(previous["applied_delta"]),
                )

            if uow.query_one(
                "SELECT 1 FROM user_xiuxian WHERE user_id=?", (user_id,)
            ) is None:
                return AdminImpartStoneResult("user_missing")
            rows = uow.query_all(
                "SELECT COALESCE(stone_num,0) AS stone_num "
                "FROM impart_data.xiuxian_impart WHERE user_id=?",
                (user_id,),
            )
            if len(rows) > 1:
                return AdminImpartStoneResult("invalid_state")
            actual_stone = int(rows[0]["stone_num"]) if rows else None
            if actual_stone != expected_stone:
                current = int(actual_stone or 0)
                return AdminImpartStoneResult("state_changed", current, current)

            previous_stone = int(actual_stone or 0)
            final_stone = max(0, previous_stone + requested_delta)
            applied_delta = final_stone - previous_stone
            if actual_stone is None:
                uow.execute(
                    "INSERT INTO impart_data.xiuxian_impart(user_id,stone_num) "
                    "VALUES(?,?)",
                    (user_id, final_stone),
                )
            else:
                changed = uow.execute(
                    "UPDATE impart_data.xiuxian_impart SET stone_num=? "
                    "WHERE user_id=? AND COALESCE(stone_num,0)=?",
                    (final_stone, user_id, previous_stone),
                )
                if changed.rowcount != 1:
                    return AdminImpartStoneResult("state_changed", previous_stone, previous_stone)

            item_delta = json.dumps(
                [{
                    "id": "impart_stone",
                    "name": "思恋结晶",
                    "type": "传承货币",
                    "amount": applied_delta,
                }],
                ensure_ascii=False,
                separators=(",", ":"),
            )
            detail = json.dumps(
                {
                    "operator_id": operator_id,
                    "target_name": str(target_name),
                    "requested_delta": requested_delta,
                    "previous_stone": previous_stone,
                    "final_stone": final_stone,
                },
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            )
            uow.execute(
                "INSERT INTO economy_log("
                "user_id,source,action,item_delta,detail,trace_id,created_at) "
                "VALUES(?,'admin',?,?,?,?,CURRENT_TIMESTAMP)",
                (
                    user_id,
                    "admin_impart_stone_add"
                    if requested_delta > 0
                    else "admin_impart_stone_cost",
                    item_delta,
                    detail,
                    operation_id,
                ),
            )
            uow.execute(
                "INSERT INTO admin_impart_stone_operations("
                "operation_id,payload,previous_stone,final_stone,applied_delta) "
                "VALUES(?,?,?,?,?)",
                (operation_id, payload, previous_stone, final_stone, applied_delta),
            )
            return AdminImpartStoneResult(
                "adjusted", previous_stone, final_stone, applied_delta
            )


__all__ = [
    "AdminImpartStoneSqlRepository",
    "AdminImpartStoneResult",
    "AdminImpartStoneSnapshot",
]
