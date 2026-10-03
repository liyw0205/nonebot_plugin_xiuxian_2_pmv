from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from threading import RLock
from typing import Any

from ...core.numeric import as_int_like
from ...infrastructure.database import DatabaseUnitOfWork


@dataclass(frozen=True)
class AdminPlayerStatusResetResult:
    status: str
    previous_state: tuple[int, int, int, int, int] = ()
    final_state: tuple[int, int, int, int, int] = ()

    @property
    def succeeded(self) -> bool:
        return self.status in {"reset", "duplicate"}


class AdminPlayerStatusResetSqlRepository:
    """Reset one player's status using the startup-migrated game schema."""

    _PLAYER_COLUMNS = {"user_id", "exp", "hp", "mp", "atk", "user_stamina"}
    _OPERATION_COLUMNS = {
        "operation_id", "payload", "previous_state", "final_state"
    }

    def __init__(self, database: str | Path, *, lock: RLock | None = None) -> None:
        self.database = Path(database)
        self.lock = lock or RLock()

    @staticmethod
    def _columns(uow: DatabaseUnitOfWork, table: str) -> set[str]:
        return {str(row["name"]) for row in uow.query_all(f'PRAGMA table_info("{table}")')}

    @classmethod
    def _schema_ready(cls, uow: DatabaseUnitOfWork) -> bool:
        tables = {
            str(row["name"])
            for row in uow.query_all("SELECT name FROM sqlite_master WHERE type='table'")
        }
        return (
            {"user_xiuxian", "admin_player_status_reset_operations"}.issubset(tables)
            and cls._PLAYER_COLUMNS.issubset(cls._columns(uow, "user_xiuxian"))
            and cls._OPERATION_COLUMNS.issubset(
                cls._columns(uow, "admin_player_status_reset_operations")
            )
        )

    @staticmethod
    def _state(row: Any) -> tuple[int, int, int, int, int]:
        return tuple(as_int_like(value, 0) for value in row)

    @staticmethod
    def _number_count(value: int) -> int | str:
        if value > 2**63 - 1 or value < -(2**63):
            return str(value)
        return value

    def snapshot(self, user_id: Any) -> tuple[int, int, int, int, int] | None:
        user_id = str(user_id).strip()
        if not user_id:
            raise ValueError("user id is required")
        if not self.database.is_file():
            raise RuntimeError("schema_missing")
        with self.lock, DatabaseUnitOfWork(self.database, read_only=True) as uow:
            if not self._schema_ready(uow):
                raise RuntimeError("schema_missing")
            row = uow.query_one(
                "SELECT exp,hp,mp,atk,user_stamina FROM user_xiuxian WHERE user_id=?",
                (user_id,),
            )
        if row is None:
            return None
        return self._state(tuple(row[field] for field in (
            "exp", "hp", "mp", "atk", "user_stamina"
        )))

    def reset(
        self,
        operation_id: Any,
        operator_id: Any,
        user_id: Any,
        expected_state: Any,
        max_stamina: Any,
        *,
        target_name: str = "",
        force: bool = False,
    ) -> AdminPlayerStatusResetResult:
        operation_id = str(operation_id).strip()
        operator_id = str(operator_id).strip()
        user_id = str(user_id).strip()
        max_stamina = int(max_stamina)
        if not operation_id or not operator_id or not user_id:
            raise ValueError("operation, operator and user are required")
        if max_stamina <= 0:
            raise ValueError("stamina limit must be positive")
        if expected_state is None:
            normalized_expected = None
        else:
            normalized_expected = tuple(as_int_like(value, 0) for value in expected_state)
            if len(normalized_expected) != 5:
                raise ValueError("complete status snapshot is required")
        del target_name

        if not self.database.is_file():
            return AdminPlayerStatusResetResult("schema_missing")
        payload = json.dumps(
            [operator_id, user_id, max_stamina, int(force)],
            ensure_ascii=True,
            separators=(",", ":"),
        )
        with self.lock, DatabaseUnitOfWork(self.database, immediate=True) as uow:
            if not self._schema_ready(uow):
                return AdminPlayerStatusResetResult("schema_missing")

            previous = uow.query_one(
                "SELECT payload,previous_state,final_state "
                "FROM admin_player_status_reset_operations WHERE operation_id=?",
                (operation_id,),
            )
            if previous is not None:
                if str(previous["payload"]) != payload:
                    return AdminPlayerStatusResetResult("operation_conflict")
                return AdminPlayerStatusResetResult(
                    "duplicate",
                    tuple(json.loads(str(previous["previous_state"]))),
                    tuple(json.loads(str(previous["final_state"]))),
                )

            row = uow.query_one(
                "SELECT exp,hp,mp,atk,user_stamina FROM user_xiuxian WHERE user_id=?",
                (user_id,),
            )
            if row is None:
                return AdminPlayerStatusResetResult("user_missing")
            actual_state = self._state(tuple(row[field] for field in (
                "exp", "hp", "mp", "atk", "user_stamina"
            )))
            if (
                not force
                and normalized_expected is not None
                and actual_state != normalized_expected
            ):
                return AdminPlayerStatusResetResult(
                    "state_changed", actual_state, actual_state
                )

            final_state = (
                actual_state[0],
                actual_state[0] // 2,
                actual_state[0],
                actual_state[0] // 10,
                max_stamina,
            )
            changed = uow.execute(
                "UPDATE user_xiuxian SET hp=CAST(? AS REAL),mp=CAST(? AS REAL),"
                "atk=CAST(? AS REAL),user_stamina=? WHERE user_id=?",
                (
                    self._number_count(final_state[1]),
                    self._number_count(final_state[2]),
                    self._number_count(final_state[3]),
                    max_stamina,
                    user_id,
                ),
            )
            if changed.rowcount != 1:
                return AdminPlayerStatusResetResult(
                    "state_changed", actual_state, actual_state
                )

            uow.execute(
                "INSERT INTO admin_player_status_reset_operations"
                "(operation_id,payload,previous_state,final_state) VALUES(?,?,?,?)",
                (
                    operation_id,
                    payload,
                    json.dumps(
                        [self._number_count(value) if index < 4 else value
                         for index, value in enumerate(actual_state)],
                        ensure_ascii=True,
                        separators=(",", ":"),
                    ),
                    json.dumps(
                        [self._number_count(value) if index < 4 else value
                         for index, value in enumerate(final_state)],
                        ensure_ascii=True,
                        separators=(",", ":"),
                    ),
                ),
            )
            return AdminPlayerStatusResetResult("reset", actual_state, final_state)


__all__ = ["AdminPlayerStatusResetResult", "AdminPlayerStatusResetSqlRepository"]
