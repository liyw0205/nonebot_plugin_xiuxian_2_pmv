from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping

from ...infrastructure.database import DatabaseUnitOfWork


@dataclass(frozen=True)
class AdminLegacyRealmAdaptationResult:
    status: str
    operation_id: str
    adapted_count: int = 0
    failed_count: int = 0
    success_count: int = 0

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


class AdminLegacyRealmAdaptationSqlRepository:
    OPERATION_TABLE = "admin_level_change_operations"
    PLAYER_COLUMNS = {"user_id", "level"}
    STAGES = ("", "初期", "中期", "圆满")

    def __init__(self, database: str | Path) -> None:
        self.database = Path(database)

    @classmethod
    def _schema_ready(cls, uow: DatabaseUnitOfWork) -> bool:
        receipt_columns = {
            str(row["name"]).casefold()
            for row in uow.query_all(f'PRAGMA main.table_info("{cls.OPERATION_TABLE}")')
        }
        player_columns = {
            str(row["name"]).casefold()
            for row in uow.query_all('PRAGMA main.table_info("user_xiuxian")')
        }
        return (
            {"operation_id", "payload", "result_json", "created_at"}.issubset(receipt_columns)
            and cls.PLAYER_COLUMNS.issubset(player_columns)
        )

    @classmethod
    def _expanded_mapping(cls, level_mapping: Mapping[str, str]) -> tuple[tuple[str, str], ...]:
        if not isinstance(level_mapping, Mapping) or not level_mapping:
            raise ValueError("legacy realm mapping must not be empty")
        expanded = tuple(
            (f"{old}{stage}", f"{new}{stage}")
            for old, new in level_mapping.items()
            for stage in cls.STAGES
            if str(old).strip() and str(new).strip()
        )
        if not expanded or len({old for old, _ in expanded}) != len(expanded):
            raise ValueError("legacy realm mapping is invalid")
        return expanded

    @staticmethod
    def _result(status: str, operation_id: str, data: Mapping[str, object] | None = None):
        values = data or {}
        return AdminLegacyRealmAdaptationResult(
            status=status,
            operation_id=operation_id,
            adapted_count=int(values.get("adapted_count", 0) or 0),
            failed_count=int(values.get("failed_count", 0) or 0),
            success_count=int(values.get("success_count", 0) or 0),
        )

    def adapt(
        self,
        operation_id: str,
        operator_id: str,
        level_mapping: Mapping[str, str],
    ) -> AdminLegacyRealmAdaptationResult:
        operation_id, operator_id = str(operation_id).strip(), str(operator_id).strip()
        if not operation_id or not operator_id:
            return self._result("invalid", operation_id)
        expanded = self._expanded_mapping(level_mapping)
        payload = json.dumps(
            [operator_id, expanded], ensure_ascii=True, separators=(",", ":")
        )
        if not self.database.is_file():
            return self._result("schema_missing", operation_id)

        mapping_values = ",".join("(?,?)" for _ in expanded)
        mapping_parameters = tuple(value for pair in expanded for value in pair)
        mapping_cte = f"WITH realm_map(old_level,new_level) AS (VALUES {mapping_values})"
        first_user_cte = (
            "first_users AS ("
            "SELECT current.user_id,current.level FROM user_xiuxian AS current "
            "WHERE current.rowid=(SELECT MIN(first.rowid) FROM user_xiuxian AS first "
            "WHERE first.user_id=current.user_id))"
        )

        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            if not self._schema_ready(uow):
                return self._result("schema_missing", operation_id)
            previous = uow.query_one(
                f"SELECT payload,result_json FROM {self.OPERATION_TABLE} WHERE operation_id=?",
                (operation_id,),
            )
            if previous is not None:
                if str(previous["payload"]) != payload:
                    return self._result("operation_conflict", operation_id)
                return self._result(
                    "duplicate", operation_id, json.loads(str(previous["result_json"]))
                )

            counts = uow.query_one(
                f"{mapping_cte},{first_user_cte} "
                "SELECT (SELECT COUNT(*) FROM user_xiuxian) AS total_count,"
                "COUNT(realm_map.new_level) AS adapted_count "
                "FROM first_users LEFT JOIN realm_map ON realm_map.old_level=first_users.level",
                mapping_parameters,
            )
            total_count = int(counts["total_count"] or 0)
            adapted_count = int(counts["adapted_count"] or 0)
            if adapted_count:
                uow.execute(
                    f"{mapping_cte},{first_user_cte} "
                    "UPDATE user_xiuxian SET level=("
                    "SELECT realm_map.new_level FROM first_users "
                    "JOIN realm_map ON realm_map.old_level=first_users.level "
                    "WHERE first_users.user_id=user_xiuxian.user_id) "
                    "WHERE user_id IN (SELECT first_users.user_id FROM first_users "
                    "JOIN realm_map ON realm_map.old_level=first_users.level)",
                    mapping_parameters,
                )

            data = {
                "adapted_count": adapted_count,
                "failed_count": 0,
                "success_count": total_count - adapted_count,
            }
            uow.execute(
                f"INSERT INTO {self.OPERATION_TABLE}(operation_id,payload,result_json) "
                "VALUES(?,?,?)",
                (
                    operation_id,
                    payload,
                    json.dumps(data, ensure_ascii=True, separators=(",", ":")),
                ),
            )
            return self._result("applied", operation_id, data)


__all__ = ["AdminLegacyRealmAdaptationResult", "AdminLegacyRealmAdaptationSqlRepository"]
