from __future__ import annotations

from pathlib import Path
from typing import Mapping

from ...infrastructure.database import DatabaseUnitOfWork
from .id_swap_repository import DATABASE_ORDER, TARGET_COLUMNS, _quote_ident


class QqidCandidateSchemaError(RuntimeError):
    code = "schema_missing"


class AdminQqidCandidateRepository:
    """Freeze candidate values without creating or modifying any legacy database."""

    def __init__(self, databases: Mapping[str, str | Path]) -> None:
        self.databases = {key: Path(databases[key]) for key in DATABASE_ORDER}

    def snapshot(self) -> tuple[str, ...]:
        if not all(path.is_file() for path in self.databases.values()):
            raise QqidCandidateSchemaError("QQID candidate database missing")
        candidates: set[str] = set()
        for key in DATABASE_ORDER:
            with DatabaseUnitOfWork(self.databases[key], read_only=True) as uow:
                tables = uow.query_all(
                    "SELECT name FROM sqlite_master WHERE type='table' "
                    "AND name NOT LIKE 'sqlite_%' ORDER BY name"
                )
                targets = 0
                for row in tables:
                    table = _quote_ident(str(row["name"]))
                    columns = {str(column["name"]) for column in uow.query_all(f"PRAGMA table_info({table})")}
                    for column_name in sorted(columns.intersection(TARGET_COLUMNS[key])):
                        targets += 1
                        column = _quote_ident(column_name)
                        cursor = uow.execute(
                            f"SELECT DISTINCT CAST({column} AS TEXT) AS candidate FROM {table} "
                            f"WHERE {column} IS NOT NULL"
                        )
                        for value in cursor:
                            candidate = str(value["candidate"]).strip()
                            if candidate:
                                candidates.add(candidate)
                if not targets:
                    raise QqidCandidateSchemaError(f"QQID candidate columns missing: {key}")
        return tuple(sorted(candidates))


__all__ = ["AdminQqidCandidateRepository", "QqidCandidateSchemaError"]
