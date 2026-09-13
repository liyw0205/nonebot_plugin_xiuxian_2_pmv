from __future__ import annotations

from pathlib import Path
from typing import Any, Mapping

from .uow import DatabaseUnitOfWork


class AttachedDatabaseUnitOfWork(DatabaseUnitOfWork):
    """Transaction UoW with explicit SQLite attached databases."""

    def __init__(self, database: str | Path, *, attachments: Mapping[str, str | Path], timeout: float = 30, immediate: bool = False) -> None:
        super().__init__(database, timeout=timeout, immediate=immediate)
        self.attachments = {str(alias): Path(path) for alias, path in attachments.items()}

    def __enter__(self) -> "AttachedDatabaseUnitOfWork":
        super().__enter__()
        try:
            for alias, database in self.attachments.items():
                if not alias.replace("_", "").isalnum():
                    raise ValueError("invalid SQLite attachment alias")
                if not database.exists():
                    raise FileNotFoundError(database)
                self.execute(f'ATTACH DATABASE ? AS "{alias}"', (str(database),))
            return self
        except Exception:
            self.__exit__(RuntimeError, RuntimeError("attachment failed"), None)
            raise


__all__ = ["AttachedDatabaseUnitOfWork"]
