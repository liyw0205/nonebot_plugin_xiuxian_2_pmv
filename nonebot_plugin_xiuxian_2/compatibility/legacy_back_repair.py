"""Explicit rollback wrapper for the historical backpack repair service."""

from __future__ import annotations

from pathlib import Path
from threading import RLock

from ..features.back.repair_repository import BackpackRepairResult, BackpackRepairSqlRepository


class BackpackRepairService(BackpackRepairSqlRepository):
    """Compatibility wrapper that preserves request-time schema setup."""

    def __init__(self, database: str | Path, lock: RLock | None = None) -> None:
        super().__init__(database, lock=lock, ensure_schema=True)


__all__ = ["BackpackRepairResult", "BackpackRepairService"]
