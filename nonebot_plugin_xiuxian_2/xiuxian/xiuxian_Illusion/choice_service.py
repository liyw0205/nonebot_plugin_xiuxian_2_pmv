"""Compatibility facade for the migrated illusion choice repository.

The historical import path remains available for old handlers and tests, but
all storage and transaction work now lives under ``features.illusion``.
The migrated repository still maintains ``illusion_choice_operations`` inside
the ``BEGIN IMMEDIATE`` transaction used by the historical API.
"""

from __future__ import annotations

from pathlib import Path
from threading import RLock
from typing import Any, Mapping

from ...features.illusion.domain import IllusionChoiceResult, period_key
from ...features.illusion.repository import IllusionRepository
from ...infrastructure.database import DatabaseUnitOfWork


class IllusionChoiceService:
    """Preserve the historical method signatures during the compatibility cycle."""

    def __init__(self, database: str | Path, lock: RLock | None = None) -> None:
        self._database = str(database)
        self._lock = lock or RLock()
        self._repository = IllusionRepository()

    @staticmethod
    def period_key(now=None) -> str:
        return period_key(now)

    def get_result(self, operation_id: str) -> IllusionChoiceResult | None:
        with self._lock, DatabaseUnitOfWork(self._database) as uow:
            return self._repository.get_result(uow, operation_id)

    def choose(
        self,
        operation_id: str,
        user_id: str,
        period: str,
        question_index: int,
        choice_index: int,
        selected_option: str,
        stone: int,
        exp: int,
        item: Mapping[str, Any] | None,
        max_goods_num: int,
    ) -> IllusionChoiceResult:
        with self._lock, DatabaseUnitOfWork(self._database, immediate=True) as uow:
            return self._repository.choose(
                uow,
                operation_id=operation_id,
                user_id=user_id,
                period=period,
                question_index=question_index,
                choice_index=choice_index,
                selected_option=selected_option,
                stone=stone,
                exp=exp,
                item=item,
                max_goods_num=max_goods_num,
            )

    def get_choice(self, user_id: str, period: str) -> dict[str, Any] | None:
        with self._lock, DatabaseUnitOfWork(self._database) as uow:
            return self._repository.get_choice(uow, user_id, period)


__all__ = ["IllusionChoiceResult", "IllusionChoiceService"]
