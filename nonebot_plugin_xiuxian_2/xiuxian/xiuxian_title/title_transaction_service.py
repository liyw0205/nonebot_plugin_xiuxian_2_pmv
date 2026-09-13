"""Compatibility facade for the migrated title transaction use cases."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from threading import RLock
from typing import Any

from ...features.title.application import TitleApplication


@dataclass(frozen=True)
class TitleTransactionResult:
    status: str
    title_id: str = ""

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate", "already_equipped"}


class TitleTransactionService:
    """Preserve the historical positional API while delegating to application."""

    def __init__(self, player_database: str | Path, lock: RLock | None = None) -> None:
        self._application = TitleApplication(player_database)

    @staticmethod
    def _result(outcome: Any) -> TitleTransactionResult:
        data = dict(getattr(outcome, "data", None) or {})
        status = "duplicate" if bool(getattr(outcome, "replayed", False)) else str(data.get("status") or getattr(outcome, "code", "rejected"))
        return TitleTransactionResult(status, str(data.get("title_id") or ""))

    def get_result(self, operation_id: str) -> TitleTransactionResult | None:
        result = self._application.get_result(operation_id)
        if result is None:
            return None
        return TitleTransactionResult(result.status, result.title_id)

    def equip(self, operation_id, user_id, expected_unlocked, expected_equipped, title_id):
        return self._result(self._application.equip(
            operation_id=str(operation_id), user_id=str(user_id),
            expected_unlocked=expected_unlocked, expected_equipped=expected_equipped, title_id=title_id,
        ))

    def unequip(self, operation_id, user_id, expected_equipped):
        return self._result(self._application.unequip(
            operation_id=str(operation_id), user_id=str(user_id), expected_equipped=expected_equipped,
        ))

    def grant(self, operation_id, user_id, expected_unlocked, title_id):
        return self._result(self._application.grant(
            operation_id=str(operation_id), user_id=str(user_id),
            expected_unlocked=expected_unlocked, title_id=title_id,
        ))

    def unlock_batch(self, operation_id, user_id, expected_unlocked, title_ids):
        return self._result(self._application.unlock_batch(
            operation_id=str(operation_id), user_id=str(user_id),
            expected_unlocked=expected_unlocked, title_ids=title_ids,
        ))


__all__ = ["TitleTransactionResult", "TitleTransactionService"]
