from __future__ import annotations

from pathlib import Path
from typing import Any, Mapping

from ...core.errors import ValidationError
from ...infrastructure.observability import trace_context
from .abort_cleanup_repository import WorkAbortCleanupResult, WorkAbortCleanupSqlRepository
from .domain import WorkAbortCleanupRequest
from .repository import WorkAbortCleanupRepository


class WorkAbortCleanupApplication:
    """Validate abort/reset intent before invoking the atomic SQL boundary."""

    def __init__(
        self,
        database: str | Path,
        *,
        repository: WorkAbortCleanupRepository | None = None,
    ) -> None:
        self.repository = repository or WorkAbortCleanupSqlRepository(database)

    def cleanup(
        self,
        operation_id: str,
        user_id: str,
        reason: str,
        expected_cd: Mapping[str, Any],
        expected_offer: Mapping[str, Any] | None = None,
        expected_stone: int | None = None,
        penalty: int = 0,
    ) -> WorkAbortCleanupResult:
        try:
            request = WorkAbortCleanupRequest(
                str(operation_id).strip(),
                str(user_id),
                str(reason).strip(),
                dict(expected_cd or {}),
                dict(expected_offer) if expected_offer else None,
                int(expected_stone) if expected_stone is not None else None,
                int(penalty),
            )
            request.validate()
        except (TypeError, ValueError) as exc:
            raise ValidationError(str(exc)) from exc

        with trace_context(operation_id=request.operation_id, user_scope=request.user_id):
            return self.repository.cleanup(
                request.operation_id,
                request.user_id,
                request.reason,
                request.expected_cd,
                request.expected_offer,
                request.expected_stone,
                request.penalty,
            )


__all__ = ["WorkAbortCleanupApplication"]
