from __future__ import annotations

from pathlib import Path
from typing import Any, Mapping

from ...core.errors import ValidationError
from ...infrastructure.observability import trace_context
from .domain import WorkRefreshRequest
from .refresh_repository import WorkRefreshResult, WorkRefreshSqlRepository
from .repository import WorkRefreshRepository


class WorkRefreshApplication:
    """Own the request boundary for ordinary and forced work refreshes."""

    def __init__(
        self,
        database: str | Path,
        *,
        repository: WorkRefreshRepository | None = None,
    ) -> None:
        self.database = str(database)
        self.repository = repository or WorkRefreshSqlRepository(database)

    def get_result(self, operation_id: str) -> WorkRefreshResult | None:
        return self.repository.get_result(str(operation_id).strip())

    def refresh(
        self,
        *,
        operation_id: str,
        user_id: str,
        expected_count: int,
        expected_cd: Mapping[str, Any],
        expected_offer: Mapping[str, Any] | None,
        new_offer: Mapping[str, Any],
        force: bool = False,
    ) -> WorkRefreshResult:
        try:
            request = WorkRefreshRequest(
                str(operation_id).strip(),
                str(user_id).strip(),
                int(expected_count),
                dict(expected_cd or {}),
                dict(expected_offer) if expected_offer is not None else None,
                dict(new_offer or {}),
                bool(force),
            )
            request.validate()
        except (TypeError, ValueError) as exc:
            raise ValidationError(str(exc)) from exc

        with trace_context(operation_id=request.operation_id, user_scope=request.user_id):
            return self.repository.refresh(
                request.operation_id,
                request.user_id,
                request.expected_count,
                request.expected_cd,
                request.expected_offer,
                request.new_offer,
                request.force,
            )


__all__ = ["WorkRefreshApplication"]
