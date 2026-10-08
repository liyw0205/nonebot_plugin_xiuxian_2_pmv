from __future__ import annotations

from pathlib import Path
from typing import Any, Callable, Mapping

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
        legacy_projection_writer: Callable[[str, Mapping[str, Any]], None] | None = None,
    ) -> None:
        self.database = str(database)
        self.repository = repository or WorkRefreshSqlRepository(database)
        self.legacy_projection_writer = legacy_projection_writer

    def get_result(self, operation_id: str) -> WorkRefreshResult | None:
        return self.repository.get_result(str(operation_id).strip())

    def mark_offer_expired(
        self,
        *,
        user_id: str,
        expected_offer: Mapping[str, Any],
        updated_at: str,
    ) -> WorkRefreshResult:
        return self.repository.mark_offer_expired(
            str(user_id), dict(expected_offer), str(updated_at)
        )

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
            result = self.repository.refresh(
                request.operation_id,
                request.user_id,
                request.expected_count,
                request.expected_cd,
                request.expected_offer,
                request.new_offer,
                request.force,
            )
            if result.status == "applied" and isinstance(result.offer, dict):
                self._project_legacy(request.user_id, result.offer)
            return result

    def _project_legacy(self, user_id: str, offer: Mapping[str, Any]) -> None:
        if self.legacy_projection_writer is not None:
            self.legacy_projection_writer(str(user_id), dict(offer))


__all__ = ["WorkRefreshApplication"]
