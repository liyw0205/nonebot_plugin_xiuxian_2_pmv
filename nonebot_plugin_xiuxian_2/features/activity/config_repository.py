from __future__ import annotations

from pathlib import Path
from typing import Any, Callable, Mapping

from ...paths import get_paths
from ...xiuxian.xiuxian_activity.config_event_service import (
    ActivityConfigEventService,
    ActivityConfigMutationResult,
    ActivityConfigState,
)


class ActivityConfigSqlRepository:
    """Own versioned Activity configuration in the legacy config database."""

    def __init__(
        self,
        database: str | Path | None = None,
        *,
        event_service: ActivityConfigEventService | None = None,
        config_loader: Callable[[], dict[str, Any]] | None = None,
        projection_writer: Callable[[dict[str, Any]], None] | None = None,
    ) -> None:
        self.database = Path(database or get_paths().data / "activity" / "activity.db")
        self.event_service = event_service or ActivityConfigEventService(self.database)
        self.config_loader = config_loader
        self.projection_writer = projection_writer

    def read(self) -> ActivityConfigState:
        state = self.event_service.read_state()
        if state is not None:
            return state
        if self.config_loader is not None:
            config = self.config_loader()
        else:
            from ...xiuxian.xiuxian_activity.activity_config import (
                _load_config_projection_readonly,
            )

            config = _load_config_projection_readonly()
        return ActivityConfigState(0, config)

    def replay(
        self, operation_id: str, request_identity: Mapping[str, Any]
    ) -> ActivityConfigMutationResult | None:
        result = self.event_service.replay(operation_id, request_identity)
        if result is not None and result.succeeded and result.config is not None:
            self._write_projection(result.config)
        return result

    def replace(
        self,
        operation_id: str,
        request_identity: Mapping[str, Any],
        expected_revision: int,
        config: Mapping[str, Any],
        *,
        result_text: str = "",
    ) -> ActivityConfigMutationResult:
        result = self.event_service.replace(
            operation_id,
            request_identity,
            expected_revision,
            config,
            result_text=result_text,
        )
        if result.succeeded and result.config is not None:
            self._write_projection(result.config)
        return result

    def _write_projection(self, config: dict[str, Any]) -> None:
        if self.projection_writer is not None:
            self.projection_writer(config)
            return
        from ...xiuxian.xiuxian_activity.activity_config import (
            _save_config_projection,
        )

        _save_config_projection(config)


__all__ = ["ActivityConfigSqlRepository"]
