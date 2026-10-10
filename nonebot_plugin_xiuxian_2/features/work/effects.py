"""Post-settlement effects for the Work feature."""

from __future__ import annotations

from typing import Callable


class WorkSettlementEffects:
    """Keep compatibility side effects behind one feature-owned boundary."""

    def __init__(
        self,
        *,
        logger: Callable[[str, str], None] | None = None,
        statistics: Callable[[str, str], object] | None = None,
        progress: Callable[..., object] | None = None,
    ) -> None:
        self._logger = logger
        self._statistics = statistics
        self._progress = progress

    def apply(self, *, user_id: str, message: str, operation_id: str) -> None:
        # Maintenance startup builds the owner without loading command matchers.
        logger, statistics, progress = self._logger, self._statistics, self._progress
        if logger is None or statistics is None:
            from ...xiuxian.xiuxian_utils.utils import log_message, update_statistics_value

            logger = logger or log_message
            statistics = statistics or update_statistics_value
        if progress is None:
            from ...xiuxian.xiuxian_tasks.task_data import record_task_progress

            progress = record_task_progress
        self._logger = logger
        self._statistics = statistics
        self._progress = progress

        self._logger(str(user_id), str(message))
        self._statistics(str(user_id), "悬赏令结算次数")
        self._progress(
            str(user_id),
            "work",
            operation_id=f"task-progress:{operation_id}",
        )


__all__ = ["WorkSettlementEffects"]
