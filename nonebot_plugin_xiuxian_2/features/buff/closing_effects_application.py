from __future__ import annotations

import sys
from pathlib import Path
from typing import Any, Callable

from .closing_log import log_closing_event_once
from .closing_statistics import ClosingStatisticsRepository


def _legacy_impersonated_user(user_id: str) -> str:
    legacy_utils = sys.modules.get("nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils.utils")
    impersonating = getattr(legacy_utils, "_impersonating_users", {})
    return str(impersonating.get(str(user_id), user_id))


def _default_task_progress(user_id: str, events, *, operation_id: str, occurred_at: str) -> Any:
    from ...xiuxian.xiuxian_tasks.task_data import record_task_progress_event_strict

    return record_task_progress_event_strict(
        user_id,
        events,
        operation_id=operation_id,
        occurred_at=occurred_at,
    )


def _default_activity_event(
    user_id: str,
    event_key: str,
    amount: int,
    *,
    event_id: str,
    occurred_at: str,
) -> Any:
    from ...xiuxian.xiuxian_activity.service import record_activity_event

    return record_activity_event(
        user_id,
        event_key,
        amount,
        event_id=event_id,
        occurred_at=occurred_at,
    )


def _default_invalidate_cache(*_args: Any) -> None:
    legacy_utils = sys.modules.get("nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils.utils")
    invalidate = getattr(legacy_utils, "invalidate_player_data_cache", None)
    if callable(invalidate):
        invalidate("statistics", ("闭关时长", "闭关修为", "闭关灵石消耗"))


class ClosingEffectsApplication:
    """Dispatch replay-safe post-settlement projections for ordinary closing."""

    def __init__(
        self,
        player_database: str | Path,
        *,
        players_dir: str | Path | None = None,
        statistics: ClosingStatisticsRepository | None = None,
        log_event: Callable[..., bool] | None = None,
        task_progress: Callable[..., Any] | None = None,
        activity_event: Callable[..., Any] | None = None,
        invalidate_cache: Callable[..., Any] | None = None,
    ) -> None:
        from ...paths import get_paths

        self.player_database = str(player_database)
        self.players_dir = Path(players_dir) if players_dir is not None else get_paths().players
        self.statistics = statistics or ClosingStatisticsRepository(player_database)
        self._log_event = log_event or log_closing_event_once
        self._task_progress = task_progress or _default_task_progress
        self._activity_event = activity_event or _default_activity_event
        self._invalidate_cache = invalidate_cache or _default_invalidate_cache

    def on_closing_settled(self, *, payload: dict[str, Any], event_id: str) -> None:
        """Apply every projection using its own stable event/operation receipt."""
        user_id = str(payload["user_id"])
        operation_id = str(payload["operation_id"])
        occurred_at = str(payload["occurred_at"])
        exp_time = int(payload["exp_time"])
        exp_gain = int(payload["exp_gain"])
        stone_cost = int(payload["stone_cost"])

        if self.statistics.record(
            event_id=event_id,
            user_id=_legacy_impersonated_user(user_id),
            increments={
                "闭关时长": exp_time,
                "闭关修为": exp_gain,
                "闭关灵石消耗": stone_cost,
            },
            occurred_at=occurred_at,
        ):
            self._invalidate_cache()

        self._log_event(
            user_id=user_id,
            message=f"[出关] 闭关{exp_time}分钟，消耗灵石{stone_cost}，获得修为{exp_gain}",
            event_id=f"{event_id}:log",
            occurred_at=occurred_at,
            player_database=self.player_database,
            players_dir=self.players_dir,
        )

        if exp_time <= 0:
            return
        self._task_progress(
            user_id,
            (("out_closing", exp_time),),
            operation_id=f"task-progress:{operation_id}",
            occurred_at=occurred_at,
        )
        self._activity_event(
            user_id,
            "out_closing",
            exp_time,
            event_id=f"{event_id}:activity:out_closing",
            occurred_at=occurred_at,
        )


__all__ = ["ClosingEffectsApplication"]
