from __future__ import annotations

import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

from ..features.buff.closing_statistics import ClosingStatisticsRepository
from ..infrastructure.database import DatabaseUnitOfWork
from ..infrastructure.filesystem import atomic_write


def log_closing_event_once(
    *,
    user_id: str,
    message: str,
    event_id: str,
    occurred_at: str,
    player_database: str | Path,
    players_dir: str | Path,
) -> bool:
    if not Path(player_database).is_file():
        raise RuntimeError("player database is unavailable for closing log projection")
    try:
        when = datetime.fromisoformat(str(occurred_at)).astimezone()
    except ValueError as exc:
        raise ValueError("closing event has an invalid occurred_at") from exc

    legacy_utils = sys.modules.get("nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils.utils")
    impersonating = getattr(legacy_utils, "_impersonating_users", {})
    target_user = str(impersonating.get(str(user_id), user_id))
    root = Path(players_dir)
    logs_dir = root / target_user / "logs"
    log_file = logs_dir / f"{when.strftime('%y%m%d')}.log"
    record = {
        "timestamp": when.strftime("%Y-%m-%d %H:%M:%S"),
        "message": str(message),
        "event_id": str(event_id),
    }
    with DatabaseUnitOfWork(player_database, immediate=True):
        logs_dir.mkdir(parents=True, exist_ok=True)
        existing = json.loads(log_file.read_text(encoding="utf-8")) if log_file.exists() else []
        if not isinstance(existing, list):
            raise ValueError("closing log must contain a JSON list")
        if any(isinstance(item, dict) and str(item.get("event_id", "")) == str(event_id) for item in existing):
            return False
        existing.insert(0, record)
        atomic_write(log_file, json.dumps(existing, ensure_ascii=False, indent=4).encode("utf-8"))
    return True


class LegacyBuffClosingEffects:
    """Replay-safe projection into the established player and activity stores."""

    def __init__(
        self,
        player_database: str | Path,
        *,
        players_dir: str | Path | None = None,
        statistics: ClosingStatisticsRepository | None = None,
    ) -> None:
        from ..paths import get_paths

        self.player_database = str(player_database)
        self.players_dir = Path(players_dir) if players_dir is not None else get_paths().players
        self.statistics = statistics or ClosingStatisticsRepository(player_database)

    @staticmethod
    def _statistics_user(user_id: str) -> str:
        legacy_utils = sys.modules.get("nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils.utils")
        impersonating = getattr(legacy_utils, "_impersonating_users", {})
        return str(impersonating.get(str(user_id), user_id))

    def on_closing_settled(self, *, payload: dict[str, Any], event_id: str) -> None:
        user_id = str(payload["user_id"])
        operation_id = str(payload["operation_id"])
        occurred_at = str(payload["occurred_at"])
        exp_time = int(payload["exp_time"])
        exp_gain = int(payload["exp_gain"])
        stone_cost = int(payload["stone_cost"])

        if self.statistics.record(
            event_id=event_id,
            user_id=self._statistics_user(user_id),
            increments={
                "闭关时长": exp_time,
                "闭关修为": exp_gain,
                "闭关灵石消耗": stone_cost,
            },
            occurred_at=occurred_at,
        ):
            legacy_utils = sys.modules.get("nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils.utils")
            invalidate = getattr(legacy_utils, "invalidate_player_data_cache", None)
            if callable(invalidate):
                invalidate("statistics", ("闭关时长", "闭关修为", "闭关灵石消耗"))

        log_closing_event_once(
            user_id=user_id,
            message=f"[出关] 闭关{exp_time}分钟，消耗灵石{stone_cost}，获得修为{exp_gain}",
            event_id=f"{event_id}:log",
            occurred_at=occurred_at,
            player_database=self.player_database,
            players_dir=self.players_dir,
        )

        if exp_time > 0:
            from ..xiuxian.xiuxian_tasks.task_data import record_task_progress_event_strict

            record_task_progress_event_strict(
                user_id,
                (("out_closing", exp_time),),
                operation_id=f"task-progress:{operation_id}",
                occurred_at=occurred_at,
            )
            from ..xiuxian.xiuxian_activity.service import record_activity_event

            record_activity_event(
                user_id,
                "out_closing",
                exp_time,
                event_id=f"{event_id}:activity:out_closing",
                occurred_at=occurred_at,
            )


__all__ = ["LegacyBuffClosingEffects", "log_closing_event_once"]
