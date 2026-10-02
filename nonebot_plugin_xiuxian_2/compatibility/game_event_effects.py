from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from ..features.game_events.statistics import GameEventStatisticsRepository
from ..infrastructure.clock import SystemClock
from ..infrastructure.database import DatabaseUnitOfWork, OutboxStore


class LegacyGameEventEffects:
    """Replay-safe projection into the existing game-event adapters."""

    event_type = "game_event.projection"

    def __init__(
        self,
        game_database: str | Path,
        player_database: str | Path,
        *,
        clock: Any | None = None,
        statistics: GameEventStatisticsRepository | None = None,
    ) -> None:
        self.game_database = str(game_database)
        self.clock = clock or SystemClock()
        self.outbox = OutboxStore(clock=self.clock)
        self.statistics = statistics or GameEventStatisticsRepository(player_database)

    def on_outbox_event(self, record: dict[str, Any]) -> None:
        if str(record.get("event_type", "")) != self.event_type:
            raise ValueError("unsupported game event outbox type")
        payload = record.get("payload") or {}
        if not isinstance(payload, dict):
            raise ValueError("game event outbox payload is invalid")
        event_id = str(record.get("event_id", "")).strip()
        if not event_id:
            raise ValueError("game event outbox ID is required")
        from ..xiuxian.xiuxian_utils.game_events import record_game_event

        meta = dict(payload.get("meta") or {})
        meta.update(
            event_id=event_id,
            occurred_at=str(payload["occurred_at"]),
            require_effects=True,
            _statistics_repository=self.statistics,
        )
        if payload.get("stat_increments"):
            meta["stat_increments"] = dict(payload["stat_increments"])
        record_game_event(
            str(payload["user_id"]),
            str(payload["event_key"]),
            int(payload.get("amount", 1)),
            meta,
        )

    def dispatch(self, event_id: str) -> bool:
        event_id = str(event_id)
        with DatabaseUnitOfWork(self.game_database, read_only=True) as uow:
            row = self.outbox.get(uow, event_id)
        if row is None:
            raise RuntimeError("game event outbox receipt is missing")
        if str(row["status"]) == "sent":
            return True
        record = {**dict(row), "payload": json.loads(str(row["payload_json"]))}
        try:
            self.on_outbox_event(record)
            with DatabaseUnitOfWork(self.game_database, immediate=True) as uow:
                self.outbox.mark_sent(uow, event_id)
            return True
        except Exception:
            try:
                with DatabaseUnitOfWork(self.game_database, immediate=True) as uow:
                    self.outbox.mark_failed(uow, event_id)
            except Exception:
                pass
            return False


__all__ = ["LegacyGameEventEffects"]
