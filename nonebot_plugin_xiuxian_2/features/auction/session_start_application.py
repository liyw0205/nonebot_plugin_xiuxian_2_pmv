from __future__ import annotations

from pathlib import Path
from typing import Any, Mapping

from ...infrastructure.clock import SystemClock
from ...infrastructure.random_source import SystemRandom
from .session_start_repository import (
    AuctionSessionStartResult,
    AuctionSessionStartSqlRepository,
)


class AuctionSessionStartApplication:
    def __init__(
        self,
        game_database: str | Path,
        trade_database: str | Path,
        *,
        repository: AuctionSessionStartSqlRepository | None = None,
        clock: Any | None = None,
        random_source: Any | None = None,
    ) -> None:
        self.repository = repository or AuctionSessionStartSqlRepository(
            game_database, trade_database
        )
        self.clock = clock or SystemClock()
        self.random_source = random_source or SystemRandom()

    def get_active_session(self) -> dict[str, Any] | None:
        return self.repository.get_active_session()

    def get_start_operation(self, operation_id: str) -> AuctionSessionStartResult | None:
        return self.repository.get_start_operation(operation_id)

    def start(
        self,
        operation_id: str,
        *,
        system_items_config: Mapping[str, Mapping[str, Any]],
        duration_hours: float,
        system_item_count: int = 5,
    ) -> AuctionSessionStartResult:
        operation_id = str(operation_id).strip()
        if not operation_id:
            raise ValueError("operation_id must not be empty")

        previous = self.repository.get_start_operation(operation_id)
        if previous is not None:
            active = self.repository.get_active_session()
            if active and active["session_id"] == previous.session_id:
                return previous
            return AuctionSessionStartResult("state_changed", operation_id)

        now = self.clock.now()
        names = list(system_items_config)
        selected = self.random_source.sample(names, min(max(int(system_item_count), 0), len(names)))
        system_items = [
            {
                "item_id": int(system_items_config[name]["id"]),
                "name": str(name),
                "start_price": int(system_items_config[name]["start_price"]),
            }
            for name in selected
        ]
        start_time = float(now.timestamp())
        end_time = float(start_time + float(duration_hours) * 3600)
        session_id = f"auction:{now.strftime('%Y%m%d%H%M%S')}:{operation_id[-12:]}"
        return self.repository.start(
            operation_id,
            session_id,
            start_time=start_time,
            end_time=end_time,
            system_items=system_items,
        )


__all__ = ["AuctionSessionStartApplication"]
