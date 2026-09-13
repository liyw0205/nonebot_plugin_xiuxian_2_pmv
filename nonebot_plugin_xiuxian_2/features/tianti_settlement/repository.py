from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any, Protocol


class TiantiSettlementRepository(Protocol):
    def settle(self, operation_id: str, user_id: str, settled_at: datetime, *, sect_fairyland_level: int = 0) -> Any: ...


class LegacyTiantiSettlementRepository:
    """Lazy adapter around the historical tianti player-db transaction."""

    def __init__(self, player_database: str | Path) -> None:
        self.player_database = str(player_database)

    def settle(self, operation_id: str, user_id: str, settled_at: datetime, *, sect_fairyland_level: int = 0) -> Any:
        try:
            from ...xiuxian.xiuxian_tianti.transaction_service import TiantiSettlementService
        except (ImportError, RuntimeError, ValueError):
            return {"status": "not_ready", "detail": {}}
        return TiantiSettlementService(self.player_database).settle(
            operation_id,
            user_id,
            settled_at,
            sect_fairyland_level=sect_fairyland_level,
        )


__all__ = ["LegacyTiantiSettlementRepository", "TiantiSettlementRepository"]
