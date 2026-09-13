from __future__ import annotations

from pathlib import Path
from typing import Any, Protocol


class SectFairylandRepository(Protocol):
    def claim(self, operation_id: str, user_id: str, sect_id: str, day: str, level: int, minutes: int) -> Any: ...


class LegacySectFairylandRepository:
    def __init__(self, player_database: str | Path) -> None:
        self.player_database = str(player_database)

    def claim(self, operation_id: str, user_id: str, sect_id: str, day: str, level: int, minutes: int) -> Any:
        from ...xiuxian.xiuxian_sect.transaction_service import FairylandClaimService

        return FairylandClaimService(self.player_database).claim(
            operation_id, user_id, sect_id, day, level, minutes
        )


__all__ = ["LegacySectFairylandRepository", "SectFairylandRepository"]
