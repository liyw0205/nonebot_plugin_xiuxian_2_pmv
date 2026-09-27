from __future__ import annotations

from pathlib import Path
from typing import Any, Protocol


class SectFairylandRepository(Protocol):
    def claim(self, operation_id: str, user_id: str, sect_id: str, day: str, level: int, minutes: int) -> Any: ...
    def get_last_claim_day(self, user_id: str, sect_id: str) -> str: ...


class LegacySectFairylandRepository:
    def __init__(self, player_database: str | Path) -> None:
        self.player_database = str(player_database)

    def claim(self, operation_id: str, user_id: str, sect_id: str, day: str, level: int, minutes: int) -> Any:
        from ...compatibility.legacy_sect_fairyland_claim import FairylandClaimService

        return FairylandClaimService(self.player_database).claim(
            operation_id, user_id, sect_id, day, level, minutes
        )

    def get_last_claim_day(self, user_id: str, sect_id: str) -> str:
        from .claim_repository import SectFairylandSqlRepository

        return SectFairylandSqlRepository(self.player_database).get_last_claim_day(user_id, sect_id)


__all__ = ["LegacySectFairylandRepository", "SectFairylandRepository"]
