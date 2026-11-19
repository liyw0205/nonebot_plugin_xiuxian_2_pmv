from __future__ import annotations

from dataclasses import replace
from pathlib import Path

from .demon_token_repository import (
    RiftDemonTokenBattleResult,
    RiftDemonTokenBattleSqlRepository,
)


RiftKeyEventResult = RiftDemonTokenBattleResult


class RiftKeyEventSqlRepository(RiftDemonTokenBattleSqlRepository):
    """Persist a pre-rolled key event using the legacy-compatible payload."""

    operation_table = "rift_key_event_operations"

    def __init__(self, game_database: str | Path, player_database: str | Path, *, clock=None) -> None:
        super().__init__(game_database, player_database, clock=clock)

    def settle(self, *args, **kwargs):
        result = super().settle(*args, **kwargs)
        reason = str(result.reason or "")
        if reason.startswith("rift_demon_token_"):
            result = replace(
                result,
                reason=reason.replace("rift_demon_token_", "rift_key_event_", 1),
            )
        return result


__all__ = ["RiftKeyEventResult", "RiftKeyEventSqlRepository"]
