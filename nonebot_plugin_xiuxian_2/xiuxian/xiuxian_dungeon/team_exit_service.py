from __future__ import annotations

from .team_transaction_service import (
    DungeonTeamTransactionService,
    TeamExitResult,
    TeamStateSnapshot,
)


class DungeonTeamExitService(DungeonTeamTransactionService):
    """Transactional owner for member leave, kick, and team disband flows."""


__all__ = ["DungeonTeamExitService", "TeamExitResult", "TeamStateSnapshot"]
