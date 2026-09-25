from __future__ import annotations

from pathlib import Path

from .blessed_flag_replace_repository import (
    BlessedFlagReplaceResult,
    BlessedFlagReplaceSqlRepository,
)


class BlessedFlagReplaceApplication:
    """Feature-owned use case for replacing a blessed-spot flag."""

    def __init__(
        self,
        game_database: str | Path,
        player_database: str | Path,
        *,
        repository: BlessedFlagReplaceSqlRepository | None = None,
    ) -> None:
        self.repository = repository or BlessedFlagReplaceSqlRepository(game_database, player_database)

    def replace(self, operation_id: str, user_id: str, *args, **kwargs) -> BlessedFlagReplaceResult:
        return self.repository.replace(operation_id, user_id, *args, **kwargs)


__all__ = ["BlessedFlagReplaceApplication", "BlessedFlagReplaceResult"]
