from __future__ import annotations

from pathlib import Path
from typing import Any

from .xiangyuan_repository import XiangyuanSqlRepository


class XiangyuanApplication:
    """Application boundary for create/claim/list/clear xiangyuan flows."""

    def __init__(
        self,
        game_database: str | Path,
        player_database: str | Path,
        *,
        repository: XiangyuanSqlRepository | None = None,
        clock: Any | None = None,
    ) -> None:
        self.repository = repository or XiangyuanSqlRepository(
            game_database, player_database, clock=clock
        )

    def create(self, *args: Any, **kwargs: Any):
        return self.repository.create(*args, **kwargs)

    def claim(self, *args: Any, **kwargs: Any):
        return self.repository.claim(*args, **kwargs)

    def get_group(self, *args: Any, **kwargs: Any):
        return self.repository.get_group(*args, **kwargs)

    def clear_all(self, *args: Any, **kwargs: Any):
        return self.repository.clear_all(*args, **kwargs)

    def reset_limits(self):
        return self.repository.reset_limits()


__all__ = ["XiangyuanApplication"]
