from __future__ import annotations

from datetime import datetime
from pathlib import Path

from .._migrated_application import MigratedFeatureApplication
from .bot_overview_repository import BotOverviewSnapshot, BotOverviewSqlRepository
from .repository import StatusRepository


class StatusApplication(MigratedFeatureApplication):
    def __init__(
        self,
        database: str | Path,
        *,
        repository: StatusRepository | None = None,
        trade_database: str | Path | None = None,
        bot_overview_repository: BotOverviewSqlRepository | None = None,
    ) -> None:
        game_database = Path(database)
        resolved_trade_database = trade_database or game_database.with_name("trade.db")
        self.bot_overview_repository = bot_overview_repository or BotOverviewSqlRepository(
            game_database, resolved_trade_database
        )
        super().__init__(database, feature="status", repository=repository or StatusRepository(database))

    def bot_overview(self, *, now: datetime | None = None) -> BotOverviewSnapshot:
        return self.bot_overview_repository.snapshot(now=now)


__all__ = ["StatusApplication"]
