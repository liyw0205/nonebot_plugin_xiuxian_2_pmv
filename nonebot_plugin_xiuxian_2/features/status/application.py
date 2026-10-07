from __future__ import annotations

from datetime import datetime
from pathlib import Path

from .._migrated_application import MigratedFeatureApplication
from .bot_overview_repository import BotOverviewSnapshot, BotOverviewSqlRepository
from .ping_probe import PingProbe
from .repository import StatusRepository
from .system_info import SystemInfoProvider, SystemInfoSnapshot


class StatusApplication(MigratedFeatureApplication):
    def __init__(
        self,
        database: str | Path,
        *,
        repository: StatusRepository | None = None,
        trade_database: str | Path | None = None,
        bot_overview_repository: BotOverviewSqlRepository | None = None,
        ping_probe: PingProbe | None = None,
        system_info_provider: SystemInfoProvider | None = None,
    ) -> None:
        game_database = Path(database)
        resolved_trade_database = trade_database or game_database.with_name("trade.db")
        self.bot_overview_repository = bot_overview_repository or BotOverviewSqlRepository(
            game_database, resolved_trade_database
        )
        self.ping_probe = ping_probe or PingProbe()
        self.system_info_provider = system_info_provider or SystemInfoProvider()
        super().__init__(database, feature="status", repository=repository or StatusRepository(database))

    def bot_overview(self, *, now: datetime | None = None) -> BotOverviewSnapshot:
        return self.bot_overview_repository.snapshot(now=now)

    async def ping_test(self) -> str:
        """Return the fixed-site latency report without ledger or database writes."""
        return (await self.ping_probe.probe_all()).render()

    def system_info(self) -> SystemInfoSnapshot:
        """Read host metrics without touching feature persistence."""
        return self.system_info_provider.snapshot()


__all__ = ["StatusApplication"]
