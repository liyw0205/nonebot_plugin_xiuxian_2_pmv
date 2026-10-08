from __future__ import annotations

from datetime import datetime
from pathlib import Path

from .._migrated_application import MigratedFeatureApplication
from .bot_overview_repository import BotOverviewSnapshot, BotOverviewSqlRepository
from .dashboard_stats_repository import DashboardStatsSqlRepository
from .ping_probe import PingProbe
from .process_info import ProcessInfoProvider
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
        message_database: str | Path | None = None,
        dashboard_stats_repository: DashboardStatsSqlRepository | None = None,
        ping_probe: PingProbe | None = None,
        system_info_provider: SystemInfoProvider | None = None,
        process_info_provider: ProcessInfoProvider | None = None,
    ) -> None:
        game_database = Path(database)
        resolved_trade_database = trade_database or game_database.with_name("trade.db")
        resolved_message_database = message_database or game_database.with_name("message.db")
        self.bot_overview_repository = bot_overview_repository or BotOverviewSqlRepository(
            game_database, resolved_trade_database
        )
        self.dashboard_stats_repository = dashboard_stats_repository or DashboardStatsSqlRepository(
            game_database, resolved_message_database
        )
        self.ping_probe = ping_probe or PingProbe()
        self.system_info_provider = system_info_provider or SystemInfoProvider()
        self.process_info_provider = process_info_provider or ProcessInfoProvider()
        super().__init__(database, feature="status", repository=repository or StatusRepository(database))

    def bot_overview(self, *, now: datetime | None = None) -> BotOverviewSnapshot:
        return self.bot_overview_repository.snapshot(now=now)

    def dashboard_stats(self, *, now: datetime | None = None) -> dict[str, int]:
        """Return legacy dashboard counters from game and message databases."""
        return self.dashboard_stats_repository.snapshot(now=now).as_dict()

    async def ping_test(self) -> str:
        """Return the fixed-site latency report without ledger or database writes."""
        return (await self.ping_probe.probe_all()).render()

    def system_info(self) -> SystemInfoSnapshot:
        """Read host metrics without touching feature persistence."""
        return self.system_info_provider.snapshot()

    @property
    def process_info_available(self) -> bool:
        return self.process_info_provider.available

    def process_info(self, limit: int = 5) -> list[dict]:
        """Return the largest processes using the legacy dashboard row shape."""
        return self.process_info_provider.snapshot(limit)


__all__ = ["StatusApplication"]
