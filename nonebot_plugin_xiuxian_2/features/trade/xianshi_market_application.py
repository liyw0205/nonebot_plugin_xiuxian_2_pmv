"""Use-case orchestration for Xianshi market quotes and the watch list."""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from .xianshi_market_repository import (
    XianshiMarketQuote,
    XianshiMarketSqlRepository,
)
from .xianshi_market_schemas import MARKET_WINDOW_DAYS, WATCH_LIMIT, name_key
from .xianshi_watch_repository import (
    XianshiWatchResult,
    XianshiWatchSqlRepository,
)
from ...infrastructure.clock import SystemClock


@dataclass(frozen=True)
class XianshiWatchOverview:
    names: tuple[str, ...]
    quotes: dict[str, XianshiMarketQuote] = field(default_factory=dict)
    limit: int = WATCH_LIMIT

    def quote_for(self, name: str) -> XianshiMarketQuote:
        key = name_key(name)
        return self.quotes.get(
            key, XianshiMarketQuote(name, trades_ready=False, listings_ready=False)
        )


class XianshiMarketApplication:
    """Player-facing market information over the existing Xianshi ledgers."""

    def __init__(
        self,
        game_database: str | Path,
        *,
        watch_repository: XianshiWatchSqlRepository | None = None,
        market_repository: XianshiMarketSqlRepository | None = None,
        clock: Any | None = None,
    ) -> None:
        self.game_database = Path(game_database)
        self.clock = clock or SystemClock()
        self.watch_repository = watch_repository or XianshiWatchSqlRepository(
            self.game_database, clock=self.clock
        )
        self.market_repository = market_repository or XianshiMarketSqlRepository(
            self.game_database, clock=self.clock
        )

    # ------------------------------------------------------------------ writes
    def watch(self, *, operation_id: str, user_id: str, goods_name: str) -> XianshiWatchResult:
        return self.watch_repository.add(operation_id, user_id, goods_name)

    def unwatch(self, *, operation_id: str, user_id: str, goods_name: str) -> XianshiWatchResult:
        return self.watch_repository.remove(operation_id, user_id, goods_name)

    # ------------------------------------------------------------------- reads
    def names(self, user_id: str) -> tuple[str, ...]:
        try:
            return tuple(self.watch_repository.names(user_id))
        except sqlite3.Error:
            return ()

    def quote(
        self,
        goods_name: str,
        *,
        days: int = MARKET_WINDOW_DAYS,
    ) -> XianshiMarketQuote:
        name = str(goods_name or "").strip()
        try:
            snapshot = self.market_repository.snapshot([name], days=days)
        except sqlite3.Error:
            return XianshiMarketQuote(name, trades_ready=False, listings_ready=False)
        return snapshot.get(
            name_key(name),
            XianshiMarketQuote(name, trades_ready=False, listings_ready=False),
        )

    def overview(
        self,
        user_id: str,
        *,
        days: int = MARKET_WINDOW_DAYS,
    ) -> XianshiWatchOverview:
        names = self.names(user_id)
        if not names:
            return XianshiWatchOverview(names=())
        try:
            quotes = self.market_repository.snapshot(names, days=days)
        except sqlite3.Error:
            quotes = {
                name_key(name): XianshiMarketQuote(
                    name, trades_ready=False, listings_ready=False
                )
                for name in names
            }
        return XianshiWatchOverview(names=names, quotes=quotes, limit=WATCH_LIMIT)


__all__ = ["XianshiMarketApplication", "XianshiWatchOverview"]
