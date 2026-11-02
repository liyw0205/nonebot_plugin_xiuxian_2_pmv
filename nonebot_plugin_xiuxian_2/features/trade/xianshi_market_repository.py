"""Read-only Xianshi market aggregation over the existing trade ledgers.

Two sources, both already written by the live buy/list paths:

* ``xianshi_operations`` — one row per completed purchase (unit price is
  ``total_cost / quantity``; the listing row stores a per-unit ``price``).
* ``xianshi_item`` — the current listings projection.

The repository never creates schema. A missing database or table is reported
per source so the renderer can say "unavailable" instead of "empty".
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path
from typing import Any, Iterable

from ...infrastructure.clock import SystemClock
from ...infrastructure.database import DatabaseUnitOfWork
from .xianshi_market_schemas import (
    MARKET_WINDOW_DAYS,
    WATCH_SNAPSHOT_LIMIT,
    name_key,
    normalize_goods_name,
)


@dataclass(frozen=True)
class XianshiMarketQuote:
    """Aggregated trade and listing numbers for one item name."""

    name: str
    trades_ready: bool = True
    listings_ready: bool = True
    trade_count: int = 0
    traded_quantity: int = 0
    min_unit_price: int = 0
    max_unit_price: int = 0
    avg_unit_price: int = 0
    last_traded_at: str = ""
    last_unit_price: int = 0
    listing_count: int = 0
    listing_quantity: int = 0
    seller_count: int = 0
    min_listing_price: int = 0

    @property
    def available(self) -> bool:
        return self.trades_ready or self.listings_ready

    @property
    def has_trades(self) -> bool:
        return self.trade_count > 0

    @property
    def has_listings(self) -> bool:
        return self.listing_count > 0


def _round_half_up(numerator: int, denominator: int) -> int:
    """Deterministic integer rounding for positive money/quantity ratios."""

    if denominator <= 0 or numerator <= 0:
        return 0
    return (numerator * 2 + denominator) // (2 * denominator)


def _unit_price(total_cost: int, quantity: int) -> int:
    return _round_half_up(int(total_cost), int(quantity))


class XianshiMarketSqlRepository:
    """Aggregate trades and listings for a bounded set of item names."""

    def __init__(self, database: str | Path, *, clock: Any | None = None) -> None:
        self.database = Path(database)
        self.clock = clock or SystemClock()

    @staticmethod
    def _has_table(uow: DatabaseUnitOfWork, name: str) -> bool:
        return (
            uow.query_one(
                "SELECT 1 AS present FROM sqlite_master WHERE type='table' AND name=?",
                (name,),
            )
            is not None
        )

    def _window_start(self, days: int) -> str:
        return (self.clock.now() - timedelta(days=max(int(days), 1))).strftime(
            "%Y-%m-%d %H:%M:%S"
        )

    def _empty(self, names: Iterable[str], *, trades_ready: bool, listings_ready: bool):
        return {
            name_key(name): XianshiMarketQuote(
                normalize_goods_name(name),
                trades_ready=trades_ready,
                listings_ready=listings_ready,
            )
            for name in names
        }

    def snapshot(
        self,
        names: Iterable[str],
        *,
        days: int = MARKET_WINDOW_DAYS,
    ) -> dict[str, XianshiMarketQuote]:
        """Return one quote per requested name, keyed by case-insensitive name."""

        cleaned: list[str] = []
        seen: set[str] = set()
        for raw in names:
            name = normalize_goods_name(raw)
            key = name_key(name)
            if not key or key in seen:
                continue
            seen.add(key)
            cleaned.append(name)
            if len(cleaned) >= WATCH_SNAPSHOT_LIMIT:
                break
        if not cleaned:
            return {}

        if not self.database.is_file():
            return self._empty(cleaned, trades_ready=False, listings_ready=False)

        quotes: dict[str, XianshiMarketQuote] = {}
        try:
            with DatabaseUnitOfWork(self.database, read_only=True) as uow:
                trades_ready = self._has_table(uow, "xianshi_operations")
                listings_ready = self._has_table(uow, "xianshi_item")
                quotes = self._empty(
                    cleaned, trades_ready=trades_ready, listings_ready=listings_ready
                )
                if trades_ready:
                    self._collect_trades(uow, quotes, days=days)
                if listings_ready:
                    self._collect_listings(uow, quotes)
        except sqlite3.Error:
            return self._empty(cleaned, trades_ready=False, listings_ready=False)
        return quotes

    def _requested_keys(self, quotes: dict[str, XianshiMarketQuote]) -> list[str]:
        return list(quotes)

    def _collect_trades(
        self,
        uow: DatabaseUnitOfWork,
        quotes: dict[str, XianshiMarketQuote],
        *,
        days: int,
    ) -> None:
        keys = self._requested_keys(quotes)
        placeholders = ",".join("?" for _ in keys)
        # Lower bound only: created_at mixes UTC and local time across writers,
        # so an upper bound would drop rows written "just now" by local clocks.
        rows = uow.query_all(
            "SELECT name,quantity,total_cost,created_at FROM xianshi_operations "
            f"WHERE created_at>=? AND LOWER(name) IN ({placeholders})",
            (self._window_start(days), *keys),
        )
        buckets: dict[str, dict[str, Any]] = {}
        for row in rows:
            key = name_key(row["name"])
            if key not in quotes:
                continue
            try:
                quantity = int(row["quantity"] or 0)
                total_cost = int(row["total_cost"] or 0)
            except (TypeError, ValueError):
                continue
            created_at = str(row["created_at"] or "").strip()
            if quantity <= 0 or total_cost < 0 or not created_at:
                continue
            unit = _unit_price(total_cost, quantity)
            bucket = buckets.setdefault(
                key,
                {
                    "count": 0,
                    "quantity": 0,
                    "cost": 0,
                    "min": unit,
                    "max": unit,
                    "last_at": "",
                    "last_price": 0,
                },
            )
            bucket["count"] += 1
            bucket["quantity"] += quantity
            bucket["cost"] += total_cost
            bucket["min"] = min(int(bucket["min"]), unit)
            bucket["max"] = max(int(bucket["max"]), unit)
            if created_at > str(bucket["last_at"]):
                bucket["last_at"] = created_at
                bucket["last_price"] = unit

        for key, bucket in buckets.items():
            base = quotes[key]
            quotes[key] = XianshiMarketQuote(
                base.name,
                trades_ready=base.trades_ready,
                listings_ready=base.listings_ready,
                trade_count=int(bucket["count"]),
                traded_quantity=int(bucket["quantity"]),
                min_unit_price=int(bucket["min"]),
                max_unit_price=int(bucket["max"]),
                avg_unit_price=_round_half_up(int(bucket["cost"]), int(bucket["quantity"])),
                last_traded_at=str(bucket["last_at"]),
                last_unit_price=int(bucket["last_price"]),
                listing_count=base.listing_count,
                listing_quantity=base.listing_quantity,
                seller_count=base.seller_count,
                min_listing_price=base.min_listing_price,
            )

    def _collect_listings(
        self,
        uow: DatabaseUnitOfWork,
        quotes: dict[str, XianshiMarketQuote],
    ) -> None:
        keys = self._requested_keys(quotes)
        placeholders = ",".join("?" for _ in keys)
        rows = uow.query_all(
            "SELECT user_id,name,price,quantity FROM xianshi_item "
            f"WHERE LOWER(name) IN ({placeholders})",
            tuple(keys),
        )
        buckets: dict[str, dict[str, Any]] = {}
        for row in rows:
            key = name_key(row["name"])
            if key not in quotes:
                continue
            try:
                price = int(row["price"] or 0)
                quantity = int(row["quantity"] or 0)
            except (TypeError, ValueError):
                continue
            if price <= 0 or quantity <= 0:
                continue
            bucket = buckets.setdefault(
                key, {"count": 0, "quantity": 0, "min": price, "sellers": set()}
            )
            bucket["count"] += 1
            bucket["quantity"] += quantity
            bucket["min"] = min(int(bucket["min"]), price)
            bucket["sellers"].add(str(row["user_id"] or ""))

        for key, bucket in buckets.items():
            base = quotes[key]
            quotes[key] = XianshiMarketQuote(
                base.name,
                trades_ready=base.trades_ready,
                listings_ready=base.listings_ready,
                trade_count=base.trade_count,
                traded_quantity=base.traded_quantity,
                min_unit_price=base.min_unit_price,
                max_unit_price=base.max_unit_price,
                avg_unit_price=base.avg_unit_price,
                last_traded_at=base.last_traded_at,
                last_unit_price=base.last_unit_price,
                listing_count=int(bucket["count"]),
                listing_quantity=int(bucket["quantity"]),
                seller_count=len(bucket["sellers"]),
                min_listing_price=int(bucket["min"]),
            )


__all__ = ["XianshiMarketQuote", "XianshiMarketSqlRepository"]
