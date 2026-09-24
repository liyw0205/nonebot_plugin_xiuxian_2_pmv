"""Explicit startup adapter for importing legacy trade tables.

The old ``TradeRepository`` remains available for data migration only. It is
never constructed by the trade command facade or used by feature transactions.
"""

from __future__ import annotations

from pathlib import Path


class LegacyXianshiSchemaAdapter:
    """Copy legacy trade projections into the feature-owned game database."""

    def __init__(self, game_database: str | Path, *, max_goods_num: int) -> None:
        self.game_database = str(game_database)
        self.max_goods_num = max(1, int(max_goods_num))

    def initialize(self, legacy_database: str | Path) -> None:
        from ..xiuxian.xiuxian_trade.repository import TradeRepository

        TradeRepository(
            self.game_database,
            max_goods_num=self.max_goods_num,
        ).initialize(legacy_database)


__all__ = ["LegacyXianshiSchemaAdapter"]
