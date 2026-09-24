from __future__ import annotations

from pathlib import Path
from typing import Any

from .query_repository import AuctionQueryRepository, AuctionQuerySqlRepository


class AuctionQueryApplication:
    def __init__(
        self,
        database: str | Path,
        *,
        repository: AuctionQueryRepository | None = None,
    ) -> None:
        self.repository = repository or AuctionQuerySqlRepository(database)

    def get_current_auction(self, auction_id: str | None = None) -> Any:
        return self.repository.get_current_auction(auction_id)

    def count_current_auctions(self) -> int:
        return self.repository.count_current_auctions()

    def get_auction_history(self, auction_id: str | None = None) -> list[dict[str, Any]]:
        return self.repository.get_auction_history(auction_id)

    def get_recent_auction_deals(self, limit: int = 5) -> list[dict[str, Any]]:
        return self.repository.get_recent_auction_deals(limit)

    def count_auction_history(self) -> int:
        return self.repository.count_auction_history()


__all__ = ["AuctionQueryApplication"]
