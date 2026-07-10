from __future__ import annotations

from uuid import uuid4

from .repository import TradeRepository, XianshiPurchase


class XianshiPurchaseService:
    def __init__(self, repository: TradeRepository) -> None:
        self._repository = repository

    def purchase(
        self,
        buyer_id,
        listing_id,
        quantity,
        *,
        operation_id: str | None = None,
    ) -> XianshiPurchase:
        operation_id = operation_id or f"xianshi:{listing_id}:{buyer_id}:{uuid4().hex}"
        return self._repository.purchase_xianshi_item(
            operation_id,
            str(buyer_id),
            str(listing_id),
            quantity,
        )


__all__ = ["XianshiPurchaseService"]
