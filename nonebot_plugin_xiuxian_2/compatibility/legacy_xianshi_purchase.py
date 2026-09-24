"""Explicit rollback adapter for pre-cutover仙肆购买 callers."""

from __future__ import annotations

from uuid import uuid4

from ..xiuxian.xiuxian_trade.repository import TradeRepository, XianshiPurchase


class LegacyXianshiPurchaseService:
    """Compatibility-only wrapper retained for external rollback callers.

    The default handler and the feature compatibility repository call
    ``TradeRepository.purchase_xianshi_item`` directly.
    """

    def __init__(self, repository: TradeRepository) -> None:
        self._repository = repository

    def purchase(
        self,
        buyer_id,
        listing_id,
        quantity,
        *,
        operation_id: str | None = None,
        stamina_operation_id: str | None = None,
        stamina_cost: int = 0,
    ) -> XianshiPurchase:
        operation_id = operation_id or f"xianshi:{listing_id}:{buyer_id}:{uuid4().hex}"
        return self._repository.purchase_xianshi_item(
            operation_id,
            str(buyer_id),
            str(listing_id),
            quantity,
            stamina_operation_id=stamina_operation_id,
            stamina_cost=stamina_cost,
        )


__all__ = ["LegacyXianshiPurchaseService"]
