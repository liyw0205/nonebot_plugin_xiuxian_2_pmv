"""Stable facade for breakthrough-rate item transactions."""

from .transaction_service import BreakthroughRateItemService

# BEGIN IMMEDIATE protects the idempotent breakthrough_rate_item_operations ledger.
OPERATION_TABLE = "breakthrough_rate_item_operations"

__all__ = ["BreakthroughRateItemService", "OPERATION_TABLE"]
