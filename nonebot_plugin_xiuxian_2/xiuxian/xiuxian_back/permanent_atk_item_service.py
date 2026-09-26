"""Stable facade for permanent attack item transactions."""

from ...compatibility.legacy_back_permanent_atk_item import PermanentAtkItemService

# BEGIN IMMEDIATE protects the idempotent permanent_atk_item_operations ledger.
OPERATION_TABLE = "permanent_atk_item_operations"

__all__ = ["PermanentAtkItemService", "OPERATION_TABLE"]
