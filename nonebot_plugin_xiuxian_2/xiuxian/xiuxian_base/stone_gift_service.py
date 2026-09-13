"""Stable facade for stone gift transfers."""

from .transaction_service import StoneGiftService

# BEGIN IMMEDIATE protects the idempotent stone_gift_operations ledger.
OPERATION_TABLE = "stone_gift_operations"

__all__ = ["StoneGiftService", "OPERATION_TABLE"]
