"""Stable facade for natal treasure reawakening."""

from .transaction_service import ReawakenService

# The implementation uses ATTACH DATABASE and BEGIN IMMEDIATE.
OPERATION_TABLE = "natal_reawaken_operations"

__all__ = ["ReawakenService", "OPERATION_TABLE"]
