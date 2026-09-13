"""Stable facade for natal treasure awakening."""

from .transaction_service import AwakenService

# BEGIN IMMEDIATE protects natal_awaken_operations.
OPERATION_TABLE = "natal_awaken_operations"

__all__ = ["AwakenService", "OPERATION_TABLE"]
