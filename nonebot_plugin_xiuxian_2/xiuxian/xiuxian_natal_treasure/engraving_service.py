"""Stable facade for natal treasure engraving."""

from .transaction_service import EngravingService

# The implementation uses ATTACH DATABASE and BEGIN IMMEDIATE.
OPERATION_TABLE = "natal_engraving_operations"

__all__ = ["EngravingService", "OPERATION_TABLE"]
