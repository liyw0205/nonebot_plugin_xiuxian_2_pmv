"""Stable facade for cross-database Guishi stone transfers."""

from .transaction_service import GuishiStoneService

# The implementation uses ATTACH DATABASE and BEGIN IMMEDIATE.
OPERATION_TABLE = "guishi_stone_operations"

__all__ = ["GuishiStoneService", "OPERATION_TABLE"]
