"""Stable facade for cross-database mixelixir harvests."""

from .transaction_service import MixelixirHarvestService

# The implementation uses ATTACH DATABASE and BEGIN IMMEDIATE.
OPERATION_TABLE = "mixelixir_harvest_operations"

__all__ = ["MixelixirHarvestService", "OPERATION_TABLE"]
