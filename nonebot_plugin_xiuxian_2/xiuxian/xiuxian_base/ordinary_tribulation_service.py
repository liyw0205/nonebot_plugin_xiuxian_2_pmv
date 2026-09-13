"""Stable facade for ordinary tribulation settlement."""

from .transaction_service import OrdinaryTribulationService

# BEGIN IMMEDIATE and replay protect ordinary tribulation operations.
OPERATION_TABLE = "ordinary_tribulation_operations"
# def replay(operation_id, user_id) is provided by the implementation.

__all__ = ["OrdinaryTribulationService", "OPERATION_TABLE"]
