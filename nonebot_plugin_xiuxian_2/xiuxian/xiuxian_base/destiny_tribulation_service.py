"""Stable facade for destiny tribulation replay and settlement."""

from .transaction_service import DestinyTribulationService

# def replay(operation_id, user_id) is provided by the implementation.
OPERATION_TABLE = "destiny_tribulation_operations"

__all__ = ["DestinyTribulationService", "OPERATION_TABLE"]
