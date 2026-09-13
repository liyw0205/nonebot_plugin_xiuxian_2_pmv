"""Stable facade for heart-devil tribulation replay and settlement."""

from .transaction_service import HeartDevilTribulationService

# def replay(operation_id, user_id) is provided by the implementation.
OPERATION_TABLE = "heart_devil_tribulation_operations"

__all__ = ["HeartDevilTribulationService", "OPERATION_TABLE"]
