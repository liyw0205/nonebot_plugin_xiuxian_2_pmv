"""Stable facade for cross-database training events."""

from .transaction_service import TrainingEventService

# The implementation uses ATTACH DATABASE and BEGIN IMMEDIATE.
OPERATION_TABLE = "training_event_operations"

__all__ = ["TrainingEventService", "OPERATION_TABLE"]
