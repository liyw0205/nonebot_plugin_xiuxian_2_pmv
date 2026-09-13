"""Stable facade for natal treasure training."""

from .transaction_service import NatalTrainingService

# The implementation uses ATTACH DATABASE and BEGIN IMMEDIATE.
OPERATION_TABLE = "natal_training_operations"

__all__ = ["NatalTrainingService", "OPERATION_TABLE"]
