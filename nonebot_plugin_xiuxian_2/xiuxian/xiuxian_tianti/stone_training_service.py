"""Stable facade for cross-database Tianti stone training."""

from .transaction_service import StoneTrainingService

# The implementation uses ATTACH DATABASE and BEGIN IMMEDIATE.
OPERATION_TABLE = "tianti_stone_training_operations"

__all__ = ["StoneTrainingService", "OPERATION_TABLE"]
