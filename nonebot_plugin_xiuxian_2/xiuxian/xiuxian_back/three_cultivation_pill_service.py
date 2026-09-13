"""Stable facade for three-cultivation-pill transactions."""

from .transaction_service import ThreeCultivationPillService

# BEGIN IMMEDIATE protects the idempotent three_cultivation_pill_operations ledger.
OPERATION_TABLE = "three_cultivation_pill_operations"

__all__ = ["ThreeCultivationPillService", "OPERATION_TABLE"]
