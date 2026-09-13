"""Stable facade for cross-database bank interest settlement."""

from .transaction_service import BankInterestService

# The implementation uses ATTACH DATABASE and BEGIN IMMEDIATE.
OPERATION_TABLE = "bank_interest_operations"

__all__ = ["BankInterestService", "OPERATION_TABLE"]
