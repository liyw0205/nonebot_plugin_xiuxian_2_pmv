"""Stable facade for cross-database bank deposits."""

from .transaction_service import BankDepositService

# The implementation uses ATTACH DATABASE and BEGIN IMMEDIATE.
OPERATION_TABLE = "bank_deposit_operations"

__all__ = ["BankDepositService", "OPERATION_TABLE"]
