"""Stable facade for cross-database bank withdrawals."""

from .transaction_service import BankWithdrawalService

# The implementation uses ATTACH DATABASE and BEGIN IMMEDIATE.
OPERATION_TABLE = "bank_withdrawal_operations"

__all__ = ["BankWithdrawalService", "OPERATION_TABLE"]
