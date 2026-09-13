"""Stable facade for cross-database bank upgrades."""

from .transaction_service import BankUpgradeService

# The implementation uses ATTACH DATABASE and BEGIN IMMEDIATE.
OPERATION_TABLE = "bank_upgrade_operations"

__all__ = ["BankUpgradeService", "OPERATION_TABLE"]
