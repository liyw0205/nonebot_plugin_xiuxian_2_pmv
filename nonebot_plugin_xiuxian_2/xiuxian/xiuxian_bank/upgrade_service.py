"""Stable facade for cross-database bank upgrades."""

from ...compatibility.legacy_bank_upgrade_interest import BankUpgradeService

# The implementation uses ATTACH DATABASE and BEGIN IMMEDIATE.
OPERATION_TABLE = "bank_upgrade_operations"

__all__ = ["BankUpgradeService", "OPERATION_TABLE"]
