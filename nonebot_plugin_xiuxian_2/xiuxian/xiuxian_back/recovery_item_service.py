"""Stable facade for recovery item transactions."""

from ...compatibility.legacy_back_recovery_item import RecoveryItemService

# BEGIN IMMEDIATE protects the idempotent recovery_item_operations ledger.
OPERATION_TABLE = "recovery_item_operations"

__all__ = ["RecoveryItemService", "OPERATION_TABLE"]
