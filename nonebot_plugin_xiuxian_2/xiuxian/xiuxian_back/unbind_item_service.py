"""Stable facade for unbind-item transactions."""

from ...compatibility.legacy_back_unbind import UnbindItemService

# BEGIN IMMEDIATE protects the idempotent unbind_operations ledger.
OPERATION_TABLE = "unbind_item_operations"

__all__ = ["UnbindItemService", "OPERATION_TABLE"]
