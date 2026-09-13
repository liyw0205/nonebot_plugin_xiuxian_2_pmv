"""Stable facade for puppet purchase and upgrade operations."""

from .transaction_service import PuppetOperationService

# BEGIN IMMEDIATE protects the idempotent puppet_operations ledger.
OPERATION_TABLE = "puppet_operations"

__all__ = ["PuppetOperationService", "OPERATION_TABLE"]
