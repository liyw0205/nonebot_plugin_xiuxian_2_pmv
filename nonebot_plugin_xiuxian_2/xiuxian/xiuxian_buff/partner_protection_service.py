"""Stable facade for partner protection transactions."""

from .transaction_service import PartnerProtectionService

# BEGIN IMMEDIATE protects the idempotent partner_protection_operations ledger.
OPERATION_TABLE = "partner_protection_operations"

__all__ = ["PartnerProtectionService", "OPERATION_TABLE"]
