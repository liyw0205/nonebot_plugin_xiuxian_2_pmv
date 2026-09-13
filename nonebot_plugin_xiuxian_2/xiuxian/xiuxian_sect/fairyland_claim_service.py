"""Stable facade for sect fairyland claims."""

from .transaction_service import FairylandClaimService

# BEGIN IMMEDIATE protects sect_fairyland_claim_operations.
OPERATION_TABLE = "sect_fairyland_claim_operations"

__all__ = ["FairylandClaimService", "OPERATION_TABLE"]
