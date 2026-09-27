"""Stable facade for sect fairyland claims."""

from ...compatibility.legacy_sect_fairyland_claim import FairylandClaimService

# BEGIN IMMEDIATE protects sect_fairyland_claim_operations.
OPERATION_TABLE = "sect_fairyland_claim_operations"

__all__ = ["FairylandClaimService", "OPERATION_TABLE"]
