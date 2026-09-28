"""Stable facade for cross-database demon reward claims."""

from ...compatibility.legacy_demon_claim import DemonClaimService

# The implementation uses ATTACH DATABASE and BEGIN IMMEDIATE.
OPERATION_TABLE = "demon_claim_operations"

__all__ = ["DemonClaimService", "OPERATION_TABLE"]
