"""Stable facade for cross-database stone robbery settlement."""

from ...compatibility.legacy_base_stone_robbery import StoneRobberySettlementService

# The implementation uses ATTACH DATABASE and BEGIN IMMEDIATE.
OPERATION_TABLE = "stone_robbery_operations"

__all__ = ["StoneRobberySettlementService", "OPERATION_TABLE"]
