"""Stable facade for destiny pill fusion transactions."""

from ...compatibility.legacy_base_pill_fusion import PillFusionService

# BEGIN IMMEDIATE protects the idempotent pill_fusion_operations ledger.
OPERATION_TABLE = "pill_fusion_operations"

__all__ = ["PillFusionService", "OPERATION_TABLE"]
