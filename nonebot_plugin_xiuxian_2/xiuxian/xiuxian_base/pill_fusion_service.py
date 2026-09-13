"""Stable facade for destiny pill fusion transactions."""

from .transaction_service import PillFusionService

# BEGIN IMMEDIATE protects the idempotent pill_fusion_operations ledger.
OPERATION_TABLE = "pill_fusion_operations"

__all__ = ["PillFusionService", "OPERATION_TABLE"]
