"""Stable facade for equipment transactions."""

from ...compatibility.legacy_back_equipment import EquipmentService

# BEGIN IMMEDIATE protects the idempotent equipment_operations ledger.
OPERATION_TABLE = "equipment_operations"

__all__ = ["EquipmentService", "OPERATION_TABLE"]
