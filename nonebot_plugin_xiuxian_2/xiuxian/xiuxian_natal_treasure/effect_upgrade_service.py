"""Stable facade for natal treasure effect upgrades."""

from .transaction_service import EffectUpgradeService

# The implementation uses ATTACH DATABASE and BEGIN IMMEDIATE.
OPERATION_TABLE = "natal_effect_upgrade_operations"

__all__ = ["EffectUpgradeService", "OPERATION_TABLE"]
