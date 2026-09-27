"""Stable facade for persistent map combat lifecycle."""

from ...compatibility.legacy_map_transactions import MapCombatLifecycleService

# BEGIN IMMEDIATE protects map_combat_start_operations and combat_cd_until.
OPERATION_TABLE = "map_combat_start_operations"
# The settlement repository updates combat_cd_until=EXCLUDED.combat_cd_until.

__all__ = ["MapCombatLifecycleService", "OPERATION_TABLE"]
