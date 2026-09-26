"""Stable facade for legacy tribulation state migration."""

from ...compatibility.legacy_base_tribulation_state_migration import TribulationStateMigrationService

# BEGIN IMMEDIATE protects the idempotent tribulation_state_migration_operations ledger.
OPERATION_TABLE = "tribulation_state_migration_operations"

__all__ = ["TribulationStateMigrationService", "OPERATION_TABLE"]
