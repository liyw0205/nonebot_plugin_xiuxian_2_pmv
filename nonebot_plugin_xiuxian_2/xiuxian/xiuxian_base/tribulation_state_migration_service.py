"""Stable facade for legacy tribulation state migration."""

from .transaction_service import TribulationStateMigrationService

# BEGIN IMMEDIATE protects the idempotent tribulation_state_migration_operations ledger.
OPERATION_TABLE = "tribulation_state_migration_operations"

__all__ = ["TribulationStateMigrationService", "OPERATION_TABLE"]
