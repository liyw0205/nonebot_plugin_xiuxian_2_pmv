"""Stable facade for player rename transactions."""

from .transaction_service import PlayerRenameService

# BEGIN IMMEDIATE protects the idempotent player_rename_operations ledger.
OPERATION_TABLE = "player_rename_operations"

__all__ = ["PlayerRenameService", "OPERATION_TABLE"]
