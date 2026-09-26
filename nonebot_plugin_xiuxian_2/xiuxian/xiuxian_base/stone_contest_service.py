"""Stable facade for stone contest transfers."""

from ...compatibility.legacy_base_stone_contest import StoneContestService

# BEGIN IMMEDIATE protects the idempotent stone_contest_operations ledger.
OPERATION_TABLE = "stone_contest_operations"

__all__ = ["StoneContestService", "OPERATION_TABLE"]
