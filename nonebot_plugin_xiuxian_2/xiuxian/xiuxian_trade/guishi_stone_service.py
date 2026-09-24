"""Stable facade for explicit rollback of cross-database Guishi transfers."""

from ...compatibility.legacy_guishi_stone import (
    LegacyGuishiStoneService as GuishiStoneService,
)

# The implementation uses ATTACH DATABASE and BEGIN IMMEDIATE.
OPERATION_TABLE = "guishi_stone_operations"

__all__ = ["GuishiStoneService", "OPERATION_TABLE"]
